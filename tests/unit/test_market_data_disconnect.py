"""MD-012 disconnect and recovery behavior against deterministic fakes."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest

from packages.market_data.adapters.binance_normalizer import BinanceSpotNormalizer
from packages.market_data.adapters.binance_rest import PublicMarketDataRequestError
from packages.market_data.adapters.connection_supervisor import ConnectionSupervisor
from packages.market_data.adapters.subscription_coordinator import SubscriptionCoordinator
from packages.market_data.adapters.value_types import (
    MarketSymbol,
    RawMarketMessage,
    StreamSubscription,
)
from packages.market_data.contracts.events import CandleClosedEvent
from packages.market_data.services.gap_recovery import (
    GapRecoveryBuffer,
    GapRecoveryGate,
    GapRecoveryService,
    RecoveryOverflowIncident,
)
from packages.market_data.services.normalization import NormalizationPipeline
from packages.market_data.services.rate_limit import (
    BinanceRateLimitCoordinator,
    RateLimitBlockedError,
)
from tests.fixtures.binance_payloads import (
    BASE_EPOCH_MS,
    MALFORMED_JSON_BYTES,
    kline_payload,
    raw_message,
    rest_kline_row,
    trade_payload,
)
from tests.fixtures.fake_rest import FakeRestAdapter
from tests.fixtures.fake_ws import FakeWebSocketAdapter, WsScriptStep

BTCUSDT = MarketSymbol("BTC", "USDT")
TRADE_SUB = StreamSubscription("binance", "spot", BTCUSDT, "trade")
KLINE_SUB = StreamSubscription("binance", "spot", BTCUSDT, "kline", "1m")

BASE_TIME = datetime.fromtimestamp(BASE_EPOCH_MS / 1000, UTC)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 5.0) -> None:
    """Spin the test loop until a supervisor-side condition holds."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0)


def _noop_sleep(recorded: list[float]) -> Callable[[float], Awaitable[None]]:
    async def sleep(seconds: float) -> None:
        recorded.append(seconds)
        await asyncio.sleep(0)

    return sleep


class TestFakeWebSocketAdapter:
    """Scripted feed scenarios independent of the supervisor."""

    async def test_delayed_frame_records_sleep_and_then_delivers(self) -> None:
        sleeps: list[float] = []
        adapter = FakeWebSocketAdapter(
            [
                WsScriptStep("ack"),
                WsScriptStep("delay", seconds=2.5),
                WsScriptStep("data", trade_payload(trade_id=1)),
            ],
            clock=lambda: BASE_TIME,
            sleep=_noop_sleep(sleeps),
        )
        await adapter.connect()
        await adapter.subscribe((TRADE_SUB,))

        messages = [message async for message in adapter.raw_messages()]

        assert sleeps == [2.5]
        assert len(messages) == 1
        assert messages[0].stream_name == "btcusdt@trade"
        assert adapter.active_subscriptions == frozenset({TRADE_SUB})
        assert adapter.state == "streaming"

    async def test_malformed_raw_payload_is_quarantined_not_fatal(self) -> None:
        adapter = FakeWebSocketAdapter(
            [
                WsScriptStep("data", trade_payload(trade_id=1)),
                WsScriptStep("raw", MALFORMED_JSON_BYTES),
                WsScriptStep("data", trade_payload(trade_id=2)),
            ],
            clock=lambda: BASE_TIME,
        )
        await adapter.connect()
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        pipeline.set_active_subscriptions({TRADE_SUB})

        dispositions = [
            (await pipeline.process(message)).disposition
            for message in [message async for message in adapter.raw_messages()]
        ]

        assert dispositions == ["accepted", "quarantined", "accepted"]

    async def test_out_of_order_candles_dedupe_without_false_gap(self) -> None:
        adapter = FakeWebSocketAdapter(
            [
                WsScriptStep("data", kline_payload(open_time=BASE_EPOCH_MS)),
                WsScriptStep("data", kline_payload(open_time=BASE_EPOCH_MS)),
                WsScriptStep("data", kline_payload(open_time=BASE_EPOCH_MS - 60_000)),
            ],
            clock=lambda: BASE_TIME,
        )
        await adapter.connect()
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        pipeline.set_active_subscriptions({KLINE_SUB})

        results = [
            await pipeline.process(message)
            for message in [message async for message in adapter.raw_messages()]
        ]

        assert [result.disposition for result in results] == [
            "accepted",
            "duplicate",
            "accepted",
        ]
        assert all(result.gap is None for result in results)

    async def test_control_rejection_leaves_no_active_subscriptions(self) -> None:
        adapter = FakeWebSocketAdapter(
            [WsScriptStep("reject", reason="subscription rejected")],
            clock=lambda: BASE_TIME,
        )
        coordinator = SubscriptionCoordinator(adapter)
        coordinator.update_subscriptions([TRADE_SUB])
        await adapter.connect()
        await coordinator.sync_with_adapter()
        assert adapter.pending_subscriptions == frozenset({TRADE_SUB})

        messages = [message async for message in adapter.raw_messages()]

        assert messages == []
        assert adapter.active_subscriptions == frozenset()
        assert adapter.pending_subscriptions == frozenset()
        assert len(adapter.control_errors) == 1


class TestConnectionSupervisor:
    """Deterministic disconnect, recovery, and retry-budget behavior."""

    async def test_reconnect_after_scripted_disconnect_emits_potential_gaps(
        self,
    ) -> None:
        handled: list[RawMarketMessage] = []
        sleeps: list[float] = []

        async def handler(message: RawMarketMessage) -> None:
            handled.append(message)

        adapter = FakeWebSocketAdapter(
            [
                WsScriptStep("ack"),
                WsScriptStep("data", trade_payload(trade_id=1)),
                WsScriptStep("data", trade_payload(trade_id=2)),
                WsScriptStep("disconnect", reason="scripted drop"),
            ],
            clock=lambda: BASE_TIME,
            sleep=_noop_sleep(sleeps),
        )
        coordinator = SubscriptionCoordinator(adapter, clock=lambda: BASE_TIME)
        coordinator.update_subscriptions([TRADE_SUB, KLINE_SUB])
        supervisor = ConnectionSupervisor(
            adapter,
            coordinator,
            max_reconnect_attempts=3,
            planned_rotation_interval=0,
            clock=lambda: BASE_TIME,
            sleep=_noop_sleep(sleeps),
            random_uniform=lambda low, high: low,
            on_raw_message=handler,
        )

        await supervisor.start()
        await _wait_until(lambda: len(handled) == 2)
        assert supervisor.state == "streaming"
        assert supervisor.reconnect_attempt == 0

        await _wait_until(lambda: adapter.connect_count == 2)
        assert supervisor.state == "subscribing"
        assert supervisor.reconnect_attempt == 1

        states = [event.current_state for event in supervisor.drain_status_events()]
        assert states == [
            "connecting",
            "subscribing",
            "streaming",
            "backing_off",
            "connecting",
            "subscribing",
        ]
        gaps = supervisor.drain_gap_events()
        assert {gap.stream_kind for gap in gaps} == {"trade", "kline"}
        assert {gap.recoverability for gap in gaps} == {"none", "closed_candles"}
        assert all(gap.certainty == "potential" for gap in gaps)
        assert all(gap.detection_basis == "connection_interruption" for gap in gaps)

        await supervisor.stop()
        assert supervisor.state == "stopped"

    async def test_reconnect_budget_exhaustion_opens_circuit_and_probes_sparsely(
        self,
    ) -> None:
        sleeps: list[float] = []
        adapter = FakeWebSocketAdapter(
            (),
            connect_failures=10,
            clock=lambda: BASE_TIME,
            sleep=_noop_sleep(sleeps),
        )
        coordinator = SubscriptionCoordinator(adapter, clock=lambda: BASE_TIME)
        coordinator.update_subscriptions([TRADE_SUB])
        supervisor = ConnectionSupervisor(
            adapter,
            coordinator,
            max_reconnect_attempts=2,
            planned_rotation_interval=0,
            clock=lambda: BASE_TIME,
            sleep=_noop_sleep(sleeps),
            random_uniform=lambda low, high: low,
        )

        await supervisor.start()
        await _wait_until(
            lambda: supervisor.get_health_report()["circuit_breaker"]["state"] == "open"
        )

        report = supervisor.get_health_report()
        assert report["retry_exhausted"] is True
        assert supervisor.reconnect_attempt == 5
        assert adapter.connect_count == 5

        await supervisor.stop()
        assert supervisor.state == "stopped"


class TestGapRecoveryWithFakeRest:
    """Bounded closed-candle repair against a scripted public history."""

    @pytest.fixture
    def pipeline(self) -> NormalizationPipeline:
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        pipeline.set_active_subscriptions({KLINE_SUB})
        return pipeline

    async def test_recovers_missing_closed_klines_from_fake_rest(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        server_time = datetime.fromtimestamp((BASE_EPOCH_MS + 150_000) / 1000, UTC)
        rest = FakeRestAdapter(
            server_time=server_time,
            klines={
                ("BTC/USDT", "1m"): [
                    rest_kline_row(open_time=BASE_EPOCH_MS),
                    rest_kline_row(open_time=BASE_EPOCH_MS + 60_000),
                ]
            },
            clock=lambda: BASE_TIME,
        )
        service = GapRecoveryService(rest, pipeline)
        watermark = datetime.fromtimestamp((BASE_EPOCH_MS - 60_000) / 1000, UTC)

        result = await service.recover(
            subscriptions=[KLINE_SUB],
            last_closed_open_times={("binance", "BTC/USDT", "1m"): watermark},
            buffer=GapRecoveryBuffer(),
        )

        assert len(result.rest_results) == 2
        assert all(r.disposition == "accepted" for r in result.rest_results)
        events = result.rest_results
        assert all(isinstance(r.event, CandleClosedEvent) for r in events)
        assert all(r.event.source == "rest_gap_recovery" for r in events)
        assert result.recovery_end_exclusive == server_time
        assert result.buffered_websocket_results == ()
        assert result.buffer_overflowed is False

    async def test_rate_limited_recovery_is_observable_and_blocked(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        server_time = datetime.fromtimestamp((BASE_EPOCH_MS + 150_000) / 1000, UTC)
        rest = FakeRestAdapter(
            server_time=server_time,
            fail_after_requests=1,
            fail_status=429,
            clock=lambda: BASE_TIME,
        )
        service = GapRecoveryService(rest, pipeline)
        watermark = datetime.fromtimestamp((BASE_EPOCH_MS - 60_000) / 1000, UTC)

        with pytest.raises(PublicMarketDataRequestError) as error:
            await service.recover(
                subscriptions=[KLINE_SUB],
                last_closed_open_times={("binance", "BTC/USDT", "1m"): watermark},
                buffer=GapRecoveryBuffer(),
            )
        assert error.value.status_code == 429

        limiter = BinanceRateLimitCoordinator(clock=lambda: BASE_TIME)
        limiter.observe_response(status_code=429, headers={"Retry-After": "1"})
        with pytest.raises(RateLimitBlockedError):
            await limiter.acquire("/api/v3/time", 1)

    async def test_gap_gate_buffers_websocket_frames_while_recovering(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        server_time = datetime.fromtimestamp((BASE_EPOCH_MS + 150_000) / 1000, UTC)
        rest = FakeRestAdapter(server_time=server_time, clock=lambda: BASE_TIME)
        gate = GapRecoveryGate(GapRecoveryService(rest, pipeline))
        watermark = datetime.fromtimestamp((BASE_EPOCH_MS - 60_000) / 1000, UTC)

        assert gate.ingest_websocket(raw_message(trade_payload(trade_id=1))) == "forward"
        assert gate.recovering is False

        frame = raw_message(
            kline_payload(open_time=BASE_EPOCH_MS + 60_000),
            stream_name="btcusdt@kline_1m",
            sequence=2,
        )
        recovery = asyncio.create_task(
            gate.recover(
                subscriptions=[KLINE_SUB],
                last_closed_open_times={("binance", "BTC/USDT", "1m"): watermark},
            )
        )
        await _wait_until(lambda: gate.recovering)
        assert gate.ingest_websocket(frame) == "buffered"

        result = await recovery
        assert len(result.buffered_websocket_results) == 1
        assert result.buffered_websocket_results[0].disposition == "accepted"
        assert result.rest_results == ()
        assert gate.recovering is False

    async def test_buffer_overflow_is_a_typed_observable_incident(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        server_time = datetime.fromtimestamp((BASE_EPOCH_MS + 150_000) / 1000, UTC)
        rest = FakeRestAdapter(server_time=server_time, clock=lambda: BASE_TIME)
        gate = GapRecoveryGate(
            GapRecoveryService(rest, pipeline),
            GapRecoveryBuffer(capacity=1),
            clock=lambda: BASE_TIME,
        )
        watermark = datetime.fromtimestamp((BASE_EPOCH_MS - 60_000) / 1000, UTC)

        first = raw_message(
            kline_payload(open_time=BASE_EPOCH_MS + 60_000),
            stream_name="btcusdt@kline_1m",
            sequence=2,
        )
        second = raw_message(
            kline_payload(open_time=BASE_EPOCH_MS + 120_000),
            stream_name="btcusdt@kline_1m",
            sequence=3,
        )
        recovery = asyncio.create_task(
            gate.recover(
                subscriptions=[KLINE_SUB],
                last_closed_open_times={("binance", "BTC/USDT", "1m"): watermark},
            )
        )
        await _wait_until(lambda: gate.recovering)
        assert gate.ingest_websocket(first) == "buffered"
        assert gate.ingest_websocket(second) == "dropped"
        assert gate.dropped_count == 1

        incidents = gate.drain_overflow_incidents()
        assert len(incidents) == 1
        incident = incidents[0]
        assert isinstance(incident, RecoveryOverflowIncident)
        assert incident.dropped_message_id == second.message_id
        assert incident.connection_id == second.connection_id
        assert incident.dropped_count == 1
        assert incident.buffer_capacity == 1
        assert incident.occurred_at == BASE_TIME
        assert gate.drain_overflow_incidents() == ()

        result = await recovery
        assert result.buffer_overflowed is True
        assert len(result.buffered_websocket_results) == 1

    async def test_no_overflow_incident_without_drop(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        server_time = datetime.fromtimestamp((BASE_EPOCH_MS + 150_000) / 1000, UTC)
        rest = FakeRestAdapter(server_time=server_time, clock=lambda: BASE_TIME)
        gate = GapRecoveryGate(
            GapRecoveryService(rest, pipeline),
            GapRecoveryBuffer(capacity=1),
            clock=lambda: BASE_TIME,
        )
        watermark = datetime.fromtimestamp((BASE_EPOCH_MS - 60_000) / 1000, UTC)

        first = raw_message(
            kline_payload(open_time=BASE_EPOCH_MS + 60_000),
            stream_name="btcusdt@kline_1m",
            sequence=2,
        )
        recovery = asyncio.create_task(
            gate.recover(
                subscriptions=[KLINE_SUB],
                last_closed_open_times={("binance", "BTC/USDT", "1m"): watermark},
            )
        )
        await _wait_until(lambda: gate.recovering)
        assert gate.ingest_websocket(first) == "buffered"

        result = await recovery
        assert gate.drain_overflow_incidents() == ()
        assert result.buffer_overflowed is False
