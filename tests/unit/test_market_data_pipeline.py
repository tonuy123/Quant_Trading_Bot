"""Deterministic coverage for MD-005 through MD-010 public data behavior."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from packages.market_data.adapters.binance_normalizer import BinanceSpotNormalizer
from packages.market_data.adapters.binance_rest import (
    BinanceSpotRestAdapter,
    HttpResponse,
    PublicMarketDataRequestError,
)
from packages.market_data.adapters.binance_ws import BinanceWebSocketAdapter
from packages.market_data.adapters.value_types import (
    MarketSymbol,
    RawMarketMessage,
    StreamSubscription,
)
from packages.market_data.contracts.events import CandleClosedEvent, TickerEvent, TradeEvent
from packages.market_data.services.freshness import FreshnessMonitor
from packages.market_data.services.gap_recovery import (
    GapRecoveryBuffer,
    GapRecoveryGate,
    GapRecoveryService,
)
from packages.market_data.services.normalization import NormalizationPipeline
from packages.market_data.services.rate_limit import (
    BinanceRateLimitCoordinator,
    RateLimitBlockedError,
    WebSocketRateBudget,
)
from packages.market_data.services.raw_capture import BoundedRawCapture

BASE_TIME = datetime(2026, 8, 18, tzinfo=UTC)
BTCUSDT = MarketSymbol("BTC", "USDT")
TRADE_SUBSCRIPTION = StreamSubscription("binance", "spot", BTCUSDT, "trade")
TICKER_SUBSCRIPTION = StreamSubscription("binance", "spot", BTCUSDT, "ticker")
KLINE_SUBSCRIPTION = StreamSubscription("binance", "spot", BTCUSDT, "kline", "1m")


def raw_message(
    payload: object,
    *,
    stream_name: str = "btcusdt@trade",
    sequence: int = 1,
    received_at: datetime = BASE_TIME,
) -> RawMarketMessage:
    """Build an ingress record with exact serialised provider bytes."""
    return RawMarketMessage(
        exchange="binance",
        market_type="spot",
        connection_id="test-connection",
        stream_name=stream_name,
        payload_bytes=json.dumps(payload, separators=(",", ":")).encode(),
        received_at=received_at,
        received_monotonic_ns=sequence,
        receive_sequence=sequence,
        source_timestamp_unit="ms",
    )


def trade_payload(*, price: str = "100.25", trade_id: int = 9) -> dict[str, object]:
    """Return a valid Binance public aggregate trade payload."""
    return {
        "e": "trade",
        "E": 1_786_579_200_000,
        "s": "BTCUSDT",
        "t": trade_id,
        "p": price,
        "q": "0.25",
        "T": 1_786_579_199_999,
        "m": False,
    }


def kline_payload(*, open_time: int, closed: bool = True) -> dict[str, object]:
    """Return a Binance public kline frame with a configurable closure flag."""
    return {
        "e": "kline",
        "E": open_time + 60_000,
        "s": "BTCUSDT",
        "k": {
            "t": open_time,
            "T": open_time + 59_999,
            "s": "BTCUSDT",
            "i": "1m",
            "o": "100",
            "h": "110",
            "l": "90",
            "c": "105",
            "v": "10",
            "n": 3,
            "x": closed,
            "q": "1020",
            "V": "4",
            "Q": "408",
        },
    }


def ticker_payload() -> dict[str, object]:
    """Return a valid Binance public websocket 24-hour ticker payload."""
    return {
        "e": "24hrTicker",
        "E": 1_786_579_200_000,
        "s": "BTCUSDT",
        "p": "5",
        "P": "5",
        "w": "102",
        "x": "100",
        "c": "105",
        "Q": "0.2",
        "b": "104",
        "B": "3",
        "a": "106",
        "A": "2",
        "o": "100",
        "h": "110",
        "l": "90",
        "v": "10",
        "q": "1020",
        "O": 1_786_492_800_000,
        "C": 1_786_579_199_999,
        "F": 1,
        "L": 5,
        "n": 5,
    }


class TestCanonicalNormalizationPipeline:
    """MD-007 through MD-009 ingress, canonicalization, and integrity tests."""

    @pytest.fixture
    def pipeline(self) -> NormalizationPipeline:
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        pipeline.set_active_subscriptions(
            {TRADE_SUBSCRIPTION, TICKER_SUBSCRIPTION, KLINE_SUBSCRIPTION}
        )
        return pipeline

    @pytest.mark.asyncio
    async def test_combined_and_raw_trade_envelopes_use_one_decimal_schema(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        combined = raw_message(
            {"stream": "btcusdt@trade", "data": trade_payload()},
            sequence=1,
        )
        result = await pipeline.process(combined)

        assert result.disposition == "accepted"
        assert isinstance(result.event, TradeEvent)
        assert str(result.event.price) == "100.25"
        assert result.event.occurred_at.tzinfo is UTC
        assert result.event.schema_version == 1

        raw = raw_message(trade_payload(trade_id=10), sequence=2)
        raw_result = await pipeline.process(raw)
        assert raw_result.disposition == "accepted"
        assert isinstance(raw_result.event, TradeEvent)

    @pytest.mark.asyncio
    async def test_partial_kline_is_ignored_and_closed_kline_is_canonical(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        partial = await pipeline.process(
            raw_message(
                kline_payload(open_time=1_786_579_200_000, closed=False),
                stream_name="btcusdt@kline_1m",
            )
        )
        closed = await pipeline.process(
            raw_message(
                kline_payload(open_time=1_786_579_200_000),
                stream_name="btcusdt@kline_1m",
                sequence=2,
            )
        )

        assert partial.disposition == "ignored"
        assert isinstance(closed.event, CandleClosedEvent)
        assert closed.event.close_time - closed.event.open_time == timedelta(minutes=1)

    @pytest.mark.asyncio
    async def test_capture_failure_and_capacity_drop_do_not_corrupt_event(
        self,
    ) -> None:
        capture = BoundedRawCapture(capacity=1)
        pipeline = NormalizationPipeline(BinanceSpotNormalizer(), raw_capture=capture)
        pipeline.set_active_subscriptions({TRADE_SUBSCRIPTION})

        first = await pipeline.process(raw_message(trade_payload(), sequence=1))
        second = await pipeline.process(raw_message(trade_payload(trade_id=10), sequence=2))

        assert first.disposition == "accepted"
        assert second.disposition == "accepted"
        assert second.raw_capture_failed is True
        assert capture.dropped_count == 1
        assert capture.size == 1

    @pytest.mark.asyncio
    async def test_invalid_numeric_data_is_quarantined_not_coerced(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        invalid = trade_payload()
        invalid["p"] = 100.25
        result = await pipeline.process(raw_message(invalid))

        assert result.disposition == "quarantined"
        assert result.quarantine is not None
        assert result.quarantine.reason_code == "numeric"

    @pytest.mark.asyncio
    async def test_invalid_candle_interval_boundary_is_quarantined(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        malformed = kline_payload(open_time=1_786_579_200_000)
        malformed["k"] = {**malformed["k"], "T": 1_786_579_230_000}  # type: ignore[arg-type]
        result = await pipeline.process(raw_message(malformed, stream_name="btcusdt@kline_1m"))

        assert result.disposition == "quarantined"
        assert result.quarantine is not None
        assert result.quarantine.reason_code == "candle_time"

    @pytest.mark.asyncio
    async def test_conflicting_trade_and_candle_duplicates_are_integrity_incidents(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        assert (
            await pipeline.process(raw_message(trade_payload(), sequence=1))
        ).disposition == "accepted"
        conflict = await pipeline.process(raw_message(trade_payload(price="101"), sequence=2))
        assert conflict.disposition == "quarantined"
        assert conflict.quarantine is not None
        assert conflict.quarantine.reason_code == "integrity_conflict"

        first_candle = await pipeline.process(
            raw_message(
                kline_payload(open_time=1_786_579_200_000),
                stream_name="btcusdt@kline_1m",
                sequence=3,
            )
        )
        altered = kline_payload(open_time=1_786_579_200_000)
        altered["k"] = {**altered["k"], "c": "106"}  # type: ignore[arg-type]
        candle_conflict = await pipeline.process(
            raw_message(altered, stream_name="btcusdt@kline_1m", sequence=4)
        )
        assert first_candle.disposition == "accepted"
        assert candle_conflict.quarantine is not None
        assert candle_conflict.quarantine.reason_code == "integrity_conflict"

    @pytest.mark.asyncio
    async def test_exact_missing_kline_slot_emits_observable_gap(
        self,
        pipeline: NormalizationPipeline,
    ) -> None:
        first = await pipeline.process(
            raw_message(kline_payload(open_time=1_786_579_200_000), stream_name="btcusdt@kline_1m")
        )
        later = await pipeline.process(
            raw_message(
                kline_payload(open_time=1_786_579_320_000),
                stream_name="btcusdt@kline_1m",
                sequence=2,
            )
        )

        assert first.gap is None
        assert later.gap is not None
        assert later.gap.missing_open_times == (datetime.fromtimestamp(1_786_579_260, UTC),)
        assert later.gap.event.recoverability == "closed_candles"

    @pytest.mark.asyncio
    async def test_rest_ticker_is_explicit_snapshot_not_replayed_delta(self) -> None:
        rest_ticker = {
            "symbol": "BTCUSDT",
            "bidPrice": "104",
            "bidQty": "3",
            "askPrice": "106",
            "askQty": "2",
            "lastPrice": "105",
            "lastQty": "0.2",
            "openTime": 1_786_492_800_000,
            "closeTime": 1_786_579_199_999,
            "openPrice": "100",
            "highPrice": "110",
            "lowPrice": "90",
            "volume": "10",
            "quoteVolume": "1020",
            "firstId": 1,
            "lastId": 5,
            "count": 5,
        }
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        pipeline.set_active_subscriptions({TICKER_SUBSCRIPTION})
        result = await pipeline.process(
            raw_message(rest_ticker, stream_name="btcusdt@ticker"),
            source="rest_snapshot",
        )

        assert isinstance(result.event, TickerEvent)
        assert result.event.source == "rest_snapshot"


class _ScriptedTransport:
    def __init__(self, responses: list[HttpResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, Mapping[str, str]]] = []

    async def get(self, url: str, params: Mapping[str, str]) -> HttpResponse:
        self.calls.append((url, params))
        return self.responses.pop(0)


class TestPublicRestAndRateLimit:
    """MD-005 and MD-006 use public endpoints, one pipeline, no hidden retry."""

    @pytest.mark.asyncio
    async def test_rest_returns_utc_raw_klines_for_shared_pipeline(self) -> None:
        transport = _ScriptedTransport(
            [
                HttpResponse(200, {}, b'{"serverTime":1786579260000}'),
                HttpResponse(
                    200,
                    {"X-MBX-USED-WEIGHT-1M": "12"},
                    b'[[1786579200000,"100","110","90","105","10",1786579259999,"1020",3,"4","408","0"]]',
                ),
            ]
        )
        adapter = BinanceSpotRestAdapter(transport=transport, clock=lambda: BASE_TIME)
        assert await adapter.get_server_time() == datetime.fromtimestamp(1_786_579_260, UTC)
        klines = await adapter.get_closed_klines(
            BTCUSDT,
            "1m",
            datetime.fromtimestamp(1_786_579_200, UTC),
            datetime.fromtimestamp(1_786_579_260, UTC),
        )
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        pipeline.set_active_subscriptions({KLINE_SUBSCRIPTION})
        normalized = await pipeline.process(klines[0], source="rest_gap_recovery")

        assert len(klines) == 1
        assert klines[0].received_at.tzinfo is UTC
        assert isinstance(normalized.event, CandleClosedEvent)
        assert normalized.event.source == "rest_gap_recovery"
        assert transport.calls[1][1]["endTime"] == "1786579259999"

    @pytest.mark.asyncio
    async def test_429_and_418_block_future_requests_without_blind_retry(self) -> None:
        now = [BASE_TIME]
        limiter = BinanceRateLimitCoordinator(clock=lambda: now[0])
        transport = _ScriptedTransport(
            [HttpResponse(429, {"Retry-After": "5", "X-MBX-USED-WEIGHT-1M": "42"}, b"{}")]
        )
        adapter = BinanceSpotRestAdapter(
            transport=transport, rate_limiter=limiter, clock=lambda: now[0]
        )

        with pytest.raises(PublicMarketDataRequestError) as first:
            await adapter.get_server_time()
        assert first.value.status_code == 429
        with pytest.raises(RateLimitBlockedError) as blocked:
            await adapter.get_server_time()
        assert blocked.value.status_code == 429
        assert len(transport.calls) == 1
        assert limiter.snapshot().used_weight_headers["x-mbx-used-weight-1m"] == 42

        limiter.observe_response(status_code=418, headers={"Retry-After": "60"})
        with pytest.raises(RateLimitBlockedError) as banned:
            await limiter.acquire("/api/v3/time", 1)
        assert banned.value.status_code == 418

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status_code", [403, 500, 503])
    async def test_non_retryable_http_failures_are_returned_once(
        self,
        status_code: int,
    ) -> None:
        transport = _ScriptedTransport([HttpResponse(status_code, {}, b"{}")])
        adapter = BinanceSpotRestAdapter(transport=transport)

        with pytest.raises(PublicMarketDataRequestError) as error:
            await adapter.get_server_time()
        assert error.value.status_code == status_code
        assert len(transport.calls) == 1

    def test_websocket_budget_preserves_control_and_reconnect_headroom(self) -> None:
        now = [0.0]
        budget = WebSocketRateBudget(
            monotonic_clock=lambda: now[0],
            max_control_messages_per_window=2,
            max_connection_attempts_per_window=1,
        )
        assert budget.reserve_control_message() is True
        assert budget.reserve_control_message() is True
        assert budget.reserve_control_message() is False
        assert budget.reserve_connection_attempt() is True
        assert budget.reserve_connection_attempt() is False


class TestWebSocketAcknowledgementContract:
    """MD-003 acknowledges provider IDs and preserves raw raw-envelope data."""

    @pytest.mark.asyncio
    async def test_batches_and_activates_only_after_binance_ack(self) -> None:
        adapter = BinanceWebSocketAdapter(max_control_params=1)
        adapter._ws = AsyncMock()
        eth = StreamSubscription("binance", "spot", MarketSymbol("ETH", "USDT"), "trade")

        await adapter.subscribe({TRADE_SUBSCRIPTION, eth})

        assert adapter.active_subscriptions == frozenset()
        assert adapter.pending_subscriptions == {TRADE_SUBSCRIPTION, eth}
        assert adapter._ws.send.await_count == 2
        ids = sorted(adapter._control_requests)
        for request_id in ids:
            adapter._handle_control_message({"id": request_id, "result": None})
        assert adapter.active_subscriptions == {TRADE_SUBSCRIPTION, eth}

    @pytest.mark.asyncio
    async def test_raw_websocket_envelope_keeps_provider_bytes_and_inferred_stream(self) -> None:
        adapter = BinanceWebSocketAdapter()
        socket = _FakeWebSocket(
            [
                json.dumps({"id": 1, "result": None}),
                json.dumps(trade_payload()),
            ]
        )
        adapter._ws = socket
        adapter._connection_id = "websocket-test"
        adapter._state.transition_to("subscribing")
        await adapter.subscribe({TRADE_SUBSCRIPTION})

        message = await anext(adapter.raw_messages())

        assert message.stream_name == "btcusdt@trade"
        assert message.payload_bytes == json.dumps(trade_payload()).encode()
        assert adapter.active_subscriptions == {TRADE_SUBSCRIPTION}
        assert adapter.state == "streaming"


class _FakeWebSocket:
    def __init__(self, frames: list[str]) -> None:
        self._frames = iter(frames)

    def __aiter__(self) -> AsyncIterator[str]:
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._frames)
        except StopIteration as error:
            raise StopAsyncIteration from error

    async def send(self, payload: str) -> None:
        del payload

    async def close(self) -> None:
        return None


class _FakeHistory:
    async def get_server_time(self) -> datetime:
        return datetime.fromtimestamp(1_786_579_260, UTC)

    async def get_closed_klines(
        self,
        symbol: MarketSymbol,
        interval: str,
        start_inclusive: datetime,
        end_exclusive: datetime,
        page_limit: int = 1000,
    ) -> tuple[RawMarketMessage, ...]:
        del symbol, interval, start_inclusive, end_exclusive, page_limit
        row: list[object] = [
            1_786_579_200_000,
            "100",
            "110",
            "90",
            "105",
            "10",
            1_786_579_259_999,
            "1020",
            3,
            "4",
            "408",
        ]
        return (raw_message(row, stream_name="btcusdt@kline_1m"),)

    async def get_ticker_snapshot(self, symbol: MarketSymbol) -> RawMarketMessage:
        raise AssertionError(f"not used for recovery: {symbol}")


class TestGapRecoveryAndFreshness:
    """MD-005 bounded recovery and MD-010 deterministic stale episodes."""

    @pytest.mark.asyncio
    async def test_recovery_uses_rest_closed_klines_then_drains_buffer(self) -> None:
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        pipeline.set_active_subscriptions({KLINE_SUBSCRIPTION, TRADE_SUBSCRIPTION})
        buffer = GapRecoveryBuffer(capacity=1)
        assert buffer.append(raw_message(trade_payload(), sequence=2))
        recovery = GapRecoveryService(_FakeHistory(), pipeline)

        result = await recovery.recover(
            subscriptions={KLINE_SUBSCRIPTION},
            last_closed_open_times={
                ("binance", "BTC/USDT", "1m"): datetime.fromtimestamp(1_786_579_140, UTC)
            },
            buffer=buffer,
        )

        assert result.rest_results[0].event is not None
        assert result.rest_results[0].event.source == "rest_gap_recovery"
        assert result.buffered_websocket_results[0].event is not None
        assert result.buffered_websocket_results[0].event.source == "websocket"
        assert buffer.drain() == ()

    def test_recovery_gate_never_forwards_frame_out_of_order_during_repair(self) -> None:
        pipeline = NormalizationPipeline(BinanceSpotNormalizer())
        gate = GapRecoveryGate(
            GapRecoveryService(_FakeHistory(), pipeline), GapRecoveryBuffer(capacity=1)
        )
        message = raw_message(trade_payload())

        assert gate.ingest_websocket(message) == "forward"
        gate._recovering = True
        assert gate.ingest_websocket(message) == "buffered"
        assert gate.ingest_websocket(message) == "dropped"
        assert gate.buffer.dropped_count == 1

    def test_quiet_trade_stream_is_not_stale_and_reminders_are_controlled(self) -> None:
        now = [BASE_TIME]
        monitor = FreshnessMonitor(
            clock=lambda: now[0],
            ticker_threshold_seconds=10,
            reminder_seconds=30,
        )
        monitor.register({TRADE_SUBSCRIPTION, TICKER_SUBSCRIPTION})

        now[0] += timedelta(seconds=11)
        initial = monitor.evaluate(connection_state="streaming", connection_id="c")
        assert [event.stream_kind for event in initial] == ["ticker"]
        assert monitor.evaluate(connection_state="streaming", connection_id="c") == ()

        now[0] += timedelta(seconds=31)
        reminder = monitor.evaluate(connection_state="streaming", connection_id="c")
        assert len(reminder) == 1
        monitor.record_valid(TICKER_SUBSCRIPTION, now[0])
        assert monitor.evaluate(connection_state="streaming", connection_id="c") == ()
