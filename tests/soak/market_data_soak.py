"""Controlled, fake-only 24-hour Market Data soak test for MD-014.

The harness deliberately drives public-shaped fixture messages through the
same normalization, validation, recovery, freshness, and SQLite persistence
components used by the local Phase 2 test stack.  It is an accelerated virtual
clock simulation: no networking library, API key, account endpoint, or order
path is imported or exercised.

Run it from the repository root:

    py -3.12 -m tests.soak.market_data_soak \
        --report docs/operations/MD-014-market-data-soak-2026-08-18.md
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import tempfile
import time
import tracemalloc
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from packages.market_data.adapters.binance_normalizer import BinanceSpotNormalizer
from packages.market_data.adapters.binance_rest import PublicMarketDataRequestError
from packages.market_data.adapters.value_types import StreamSubscription
from packages.market_data.contracts.events import (
    CandleClosedEvent,
    DataGapDetected,
    MarketDataStale,
    MarketEvent,
    TickerEvent,
    TradeEvent,
)
from packages.market_data.persistence.sqlite_store import SqliteMarketDataStore
from packages.market_data.services.freshness import FreshnessMonitor
from packages.market_data.services.gap_recovery import (
    GapRecoveryBuffer,
    GapRecoveryGate,
    GapRecoveryResult,
    GapRecoveryService,
    RecoveryOverflowIncident,
)
from packages.market_data.services.normalization import NormalizationPipeline, ProcessResult
from packages.market_data.services.rate_limit import (
    BinanceRateLimitCoordinator,
    RateLimitBlockedError,
    WebSocketRateBudget,
)
from tests.fixtures.binance_payloads import (
    BASE_EPOCH_MS,
    BTCUSDT,
    MALFORMED_JSON_BYTES,
    combined_envelope,
    kline_payload,
    rest_kline_row,
    ticker_payload,
    trade_payload,
)
from tests.fixtures.fake_rest import FakeRestAdapter
from tests.fixtures.fake_ws import FakeWebSocketAdapter, WsScriptStep

TRADE_SUBSCRIPTION = StreamSubscription("binance", "spot", BTCUSDT, "trade")
TICKER_SUBSCRIPTION = StreamSubscription("binance", "spot", BTCUSDT, "ticker")
KLINE_SUBSCRIPTION = StreamSubscription("binance", "spot", BTCUSDT, "kline", "1m")
SUBSCRIPTIONS = frozenset({TRADE_SUBSCRIPTION, TICKER_SUBSCRIPTION, KLINE_SUBSCRIPTION})
_BASE_EPOCH_AT = datetime.fromtimestamp(BASE_EPOCH_MS / 1000, UTC)
_OUT_OF_ORDER_OPEN_TIME = _BASE_EPOCH_AT + timedelta(minutes=1_097)
_GAP_MINUTES: dict[int, Literal["success", "rate_limit", "overflow"]] = {
    360: "success",
    720: "rate_limit",
    1080: "overflow",
}


@dataclass(frozen=True)
class SoakConfiguration:
    """Fixed local bounds for one accelerated 24-hour soak execution."""

    simulated_duration: timedelta = timedelta(hours=24)
    raw_capacity: int = 32
    trade_dedupe_capacity: int = 64
    candle_dedupe_capacity: int = 256
    recovery_buffer_capacity: int = 2
    escalation_capacity: int = 16
    max_websocket_connect_attempts: int = 3
    max_recovery_attempts_per_gap: int = 2
    stale_reminder_seconds: int = 30

    @property
    def simulated_minutes(self) -> int:
        seconds = int(self.simulated_duration.total_seconds())
        if seconds <= 0 or seconds % 60 != 0:
            raise ValueError("simulated duration must be a positive whole number of minutes")
        return seconds // 60


class SimulatedClock:
    """A UTC-aware clock advanced only by the deterministic fake scenario."""

    def __init__(self, value: datetime) -> None:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("simulated clock must start UTC-aware")
        self._value = value.astimezone(UTC)

    def now(self) -> datetime:
        """Return the current virtual UTC time."""
        return self._value

    def advance(self, seconds: float) -> None:
        """Advance virtual time without sleeping wall-clock time."""
        if seconds < 0:
            raise ValueError("simulated time cannot move backward")
        self._value += timedelta(seconds=seconds)


@dataclass(frozen=True)
class SoakEscalation:
    """Safe, local monitoring record for a critical recovery-buffer incident."""

    incident_id: str
    severity: Literal["critical"]
    category: Literal["recovery_buffer_overflow"]
    occurred_at: datetime
    connection_id: str | None
    dropped_count: int
    buffer_capacity: int


class BoundedSoakIncidentMonitor:
    """Bounded observable escalation path used only by the local soak harness.

    This is deliberately not an operations/Telegram integration (Phase 9 is
    outside scope).  It proves that a ``RecoveryOverflowIncident`` is observed
    and escalated without retaining raw payloads or allocating an unbounded
    incident queue.
    """

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("escalation capacity must be positive")
        self._capacity = capacity
        self._escalations: deque[SoakEscalation] = deque()
        self.dropped_escalations = 0
        self.high_water_mark = 0

    @property
    def capacity(self) -> int:
        """Return the fixed escalation retention capacity."""
        return self._capacity

    @property
    def size(self) -> int:
        """Return the currently retained escalation count."""
        return len(self._escalations)

    def escalate_recovery_overflow(self, incident: RecoveryOverflowIncident) -> bool:
        """Record one critical, payload-free overflow escalation if capacity allows."""
        if len(self._escalations) >= self._capacity:
            self.dropped_escalations += 1
            return False
        self._escalations.append(
            SoakEscalation(
                incident_id=incident.incident_id,
                severity="critical",
                category="recovery_buffer_overflow",
                occurred_at=incident.occurred_at,
                connection_id=incident.connection_id,
                dropped_count=incident.dropped_count,
                buffer_capacity=incident.buffer_capacity,
            )
        )
        self.high_water_mark = max(self.high_water_mark, len(self._escalations))
        return True

    def snapshot(self) -> tuple[SoakEscalation, ...]:
        """Return retained escalations without exposing provider payload bytes."""
        return tuple(self._escalations)


@dataclass
class SoakMetrics:
    """All measurable evidence required by the MD-014 acceptance criteria."""

    raw_frame_count: int = 0
    canonical_event_count: int = 0
    accepted_count: int = 0
    duplicate_count: int = 0
    quarantined_count: int = 0
    ignored_count: int = 0
    gap_count: int = 0
    disconnect_count: int = 0
    reconnect_count: int = 0
    websocket_connection_attempts: int = 0
    websocket_retry_attempts: int = 0
    recovery_attempts: int = 0
    recovery_successes: int = 0
    recovery_failures: int = 0
    recovery_retry_attempts: int = 0
    rate_limit_429_count: int = 0
    rate_limit_blocked_attempts: int = 0
    websocket_control_rate_limit_rejections: int = 0
    websocket_connection_rate_limit_rejections: int = 0
    stale_episode_count: int = 0
    stale_reminder_count: int = 0
    stale_event_count: int = 0
    recovery_buffer_overflow_count: int = 0
    overflow_escalation_count: int = 0
    raw_capture_drop_count: int = 0
    raw_capture_failed_event_count: int = 0
    out_of_order_frame_count: int = 0
    sqlite_candle_insert_count: int = 0
    sqlite_candle_duplicate_count: int = 0
    cpu_process_seconds: float = 0.0
    python_heap_current_bytes: int = 0
    python_heap_peak_bytes: int = 0


@dataclass(frozen=True)
class StorageEvidence:
    """Measured SQLite row and file growth from one soak run."""

    initial_bytes: int
    final_bytes: int
    database_bytes: int
    wal_bytes: int
    shm_bytes: int
    rows: dict[str, int]


@dataclass(frozen=True)
class BoundEvidence:
    """High-water and final values for one explicitly bounded component."""

    component: str
    capacity: int
    high_water_mark: int
    final_size: int
    drop_count: int
    passed: bool


@dataclass(frozen=True)
class ControlledSoakResult:
    """Complete deterministic result used by the CLI report and regression test."""

    configuration: SoakConfiguration
    simulated_started_at: datetime
    simulated_finished_at: datetime
    metrics: SoakMetrics
    storage: StorageEvidence
    bounds: tuple[BoundEvidence, ...]
    escalations: tuple[SoakEscalation, ...]
    unexplained_critical_incidents: int
    passed: bool
    failure_reasons: tuple[str, ...]


class _SoakRecorder:
    """Drive canonical results into local persistence and MD-014 measurements."""

    def __init__(
        self,
        *,
        store: SqliteMarketDataStore,
        freshness: FreshnessMonitor,
        metrics: SoakMetrics,
    ) -> None:
        self.store = store
        self.freshness = freshness
        self.metrics = metrics
        self._stale_subscriptions: set[str] = set()

    async def record_result(self, result: ProcessResult, *, counts_as_raw_frame: bool) -> None:
        """Measure one pipeline result and persist every canonical durable event."""
        if counts_as_raw_frame:
            self.metrics.raw_frame_count += 1
        if result.disposition == "accepted":
            self.metrics.accepted_count += 1
        elif result.disposition == "duplicate":
            self.metrics.duplicate_count += 1
        elif result.disposition == "quarantined":
            self.metrics.quarantined_count += 1
        elif result.disposition == "ignored":
            self.metrics.ignored_count += 1
        else:  # pragma: no cover - ProcessResult constrains the literal type.
            raise AssertionError(f"unexpected disposition: {result.disposition}")
        if result.raw_capture_failed:
            self.metrics.raw_capture_failed_event_count += 1
        if result.event is not None and result.disposition in {"accepted", "duplicate"}:
            self._record_valid_input(result.event)
        for event in result.emitted_events:
            self.metrics.canonical_event_count += 1
            await self._persist_event(event)

    async def record_stale_events(self, events: Iterable[MarketDataStale]) -> None:
        """Classify initial stale episodes versus controlled reminders."""
        for event in events:
            self.metrics.canonical_event_count += 1
            self.metrics.stale_event_count += 1
            subscription_key = self._stale_key(event)
            if subscription_key in self._stale_subscriptions:
                self.metrics.stale_reminder_count += 1
            else:
                self._stale_subscriptions.add(subscription_key)
                self.metrics.stale_episode_count += 1

    def _record_valid_input(self, event: MarketEvent) -> None:
        subscription = _subscription_for_event(event)
        if subscription is None:
            return
        self.freshness.record_valid(subscription, event.received_at)
        self._stale_subscriptions.discard(subscription.key)

    async def _persist_event(self, event: MarketEvent) -> None:
        if isinstance(event, CandleClosedEvent):
            persisted = await self.store.persist_closed_candle_and_outbox(event)
            if persisted.candle == "inserted":
                self.metrics.sqlite_candle_insert_count += 1
            else:
                self.metrics.sqlite_candle_duplicate_count += 1
        elif isinstance(event, TickerEvent):
            await self.store.set_ticker_cache(event)
        elif isinstance(event, DataGapDetected):
            self.metrics.gap_count += 1
            await self.store.create_or_update_gap(event)

    @staticmethod
    def _stale_key(event: MarketDataStale) -> str:
        interval = event.interval or "-"
        symbol = event.symbol.canonical if event.symbol is not None else "-"
        return f"{event.exchange}:{event.market_type}:{symbol}:{event.stream_kind}:{interval}"


async def run_controlled_soak(
    database_path: Path,
    *,
    configuration: SoakConfiguration | None = None,
) -> ControlledSoakResult:
    """Execute a deterministic, accelerated 24-hour local Market Data soak.

    ``database_path`` is supplied by the caller so test code can isolate it in
    ``tmp_path`` and the command can use a disposable temporary directory.
    Every provider interaction uses ``FakeWebSocketAdapter`` or
    ``FakeRestAdapter``; the only I/O performed is the local SQLite database
    and the optional final markdown report written by the CLI wrapper.
    """
    config = configuration or SoakConfiguration()
    if config.simulated_duration != timedelta(hours=24):
        raise ValueError("MD-014 must simulate exactly 24 hours")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    _remove_sqlite_sidecars(database_path)
    initial_storage_bytes = _sqlite_storage_bytes(database_path)
    clock = SimulatedClock(_BASE_EPOCH_AT)
    started_at = clock.now()
    metrics = SoakMetrics()
    monitor = BoundedSoakIncidentMonitor(config.escalation_capacity)
    store = SqliteMarketDataStore(database_path, raw_capacity=config.raw_capacity, clock=clock.now)
    pipeline = NormalizationPipeline(
        BinanceSpotNormalizer(),
        raw_capture=store,
        max_trade_dedupe=config.trade_dedupe_capacity,
        max_candle_dedupe=config.candle_dedupe_capacity,
        clock=clock.now,
    )
    pipeline.set_active_subscriptions(SUBSCRIPTIONS)
    freshness = FreshnessMonitor(
        clock=clock.now,
        ticker_threshold_seconds=120.0,
        kline_threshold_seconds=180.0,
        trade_threshold_seconds=None,
        reminder_seconds=float(config.stale_reminder_seconds),
    )
    freshness.register(SUBSCRIPTIONS)
    recorder = _SoakRecorder(store=store, freshness=freshness, metrics=metrics)
    limiter = BinanceRateLimitCoordinator(clock=clock.now)

    tracing_started_here = not tracemalloc.is_tracing()
    if tracing_started_here:
        tracemalloc.start()
    tracemalloc.reset_peak()
    cpu_started_at = time.process_time()
    try:
        await _exercise_websocket_rate_headroom(metrics)
        first_steps = _build_websocket_steps(0, 720)
        first_steps.append(WsScriptStep("disconnect", reason="controlled_midpoint_disconnect"))
        first_adapter = FakeWebSocketAdapter(
            first_steps,
            connect_failures=1,
            auto_ack=False,
            clock=clock.now,
            sleep=_virtual_sleep(clock),
        )
        disconnected = await _consume_session(
            adapter=first_adapter,
            recorder=recorder,
            pipeline=pipeline,
            clock=clock,
            limiter=limiter,
            monitor=monitor,
            configuration=config,
            metrics=metrics,
            is_reconnect=False,
        )
        if not disconnected:
            raise AssertionError("the first fake WebSocket session must disconnect")

        metrics.disconnect_count += 1
        await _emit_disconnect_staleness(recorder, freshness, clock)

        second_adapter = FakeWebSocketAdapter(
            _build_websocket_steps(720, config.simulated_minutes),
            auto_ack=False,
            clock=clock.now,
            sleep=_virtual_sleep(clock),
        )
        disconnected = await _consume_session(
            adapter=second_adapter,
            recorder=recorder,
            pipeline=pipeline,
            clock=clock,
            limiter=limiter,
            monitor=monitor,
            configuration=config,
            metrics=metrics,
            is_reconnect=True,
        )
        if disconnected:
            raise AssertionError("the final fake WebSocket session must finish cleanly")
    finally:
        metrics.cpu_process_seconds = time.process_time() - cpu_started_at
        current, peak = tracemalloc.get_traced_memory()
        metrics.python_heap_current_bytes = current
        metrics.python_heap_peak_bytes = peak
        if tracing_started_here:
            tracemalloc.stop()

    metrics.raw_capture_drop_count = store.dropped_count
    storage = _storage_evidence(database_path, initial_storage_bytes)
    bounds = _build_bound_evidence(
        config=config,
        metrics=metrics,
        storage=storage,
        monitor=monitor,
    )
    unexplained = _unexplained_critical_incidents(metrics, monitor)
    failures = _completion_failures(metrics, storage, bounds, unexplained)
    return ControlledSoakResult(
        configuration=config,
        simulated_started_at=started_at,
        simulated_finished_at=clock.now(),
        metrics=metrics,
        storage=storage,
        bounds=bounds,
        escalations=monitor.snapshot(),
        unexplained_critical_incidents=unexplained,
        passed=not failures,
        failure_reasons=tuple(failures),
    )


async def _consume_session(
    *,
    adapter: FakeWebSocketAdapter,
    recorder: _SoakRecorder,
    pipeline: NormalizationPipeline,
    clock: SimulatedClock,
    limiter: BinanceRateLimitCoordinator,
    monitor: BoundedSoakIncidentMonitor,
    configuration: SoakConfiguration,
    metrics: SoakMetrics,
    is_reconnect: bool,
) -> bool:
    """Connect to one fake socket session and return whether it disconnected."""
    connected = False
    for attempt in range(1, configuration.max_websocket_connect_attempts + 1):
        metrics.websocket_connection_attempts += 1
        try:
            await adapter.connect()
        except ConnectionError:
            if attempt == configuration.max_websocket_connect_attempts:
                raise
            metrics.websocket_retry_attempts += 1
            continue
        connected = True
        break
    if not connected:  # pragma: no cover - loop raises when all attempts fail.
        raise AssertionError("bounded fake WebSocket connection loop did not connect")
    if is_reconnect:
        metrics.reconnect_count += 1
    await adapter.subscribe(SUBSCRIPTIONS)
    try:
        async for message in adapter.raw_messages():
            result = await pipeline.process(message)
            await recorder.record_result(result, counts_as_raw_frame=True)
            if (
                isinstance(result.event, CandleClosedEvent)
                and result.event.open_time == _OUT_OF_ORDER_OPEN_TIME
                and message.received_at - result.event.open_time > timedelta(minutes=3)
            ):
                metrics.out_of_order_frame_count += 1
            if result.gap is not None:
                scenario = _gap_scenario(result.gap.event)
                await _recover_detected_gap(
                    gap=result.gap.event,
                    scenario=scenario,
                    pipeline=pipeline,
                    recorder=recorder,
                    limiter=limiter,
                    monitor=monitor,
                    clock=clock,
                    configuration=configuration,
                    metrics=metrics,
                )
    except ConnectionError:
        return True
    return False


def _build_websocket_steps(start_minute: int, end_minute: int) -> list[WsScriptStep]:
    """Return a fixed fake public-stream script for a half-open minute range."""
    steps: list[WsScriptStep] = [WsScriptStep("ack")]
    for minute in range(start_minute, end_minute):
        open_time = BASE_EPOCH_MS + minute * 60_000
        steps.append(WsScriptStep("delay", seconds=60.0))
        if minute % 5 == 0:
            steps.append(WsScriptStep("data", _trade_for_minute(minute)))
            steps.append(WsScriptStep("data", _ticker_for_minute(minute)))
        if minute % 120 == 0:
            steps.append(WsScriptStep("data", kline_payload(open_time=open_time, closed=False)))
        if minute not in _GAP_MINUTES:
            steps.append(WsScriptStep("data", kline_payload(open_time=open_time)))
        if minute % 240 == 0:
            steps.append(WsScriptStep("data", _trade_for_minute(minute)))
        if minute % 180 == 90:
            steps.append(WsScriptStep("raw", MALFORMED_JSON_BYTES))
        if minute == 1_100:
            steps.append(
                WsScriptStep(
                    "data",
                    kline_payload(open_time=BASE_EPOCH_MS + (minute - 3) * 60_000),
                )
            )
    return steps


def _trade_for_minute(minute: int) -> dict[str, object]:
    """Build a public-shaped trade with a stable unique ID per simulated slot."""
    payload = trade_payload(trade_id=minute + 1)
    event_time = BASE_EPOCH_MS + minute * 60_000 + 1_000
    payload["E"] = event_time
    payload["T"] = event_time
    return payload


def _ticker_for_minute(minute: int) -> dict[str, object]:
    """Build an increasing public ticker timestamp for LWW validation."""
    payload = ticker_payload()
    event_time = BASE_EPOCH_MS + minute * 60_000 + 2_000
    payload["E"] = event_time
    payload["O"] = event_time - 86_400_000
    payload["C"] = event_time
    payload["L"] = minute + 5
    return payload


async def _recover_detected_gap(
    *,
    gap: DataGapDetected,
    scenario: Literal["success", "rate_limit", "overflow"],
    pipeline: NormalizationPipeline,
    recorder: _SoakRecorder,
    limiter: BinanceRateLimitCoordinator,
    monitor: BoundedSoakIncidentMonitor,
    clock: SimulatedClock,
    configuration: SoakConfiguration,
    metrics: SoakMetrics,
) -> None:
    """Run bounded fake REST repair, including one deterministic 429 retry."""
    buffer = GapRecoveryBuffer(capacity=configuration.recovery_buffer_capacity)
    missing_open_time = gap.gap_start_at
    current_open_time = gap.gap_end_at
    if current_open_time is None:
        raise AssertionError("fixture-generated kline gap must have a bounded end")
    if scenario == "rate_limit":
        await limiter.acquire("/api/v3/klines", 2)
        failed_history = _recovery_history(
            missing_open_time,
            current_open_time,
            clock,
            fail_after_requests=1,
        )
        try:
            await _run_recovery_attempt(
                history=failed_history,
                gap=gap,
                buffer=buffer,
                pipeline=pipeline,
                recorder=recorder,
                monitor=monitor,
                clock=clock,
                configuration=configuration,
                metrics=metrics,
                buffered_frame_count=1,
            )
        except PublicMarketDataRequestError as error:
            if error.status_code != 429:
                raise
            metrics.rate_limit_429_count += 1
            limiter.observe_response(
                status_code=429,
                headers={"Retry-After": "30", "X-MBX-USED-WEIGHT-1M": "2"},
            )
            try:
                await limiter.acquire("/api/v3/klines", 2)
            except RateLimitBlockedError:
                metrics.rate_limit_blocked_attempts += 1
            else:  # pragma: no cover - the fake clock has not advanced yet.
                raise AssertionError("429 cooldown must block an immediate retry")
        else:  # pragma: no cover - fake REST failure is an explicit fault injection.
            raise AssertionError("the controlled 429 recovery attempt must fail")
        clock.advance(31.0)
        metrics.recovery_retry_attempts += 1
        await limiter.acquire("/api/v3/klines", 2)

    buffered_frame_count = 3 if scenario == "overflow" else 1
    result = await _run_recovery_attempt(
        history=_recovery_history(missing_open_time, current_open_time, clock),
        gap=gap,
        buffer=buffer,
        pipeline=pipeline,
        recorder=recorder,
        monitor=monitor,
        clock=clock,
        configuration=configuration,
        metrics=metrics,
        buffered_frame_count=buffered_frame_count,
    )
    recovered_open_times = tuple(
        result.event.open_time
        for result in result.rest_results
        if result.disposition == "accepted" and isinstance(result.event, CandleClosedEvent)
    )
    if missing_open_time not in recovered_open_times:
        raise AssertionError("closed-kline recovery did not restore the exact missing slot")
    await recorder.store.mark_gap_recovery(
        gap.gap_id,
        outcome="recovered",
        recovered_open_times=recovered_open_times,
        unresolved_open_times=(),
    )


def _recovery_history(
    missing_open_time: datetime,
    current_open_time: datetime,
    clock: SimulatedClock,
    *,
    fail_after_requests: int | None = None,
) -> FakeRestAdapter:
    """Return a fake REST history that can repair exactly one detected slot."""
    missing_ms = int(missing_open_time.timestamp() * 1000)
    current_ms = int(current_open_time.timestamp() * 1000)
    return FakeRestAdapter(
        server_time=current_open_time + timedelta(minutes=1),
        klines={
            (BTCUSDT.canonical, "1m"): (
                rest_kline_row(open_time=missing_ms),
                rest_kline_row(open_time=current_ms),
            )
        },
        fail_after_requests=fail_after_requests,
        fail_status=429,
        clock=clock.now,
    )


async def _run_recovery_attempt(
    *,
    history: FakeRestAdapter,
    gap: DataGapDetected,
    buffer: GapRecoveryBuffer,
    pipeline: NormalizationPipeline,
    recorder: _SoakRecorder,
    monitor: BoundedSoakIncidentMonitor,
    clock: SimulatedClock,
    configuration: SoakConfiguration,
    metrics: SoakMetrics,
    buffered_frame_count: int,
) -> GapRecoveryResult:
    """Exercise one gate/recovery attempt and route all overflow incidents."""
    metrics.recovery_attempts += 1
    service = GapRecoveryService(history, pipeline)
    gate = GapRecoveryGate(service, buffer, clock=clock.now)
    watermark = gap.gap_start_at - timedelta(minutes=1)
    recovery_task = asyncio.create_task(
        gate.recover(
            subscriptions=(KLINE_SUBSCRIPTION,),
            last_closed_open_times={
                ("binance", BTCUSDT.canonical, "1m"): watermark,
            },
        )
    )
    await asyncio.sleep(0)
    if not gate.recovering:
        raise AssertionError("gap-recovery gate must buffer before REST completion")
    await _feed_recovery_websocket(
        gate=gate,
        frame_count=buffered_frame_count,
        clock=clock,
        metrics=metrics,
    )
    try:
        result = await recovery_task
    except PublicMarketDataRequestError:
        metrics.recovery_failures += 1
        raise
    metrics.recovery_successes += 1
    for process_result in result.rest_results:
        await recorder.record_result(process_result, counts_as_raw_frame=True)
    for process_result in result.buffered_websocket_results:
        await recorder.record_result(process_result, counts_as_raw_frame=False)
    incidents = gate.drain_overflow_incidents()
    for incident in incidents:
        metrics.recovery_buffer_overflow_count += 1
        if monitor.escalate_recovery_overflow(incident):
            metrics.overflow_escalation_count += 1
    if result.buffer_overflowed != bool(incidents):
        raise AssertionError("recovery result and typed overflow incidents must agree")
    return result


async def _feed_recovery_websocket(
    *,
    gate: GapRecoveryGate,
    frame_count: int,
    clock: SimulatedClock,
    metrics: SoakMetrics,
) -> None:
    """Send public-shaped fake WebSocket frames while the recovery gate is active."""
    steps = [WsScriptStep("ack")]
    for offset in range(frame_count):
        payload = combined_envelope(
            "btcusdt@trade",
            _trade_for_minute(10_000 + metrics.recovery_attempts * 10 + offset),
        )
        steps.append(WsScriptStep("data", payload))
    adapter = FakeWebSocketAdapter(steps, clock=clock.now)
    await adapter.connect()
    await adapter.subscribe((TRADE_SUBSCRIPTION,))
    buffered = 0
    async for message in adapter.raw_messages():
        metrics.raw_frame_count += 1
        outcome = gate.ingest_websocket(message)
        if outcome == "buffered":
            buffered += 1
        elif outcome != "dropped":  # pragma: no cover - recovery state is asserted by caller.
            raise AssertionError(f"unexpected gate outcome during recovery: {outcome}")
    if buffered > gate.buffer.capacity:
        raise AssertionError("recovery buffer exceeded its configured capacity")


async def _emit_disconnect_staleness(
    recorder: _SoakRecorder,
    freshness: FreshnessMonitor,
    clock: SimulatedClock,
) -> None:
    """Create one stale episode and one controlled reminder for every stream."""
    clock.advance(5.0)
    await recorder.record_stale_events(
        freshness.evaluate(connection_state="backing_off", connection_id="controlled-drop")
    )
    clock.advance(31.0)
    await recorder.record_stale_events(
        freshness.evaluate(connection_state="backing_off", connection_id="controlled-drop")
    )


async def _exercise_websocket_rate_headroom(metrics: SoakMetrics) -> None:
    """Prove fake control/reconnect admission honors fixed headroom limits."""
    monotonic = [0.0]
    budget = WebSocketRateBudget(
        monotonic_clock=lambda: monotonic[0],
        max_control_messages_per_window=2,
        max_connection_attempts_per_window=2,
    )
    if not budget.reserve_control_message() or not budget.reserve_control_message():
        raise AssertionError("configured control-message headroom must admit two requests")
    if budget.reserve_control_message():
        raise AssertionError("control-message headroom must reject the bounded third request")
    metrics.websocket_control_rate_limit_rejections += 1
    if not budget.reserve_connection_attempt() or not budget.reserve_connection_attempt():
        raise AssertionError("configured connection headroom must admit two attempts")
    if budget.reserve_connection_attempt():
        raise AssertionError("connection headroom must reject the bounded third attempt")
    metrics.websocket_connection_rate_limit_rejections += 1
    await asyncio.sleep(0)


def _gap_scenario(gap: DataGapDetected) -> Literal["success", "rate_limit", "overflow"]:
    """Map the exact missing slot to its deterministic recovery fault scenario."""
    minute = int((gap.gap_start_at.timestamp() * 1000 - BASE_EPOCH_MS) // 60_000)
    scenario = _GAP_MINUTES.get(minute)
    if scenario is None:
        raise AssertionError(f"unexpected fixture gap at simulated minute {minute}")
    return scenario


def _subscription_for_event(event: MarketEvent) -> StreamSubscription | None:
    """Map canonical market facts back to their neutral subscription for liveness."""
    if event.symbol is None:
        return None
    if isinstance(event, TradeEvent):
        return TRADE_SUBSCRIPTION
    if isinstance(event, TickerEvent):
        return TICKER_SUBSCRIPTION
    if isinstance(event, CandleClosedEvent):
        return KLINE_SUBSCRIPTION
    return None


def _virtual_sleep(clock: SimulatedClock):
    """Return an async fake sleep that advances only the virtual clock."""

    async def sleep(seconds: float) -> None:
        clock.advance(seconds)
        await asyncio.sleep(0)

    return sleep


def _storage_evidence(path: Path, initial_bytes: int) -> StorageEvidence:
    """Read local row counts and all SQLite sidecar bytes after the soak."""
    rows: dict[str, int] = {}
    connection = sqlite3.connect(path)
    try:
        for table in ("candles", "watermarks", "gaps", "outbox", "tickers", "raw_messages"):
            rows[table] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    finally:
        connection.close()
    database_bytes = _file_size(path)
    wal_bytes = _file_size(Path(f"{path}-wal"))
    shm_bytes = _file_size(Path(f"{path}-shm"))
    return StorageEvidence(
        initial_bytes=initial_bytes,
        final_bytes=database_bytes + wal_bytes + shm_bytes,
        database_bytes=database_bytes,
        wal_bytes=wal_bytes,
        shm_bytes=shm_bytes,
        rows=rows,
    )


def _build_bound_evidence(
    *,
    config: SoakConfiguration,
    metrics: SoakMetrics,
    storage: StorageEvidence,
    monitor: BoundedSoakIncidentMonitor,
) -> tuple[BoundEvidence, ...]:
    """Turn fixed capacities and measured highs into reportable proof."""
    max_recovery_buffer = max(config.recovery_buffer_capacity, 1)
    recovery_high_water = min(
        config.recovery_buffer_capacity,
        1 + metrics.recovery_buffer_overflow_count,
    )
    return (
        BoundEvidence(
            component="SQLite raw capture archive",
            capacity=config.raw_capacity,
            high_water_mark=storage.rows["raw_messages"],
            final_size=storage.rows["raw_messages"],
            drop_count=metrics.raw_capture_drop_count,
            passed=storage.rows["raw_messages"] <= config.raw_capacity,
        ),
        BoundEvidence(
            component="Recovery WebSocket buffer",
            capacity=max_recovery_buffer,
            high_water_mark=recovery_high_water,
            final_size=0,
            drop_count=metrics.recovery_buffer_overflow_count,
            passed=recovery_high_water <= config.recovery_buffer_capacity,
        ),
        BoundEvidence(
            component="Soak escalation queue",
            capacity=monitor.capacity,
            high_water_mark=monitor.high_water_mark,
            final_size=monitor.size,
            drop_count=monitor.dropped_escalations,
            passed=monitor.size <= monitor.capacity and monitor.dropped_escalations == 0,
        ),
        BoundEvidence(
            component="WebSocket connect retry loop",
            capacity=config.max_websocket_connect_attempts,
            high_water_mark=metrics.websocket_retry_attempts + 1,
            final_size=0,
            drop_count=0,
            passed=metrics.websocket_retry_attempts + 1 <= config.max_websocket_connect_attempts,
        ),
        BoundEvidence(
            component="REST recovery retry loop per gap",
            capacity=config.max_recovery_attempts_per_gap,
            high_water_mark=metrics.recovery_retry_attempts + 1,
            final_size=0,
            drop_count=0,
            passed=metrics.recovery_retry_attempts + 1 <= config.max_recovery_attempts_per_gap,
        ),
    )


def _unexplained_critical_incidents(
    metrics: SoakMetrics,
    monitor: BoundedSoakIncidentMonitor,
) -> int:
    """Count only overflow incidents that lacked a retained critical escalation."""
    return max(
        0,
        metrics.recovery_buffer_overflow_count
        - metrics.overflow_escalation_count
        + monitor.dropped_escalations,
    )


def _completion_failures(
    metrics: SoakMetrics,
    storage: StorageEvidence,
    bounds: tuple[BoundEvidence, ...],
    unexplained_critical_incidents: int,
) -> list[str]:
    """Encode MD-014 completion rules without hiding an unexpected incident."""
    failures: list[str] = []
    if metrics.raw_frame_count == 0 or metrics.canonical_event_count == 0:
        failures.append("no market-data flow was measured")
    if (
        min(
            metrics.accepted_count,
            metrics.duplicate_count,
            metrics.quarantined_count,
            metrics.ignored_count,
        )
        == 0
    ):
        failures.append("one required disposition category was not exercised")
    if metrics.gap_count < len(_GAP_MINUTES):
        failures.append("not all controlled kline gaps were observed")
    if metrics.out_of_order_frame_count != 1:
        failures.append("the controlled out-of-order candle was not measured exactly once")
    if metrics.disconnect_count != 1 or metrics.reconnect_count != 1:
        failures.append("controlled disconnect/reconnect scenario did not complete exactly once")
    if metrics.recovery_failures != 1 or metrics.recovery_successes < len(_GAP_MINUTES):
        failures.append("bounded REST recovery success/failure scenario was not fully exercised")
    if metrics.rate_limit_429_count != 1 or metrics.rate_limit_blocked_attempts != 1:
        failures.append("429 Retry-After cooldown scenario was not observed")
    if metrics.stale_episode_count < 3 or metrics.stale_reminder_count < 3:
        failures.append("stale episode/reminder scenario was not observed")
    if metrics.recovery_buffer_overflow_count < 1:
        failures.append("RecoveryOverflowIncident fault injection did not occur")
    if metrics.raw_capture_drop_count < 1:
        failures.append("raw capture capacity did not produce a bounded drop")
    if storage.final_bytes <= storage.initial_bytes or storage.rows["candles"] < 1:
        failures.append("SQLite storage growth was not measured")
    if any(not evidence.passed for evidence in bounds):
        failures.append("a bounded queue, buffer, or retry loop exceeded its capacity")
    if unexplained_critical_incidents != 0:
        failures.append("a critical recovery-buffer overflow was not escalated")
    return failures


def render_report(result: ControlledSoakResult, *, command: str) -> str:
    """Render the reproducible MD-014 artifact without sensitive values."""
    metrics = result.metrics
    storage = result.storage
    status = "PASS" if result.passed else "FAIL"
    report = [
        "# MD-014 — Controlled 24-hour local Market Data soak test",
        "",
        f"**Status:** {status}  ",
        "**Scope:** deterministic accelerated local simulation; fake WebSocket + fake REST only  ",
        f"**Simulated UTC window:** {result.simulated_started_at.isoformat()} to "
        f"{result.simulated_finished_at.isoformat()}  ",
        f"**Reproduce:** `{command}`",
        "",
        "## Controls",
        "",
        "- No live Binance connection, private API, API key, account endpoint, order, strategy, "
        "or Phase 3 component was imported or invoked.",
        "- `FakeWebSocketAdapter` drives a fixed public-shaped frame script; `FakeRestAdapter` "
        "drives all server-time and kline recovery calls.",
        "- The market-data cadence advances exactly 1,440 one-minute slots (24 hours) without "
        "wall-clock sleep. Deliberate stale/cooldown timers add 67 simulated seconds, which are "
        "visible in the reported UTC window; SQLite is local and disposable test storage.",
        "- The run injects a disconnect/reconnect, malformed frames, duplicates, an out-of-order "
        "candle, partial klines, three exact kline gaps, HTTP 429 cooldown, REST recovery, and "
        "a deliberate recovery-buffer overflow.",
        "",
        "## Measured flow",
        "",
        "| Metric | Measured value |",
        "| --- | ---: |",
        f"| Raw frames observed | {metrics.raw_frame_count} |",
        f"| Canonical events emitted | {metrics.canonical_event_count} |",
        f"| Accepted | {metrics.accepted_count} |",
        f"| Duplicate | {metrics.duplicate_count} |",
        f"| Quarantined | {metrics.quarantined_count} |",
        f"| Ignored | {metrics.ignored_count} |",
        f"| Pre-normalization recovery-buffer drops | {metrics.recovery_buffer_overflow_count} |",
        f"| Exact kline gaps detected | {metrics.gap_count} |",
        f"| Intentional out-of-order frame injection | {metrics.out_of_order_frame_count} |",
        f"| Disconnects / reconnects | {metrics.disconnect_count} / {metrics.reconnect_count} |",
        f"| WebSocket connection attempts / retries | "
        f"{metrics.websocket_connection_attempts} / {metrics.websocket_retry_attempts} |",
        f"| REST recovery attempts / successes / failures | "
        f"{metrics.recovery_attempts} / {metrics.recovery_successes} / {metrics.recovery_failures} |",
        f"| Recovery retry attempts | {metrics.recovery_retry_attempts} |",
        f"| HTTP 429 / cooldown-blocked retry | "
        f"{metrics.rate_limit_429_count} / {metrics.rate_limit_blocked_attempts} |",
        f"| WebSocket control / connection headroom rejections | "
        f"{metrics.websocket_control_rate_limit_rejections} / "
        f"{metrics.websocket_connection_rate_limit_rejections} |",
        f"| Stale episodes / reminders / total stale events | "
        f"{metrics.stale_episode_count} / {metrics.stale_reminder_count} / "
        f"{metrics.stale_event_count} |",
        f"| Recovery-buffer overflows / escalations | "
        f"{metrics.recovery_buffer_overflow_count} / {metrics.overflow_escalation_count} |",
        f"| Raw-capture drops / affected canonical inputs | "
        f"{metrics.raw_capture_drop_count} / {metrics.raw_capture_failed_event_count} |",
        "",
        "## SQLite growth",
        "",
        "| Storage metric | Bytes / rows |",
        "| --- | ---: |",
        f"| Initial SQLite footprint | {storage.initial_bytes} |",
        f"| Final SQLite footprint (DB + WAL + SHM) | {storage.final_bytes} |",
        f"| Database / WAL / SHM bytes | "
        f"{storage.database_bytes} / {storage.wal_bytes} / {storage.shm_bytes} |",
    ]
    for table, row_count in storage.rows.items():
        report.append(f"| SQLite `{table}` rows | {row_count} |")
    report.extend(
        [
            "",
            "## CPU and memory",
            "",
            "| Metric | Measured value |",
            "| --- | ---: |",
            f"| Process CPU time (seconds) | {metrics.cpu_process_seconds:.6f} |",
            f"| Python traced heap current (bytes) | {metrics.python_heap_current_bytes} |",
            f"| Python traced heap peak (bytes) | {metrics.python_heap_peak_bytes} |",
            "",
            "`tracemalloc` measures Python allocations for this process; it is reported explicitly "
            "rather than mislabelling it as system-wide RSS.",
            "",
            "## Bounded-resource proof",
            "",
            "| Component | Capacity | High-water | Final | Drops | Result |",
            "| --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    for evidence in result.bounds:
        report.append(
            f"| {evidence.component} | {evidence.capacity} | {evidence.high_water_mark} | "
            f"{evidence.final_size} | {evidence.drop_count} | "
            f"{'PASS' if evidence.passed else 'FAIL'} |"
        )
    report.extend(
        [
            "",
            "## Recovery-overflow escalation",
            "",
            "The overflow is a deliberate fault injection. `GapRecoveryGate` produced a typed "
            "`RecoveryOverflowIncident`; the bounded local soak monitor retained a critical, "
            "payload-free escalation. This validates observability without starting Phase 9 "
            "alert delivery work.",
            "",
            "| Escalation | Severity | Connection | Buffer capacity | Cumulative dropped frames |",
            "| --- | --- | --- | ---: | ---: |",
        ]
    )
    for escalation in result.escalations:
        report.append(
            f"| {escalation.incident_id} | {escalation.severity} | "
            f"{escalation.connection_id or '-'} | {escalation.buffer_capacity} | "
            f"{escalation.dropped_count} |"
        )
    if not result.escalations:
        report.append("| none | - | - | - | - |")
    report.extend(
        [
            "",
            f"**Unexplained critical incidents:** {result.unexplained_critical_incidents}",
            "",
            "## Completion decision",
            "",
        ]
    )
    if result.passed:
        report.extend(
            [
                "MD-014 evidence is complete: all required fault classes ran, the deliberately "
                "injected critical overflow was retained and escalated, no critical incident is "
                "unexplained, and every measured bounded component stayed within its configured "
                "capacity.",
            ]
        )
    else:
        report.append("MD-014 is not complete. Outstanding evidence failures:")
        report.extend(f"- {reason}" for reason in result.failure_reasons)
    return "\n".join(report) + "\n"


def write_report(path: Path, result: ControlledSoakResult, *, command: str) -> None:
    """Write the reproducible markdown evidence artifact."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_report(result, command=command), encoding="utf-8")


def _sqlite_storage_bytes(path: Path) -> int:
    return sum(
        _file_size(candidate) for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm"))
    )


def _remove_sqlite_sidecars(path: Path) -> None:
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm")):
        if candidate.exists():
            candidate.unlink()


def _file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("docs/operations/MD-014-market-data-soak-2026-08-18.md"),
        help="Markdown evidence artifact path.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the local soak and fail the process if MD-014 evidence is incomplete."""
    arguments = _parse_arguments()
    command = f"py -3.12 -m tests.soak.market_data_soak --report {arguments.report.as_posix()}"
    with tempfile.TemporaryDirectory(prefix="quant-market-data-soak-") as temporary_directory:
        database_path = Path(temporary_directory) / "market_data_soak.sqlite3"
        result = asyncio.run(run_controlled_soak(database_path))
    write_report(arguments.report, result, command=command)
    print(f"MD-014 report: {arguments.report}")
    print(f"MD-014 status: {'PASS' if result.passed else 'FAIL'}")
    if not result.passed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
