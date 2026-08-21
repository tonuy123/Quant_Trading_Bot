"""Lifecycle supervision for public market-data WebSocket connections."""

from __future__ import annotations

import asyncio
import random
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from packages.market_data.adapters.value_types import (
    ConnectionSnapshot,
    ConnectionState,
    GapRecoverability,
    RawMarketMessage,
    StreamKind,
)
from packages.market_data.contracts.events import (
    ConnectionStatusChanged,
    DataGapDetected,
)


async def _discard_raw_message(message: RawMarketMessage) -> None:
    """Consume a raw frame when a composition root has no downstream handler."""
    del message


@dataclass
class CircuitBreaker:
    """Bound failed connection attempts and admit one cooldown probe at a time."""

    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    success_threshold: int = 1
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(UTC), repr=False)
    _state: Literal["closed", "open", "half_open"] = "closed"
    _consecutive_failures: int = 0
    _consecutive_successes: int = 0
    _last_failure_time: datetime | None = None
    _opened_at: datetime | None = None
    _probe_in_flight: bool = False

    @property
    def state(self) -> Literal["closed", "open", "half_open"]:
        """Return current breaker state."""
        return self._state

    @property
    def is_closed(self) -> bool:
        """Return whether ordinary attempts are admitted."""
        return self._state == "closed"

    def record_success(self) -> None:
        """Record successful fresh input; only this closes a half-open circuit."""
        if self._state == "half_open":
            self._consecutive_successes += 1
            self._probe_in_flight = False
            if self._consecutive_successes >= self.success_threshold:
                self._close()
        elif self._state == "closed":
            self._consecutive_failures = 0

    def record_failure(self) -> None:
        """Record failed connection/heartbeat input and open when threshold hits."""
        self._last_failure_time = self._utc_now()
        if self._state == "half_open":
            self._open()
            return
        self._consecutive_failures += 1
        if self._state == "closed" and self._consecutive_failures >= self.failure_threshold:
            self._open()

    def allow_request(self) -> bool:
        """Allow ordinary calls or a single post-cooldown sparse probe."""
        if self._state == "closed":
            return True
        if self._state == "open":
            if self._opened_at is None:
                return False
            elapsed = (self._utc_now() - self._opened_at).total_seconds()
            if elapsed < self.recovery_timeout:
                return False
            self._state = "half_open"
            self._consecutive_successes = 0
            self._probe_in_flight = True
            return True
        if self._probe_in_flight:
            return False
        self._probe_in_flight = True
        return True

    def get_snapshot(self) -> dict[str, Any]:
        """Return non-sensitive breaker observability."""
        return {
            "state": self._state,
            "consecutive_failures": self._consecutive_failures,
            "consecutive_successes": self._consecutive_successes,
            "opened_at": self._opened_at.isoformat() if self._opened_at else None,
            "last_failure_time": (
                self._last_failure_time.isoformat() if self._last_failure_time else None
            ),
        }

    def _open(self) -> None:
        self._state = "open"
        self._opened_at = self._utc_now()
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._probe_in_flight = False

    def _close(self) -> None:
        self._state = "closed"
        self._consecutive_failures = 0
        self._consecutive_successes = 0
        self._opened_at = None
        self._probe_in_flight = False

    def _utc_now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("circuit-breaker clock must return an aware datetime")
        return value.astimezone(UTC)


class ConnectionSupervisor:
    """Own the public feed receive loop, heartbeats, retry budget, and breaker.

    A successful TCP/WebSocket handshake deliberately stays ``subscribing``.
    State becomes ``streaming`` only after the adapter has received a market
    frame; this prevents a socket-open event from being treated as data health.
    """

    def __init__(
        self,
        adapter: Any,
        coordinator: Any,
        *,
        heartbeat_timeout: float = 30.0,
        max_reconnect_attempts: int = 10,
        initial_backoff: float = 1.0,
        max_backoff: float = 60.0,
        backoff_multiplier: float = 2.0,
        planned_rotation_interval: float = 3_600.0,
        sparse_probe_interval: float = 300.0,
        on_raw_message: Callable[[RawMarketMessage], Awaitable[None]] | None = None,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        random_uniform: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if heartbeat_timeout <= 0 or max_reconnect_attempts < 1:
            raise ValueError("supervisor timeout and retry budget must be positive")
        self._adapter = adapter
        self._coordinator = coordinator
        self._heartbeat_timeout = heartbeat_timeout
        self._max_reconnect_attempts = max_reconnect_attempts
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._backoff_multiplier = backoff_multiplier
        self._planned_rotation_interval = planned_rotation_interval
        self._sparse_probe_interval = sparse_probe_interval
        self._on_raw_message = on_raw_message or _discard_raw_message
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleep = sleep
        self._random_uniform = random_uniform
        self._state: ConnectionState = "stopped"
        self._reconnect_attempt = 0
        self._last_frame_time: datetime | None = None
        self._connection_start_time: datetime | None = None
        self._circuit = CircuitBreaker(clock=self._clock)
        self._message_loop_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._rotation_task: asyncio.Task[None] | None = None
        self._sparse_probe_task: asyncio.Task[None] | None = None
        self._closing = False
        self._status_events: deque[ConnectionStatusChanged] = deque(maxlen=1_000)
        self._gap_events: deque[DataGapDetected] = deque(maxlen=1_000)
        self._on_state_change: Callable[[ConnectionState, ConnectionState, str], None] | None = None
        self._on_gap_detected: Callable[[str, datetime | None, datetime], None] | None = None
        set_observer = getattr(self._adapter, "set_subscription_observer", None)
        if callable(set_observer):
            set_observer(self._coordinator.reconcile_confirmed)

    def set_clock(self, clock: Callable[[], datetime]) -> None:
        """Set the same deterministic clock for supervisor and breaker."""
        self._clock = clock
        self._circuit.clock = clock

    def set_message_handler(
        self,
        handler: Callable[[RawMarketMessage], Awaitable[None]] | None,
    ) -> None:
        """Set the canonical raw-ingress handler used by the receive loop."""
        self._on_raw_message = handler or _discard_raw_message

    @property
    def state(self) -> ConnectionState:
        """Return supervisor lifecycle state."""
        return self._state

    @property
    def reconnect_attempt(self) -> int:
        """Return current finite fast-reconnect attempt count."""
        return self._reconnect_attempt

    def drain_status_events(self) -> tuple[ConnectionStatusChanged, ...]:
        """Return and clear bounded canonical lifecycle observations."""
        events = tuple(self._status_events)
        self._status_events.clear()
        return events

    def drain_gap_events(self) -> tuple[DataGapDetected, ...]:
        """Return and clear bounded potential-gap observations after disconnect."""
        events = tuple(self._gap_events)
        self._gap_events.clear()
        return events

    async def start(self) -> None:
        """Connect and submit desired subscriptions once."""
        if self._state != "stopped":
            raise RuntimeError(f"cannot start from state: {self._state}")
        self._closing = False
        await self._connect()

    async def stop(self, reason: str = "user request") -> None:
        """Cancel receive/monitor tasks and close the public adapter cleanly."""
        self._closing = True
        self._set_state("stopping", reason)
        await self._cancel_tasks()
        await self._coordinator.stop_stale_detection()
        await self._adapter.close(reason)
        self._set_state("stopped", reason)
        self._reconnect_attempt = 0

    async def _connect(self) -> None:
        if self._closing:
            return
        if not self._circuit.allow_request():
            self._set_state("backing_off", "circuit breaker open")
            self._ensure_sparse_probe()
            return
        self._set_state("connecting", "connect request")
        try:
            await self._adapter.connect()
            self._connection_start_time = self._utc_now()
            self._last_frame_time = None
            await self._coordinator.sync_with_adapter()
            self._set_state("subscribing", "subscriptions submitted")
            await self._start_background_tasks()
        except Exception as error:
            self._circuit.record_failure()
            await self._handle_connection_failure(f"connect failed: {type(error).__name__}")

    async def _start_background_tasks(self) -> None:
        await self._coordinator.start_stale_detection(interval=10.0)
        self._message_loop_task = asyncio.create_task(self._message_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        if self._planned_rotation_interval > 0:
            self._rotation_task = asyncio.create_task(self._rotation_loop())

    async def _message_loop(self) -> None:
        try:
            handler = self._on_raw_message
            async for message in self._adapter.raw_messages():
                self.record_frame_received(message.received_at)
                self._coordinator.reconcile_confirmed(self._adapter.active_subscriptions)
                await handler(message)
                if self._state == "subscribing":
                    self._reconnect_attempt = 0
                    self._circuit.record_success()
                    self._set_state("streaming", "first valid raw market frame")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self._closing:
                await self._handle_disconnect(f"receive loop: {type(error).__name__}")

    async def _handle_connection_failure(self, reason: str) -> None:
        if self._closing:
            self._set_state("stopped", "closing")
            return
        self._reconnect_attempt += 1
        self._set_state("backing_off", reason)
        if self._reconnect_attempt > self._max_reconnect_attempts:
            self._ensure_sparse_probe()
            return
        await self._backoff_then_connect()

    async def _backoff_then_connect(self) -> None:
        maximum = min(
            self._max_backoff,
            self._initial_backoff * (self._backoff_multiplier**self._reconnect_attempt),
        )
        await self._sleep(self._random_uniform(0.0, maximum))
        if not self._closing:
            await self._connect()

    def _ensure_sparse_probe(self) -> None:
        if self._sparse_probe_task is None or self._sparse_probe_task.done():
            self._sparse_probe_task = asyncio.create_task(self._sparse_probe_loop())

    async def _sparse_probe_loop(self) -> None:
        try:
            while not self._closing and self._reconnect_attempt > self._max_reconnect_attempts:
                await self._sleep(self._sparse_probe_interval)
                if not self._closing:
                    await self._connect()
        except asyncio.CancelledError:
            raise

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._closing:
                await self._sleep(min(5.0, self._heartbeat_timeout / 2))
                if self._state not in {"subscribing", "streaming"}:
                    continue
                if self._last_frame_time is None:
                    continue
                if (
                    self._utc_now() - self._last_frame_time
                ).total_seconds() > self._heartbeat_timeout:
                    await self._handle_disconnect("heartbeat timeout")
                    return
        except asyncio.CancelledError:
            raise

    async def _rotation_loop(self) -> None:
        try:
            await self._sleep(self._planned_rotation_interval)
            if not self._closing and self._state in {"subscribing", "streaming"}:
                await self._handle_disconnect("planned rotation")
        except asyncio.CancelledError:
            raise

    async def _handle_disconnect(self, reason: str) -> None:
        if self._closing:
            return
        self._emit_potential_gaps(reason)
        if self._on_gap_detected is not None:
            self._on_gap_detected(reason, self._last_frame_time, self._utc_now())
        self._circuit.record_failure()
        await self._cancel_tasks(exclude=asyncio.current_task())
        await self._handle_connection_failure(reason)

    async def _cancel_tasks(self, exclude: asyncio.Task[Any] | None = None) -> None:
        tasks = (
            self._message_loop_task,
            self._heartbeat_task,
            self._rotation_task,
            self._sparse_probe_task,
        )
        for task in tasks:
            if task is not None and task is not exclude and not task.done():
                task.cancel()
        for task in tasks:
            if task is not None and task is not exclude and not task.done():
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    def record_frame_received(self, received_at: datetime) -> None:
        """Record only an aware raw frame timestamp for heartbeat state."""
        if received_at.tzinfo is None or received_at.utcoffset() is None:
            raise ValueError("frame timestamp must be UTC-aware")
        self._last_frame_time = received_at.astimezone(UTC)

    async def snapshot(self) -> ConnectionSnapshot:
        """Return adapter snapshot without adding provider-specific state."""
        snapshot = await self._adapter.snapshot()
        if not isinstance(snapshot, ConnectionSnapshot):
            raise TypeError("market-data adapter returned an invalid connection snapshot")
        return snapshot

    def get_health_report(self) -> dict[str, Any]:
        """Return safe supervisor health and bounded-attempt state."""
        return {
            "state": self._state,
            "reconnect_attempt": self._reconnect_attempt,
            "max_reconnect_attempts": self._max_reconnect_attempts,
            "retry_exhausted": self._reconnect_attempt > self._max_reconnect_attempts,
            "circuit_breaker": self._circuit.get_snapshot(),
            "last_frame_time": self._last_frame_time.isoformat() if self._last_frame_time else None,
            "connection_uptime_seconds": (
                (self._utc_now() - self._connection_start_time).total_seconds()
                if self._connection_start_time
                else None
            ),
            "heartbeat_timeout": self._heartbeat_timeout,
            "planned_rotation_interval": self._planned_rotation_interval,
        }

    def _set_state(self, new_state: ConnectionState, reason: str) -> None:
        if self._state == new_state:
            return
        previous = self._state
        self._state = new_state
        event = ConnectionStatusChanged(
            event_id=f"connection:{previous}:{new_state}:{int(self._utc_now().timestamp() * 1000)}",
            exchange="binance",
            market_type="spot",
            symbol=None,
            source="websocket",
            occurred_at=self._utc_now(),
            exchange_event_at=None,
            received_at=self._utc_now(),
            connection_id=getattr(self._adapter, "connection_id", None),
            previous_state=previous,
            current_state=new_state,
            reason_code=reason[:128] or "unspecified",
            reconnect_attempt=self._reconnect_attempt,
            next_retry_at=None,
            endpoint_label="binance.public.spot.websocket",
            subscriptions_affected=len(self._coordinator.get_desired()),
        )
        self._status_events.append(event)
        if self._on_state_change is not None:
            self._on_state_change(previous, new_state, reason)

    def _emit_potential_gaps(self, reason: str) -> None:
        detected_at = self._utc_now()
        grouped: dict[tuple[StreamKind, str | None], int] = {}
        recoverability_by_kind: dict[StreamKind, GapRecoverability] = {
            "kline": "closed_candles",
            "ticker": "snapshot_only",
            "trade": "none",
        }
        for subscription in self._coordinator.get_desired():
            key = (subscription.kind, subscription.interval)
            grouped[key] = grouped.get(key, 0) + 1
        for (stream_kind, interval), affected_count in grouped.items():
            recoverability = recoverability_by_kind[stream_kind]
            self._gap_events.append(
                DataGapDetected(
                    event_id=(
                        f"gap:{stream_kind}:{interval or '-'}:{int(detected_at.timestamp() * 1000)}"
                    ),
                    exchange="binance",
                    market_type="spot",
                    symbol=None,
                    source="websocket",
                    occurred_at=detected_at,
                    exchange_event_at=None,
                    received_at=detected_at,
                    connection_id=getattr(self._adapter, "connection_id", None),
                    gap_id=(
                        f"connection:{stream_kind}:{interval or '-'}:"
                        f"{int(detected_at.timestamp() * 1000)}"
                    ),
                    stream_kind=stream_kind,
                    interval=interval,
                    gap_start_at=self._last_frame_time or detected_at,
                    gap_end_at=detected_at,
                    detection_basis="connection_interruption",
                    certainty="potential",
                    recoverability=recoverability,
                    last_known_cursor=None,
                    affected_subscription_count=affected_count,
                    quality_flags=frozenset({"disconnect", reason[:64]}),
                )
            )

    def _utc_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("supervisor clock must return an aware datetime")
        return value.astimezone(UTC)
