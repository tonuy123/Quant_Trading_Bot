"""Bounded closed-kline recovery while WebSocket input remains buffered."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from packages.market_data.adapters.protocols import PublicMarketDataHistory
from packages.market_data.adapters.value_types import (
    ExchangeId,
    MarketType,
    RawMarketMessage,
    StreamSubscription,
)
from packages.market_data.services.normalization import NormalizationPipeline, ProcessResult


class GapRecoveryBuffer:
    """A bounded FIFO buffer for frames received during REST gap recovery."""

    def __init__(self, capacity: int = 1_000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._messages: deque[RawMarketMessage] = deque()
        self.dropped_count = 0

    @property
    def capacity(self) -> int:
        """Return the maximum number of buffered frames."""
        return self._capacity

    def append(self, message: RawMarketMessage) -> bool:
        """Buffer one new WebSocket frame without unbounded growth."""
        if len(self._messages) >= self._capacity:
            self.dropped_count += 1
            return False
        self._messages.append(message)
        return True

    def drain(self) -> tuple[RawMarketMessage, ...]:
        """Return buffered frames in arrival order and clear the buffer."""
        messages = tuple(self._messages)
        self._messages.clear()
        return messages


@dataclass(frozen=True)
class GapRecoveryResult:
    """Observable result of one bounded, closed-candle recovery pass."""

    rest_results: tuple[ProcessResult, ...]
    buffered_websocket_results: tuple[ProcessResult, ...]
    recovery_end_exclusive: datetime
    buffer_overflowed: bool


@dataclass(frozen=True)
class RecoveryOverflowIncident:
    """Typed, observable record of one bounded recovery-buffer overflow.

    Produced by :meth:`GapRecoveryGate.ingest_websocket` when a frame is
    deliberately dropped because the bounded buffer is full.  The incident is
    retained in a bounded per-gate history and drained through
    :meth:`GapRecoveryGate.drain_overflow_incidents`, so an overflow state is
    never silently discarded.  The incident contains no provider secrets.
    """

    incident_id: str
    exchange: ExchangeId
    market_type: MarketType
    connection_id: str | None
    occurred_at: datetime
    dropped_message_id: str
    dropped_count: int
    buffer_capacity: int


class GapRecoveryService:
    """Repair closed candles only and then drain live WebSocket input."""

    def __init__(
        self,
        history: PublicMarketDataHistory,
        pipeline: NormalizationPipeline,
        *,
        page_limit: int = 1000,
    ) -> None:
        self._history = history
        self._pipeline = pipeline
        self._page_limit = page_limit

    async def recover(
        self,
        *,
        subscriptions: Collection[StreamSubscription],
        last_closed_open_times: dict[tuple[str, str, str], datetime],
        buffer: GapRecoveryBuffer,
    ) -> GapRecoveryResult:
        """Recover known closed-kline ranges and process buffered live frames."""
        server_time = await self._history.get_server_time()
        recovery_end = server_time.astimezone(UTC)
        rest_results: list[ProcessResult] = []
        for subscription in subscriptions:
            if subscription.kind != "kline" or subscription.interval is None:
                continue
            subscription_recovery_end = self._latest_closed_boundary(
                server_time,
                subscription.interval,
            )
            watermark = last_closed_open_times.get(
                (subscription.exchange, subscription.symbol.canonical, subscription.interval)
            )
            if watermark is None:
                continue
            start = self._next_candle_open(watermark, subscription.interval)
            if start >= subscription_recovery_end:
                continue
            raw_klines = await self._history.get_closed_klines(
                subscription.symbol,
                subscription.interval,
                start,
                subscription_recovery_end,
                self._page_limit,
            )
            for raw in raw_klines:
                rest_results.append(await self._pipeline.process(raw, source="rest_gap_recovery"))
        websocket_results = [
            await self._pipeline.process(raw, source="websocket") for raw in buffer.drain()
        ]
        return GapRecoveryResult(
            rest_results=tuple(rest_results),
            buffered_websocket_results=tuple(websocket_results),
            recovery_end_exclusive=recovery_end,
            buffer_overflowed=buffer.dropped_count > 0,
        )

    @staticmethod
    def _latest_closed_boundary(
        server_time: datetime,
        interval: str,
    ) -> datetime:
        if server_time.tzinfo is None or server_time.utcoffset() is None:
            raise ValueError("server time must be UTC-aware")
        fixed_interval = GapRecoveryService._interval_delta(interval)
        if fixed_interval is None:
            if interval.endswith("M") and interval[:-1].isdigit() and int(interval[:-1]) > 0:
                months = int(interval[:-1])
                month = ((server_time.month - 1) // months) * months + 1
                return datetime(server_time.year, month, 1, tzinfo=UTC)
            raise ValueError(f"unsupported kline interval for recovery: {interval}")
        epoch_seconds = int(server_time.timestamp())
        boundary_seconds = epoch_seconds - (epoch_seconds % int(fixed_interval.total_seconds()))
        return datetime.fromtimestamp(boundary_seconds, tz=UTC)

    @staticmethod
    def _interval_delta(value: str) -> timedelta | None:
        units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
        if len(value) < 2 or value[-1] not in units:
            return None
        try:
            amount = int(value[:-1])
        except ValueError:
            return None
        return timedelta(seconds=amount * units[value[-1]]) if amount > 0 else None

    @staticmethod
    def _next_candle_open(open_time: datetime, interval: str) -> datetime:
        delta = GapRecoveryService._interval_delta(interval)
        if delta is not None:
            return open_time + delta
        if not interval.endswith("M") or not interval[:-1].isdigit() or int(interval[:-1]) < 1:
            raise ValueError(f"unsupported kline interval for recovery: {interval}")
        months = int(interval[:-1])
        next_month = open_time.month + months
        year = open_time.year + (next_month - 1) // 12
        month = (next_month - 1) % 12 + 1
        return open_time.replace(year=year, month=month)


class GapRecoveryGate:
    """Route WebSocket frames to a bounded buffer while recovery is in progress.

    Composition roots attach :meth:`ingest_websocket` to the public feed's raw
    receive loop.  The gate never manufactures missing trades: live frames are
    simply held, REST repairs only closed kline slots, then held frames are
    normalized in arrival order by :class:`GapRecoveryService`.  Every frame
    dropped because the bounded buffer is full is recorded as a typed
    :class:`RecoveryOverflowIncident` that callers can drain and escalate.
    """

    def __init__(
        self,
        service: GapRecoveryService,
        buffer: GapRecoveryBuffer | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._service = service
        self.buffer = buffer or GapRecoveryBuffer()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._overflow_incidents: deque[RecoveryOverflowIncident] = deque(maxlen=1_000)
        self._recovering = False

    @property
    def recovering(self) -> bool:
        """Return whether WebSocket messages must be buffered."""
        return self._recovering

    @property
    def dropped_count(self) -> int:
        """Return cumulative frames dropped by the bounded buffer."""
        return self.buffer.dropped_count

    def drain_overflow_incidents(self) -> tuple[RecoveryOverflowIncident, ...]:
        """Return and clear recorded overflow incidents in occurrence order."""
        incidents = tuple(self._overflow_incidents)
        self._overflow_incidents.clear()
        return incidents

    def ingest_websocket(
        self,
        message: RawMarketMessage,
    ) -> Literal["forward", "buffered", "dropped"]:
        """Buffer a raw WebSocket frame only while a bounded repair is active.

        ``forward`` means normal live ingress should process the message now.
        ``buffered`` means the message is held for ordered post-recovery
        normalization.  ``dropped`` means the bounded buffer overflowed; the
        overflow state is retained as a typed :class:`RecoveryOverflowIncident`
        instead of being silently discarded, and the caller must treat it as a
        gap incident rather than processing the message out of order.
        """
        if not self._recovering:
            return "forward"
        if self.buffer.append(message):
            return "buffered"
        self._overflow_incidents.append(
            RecoveryOverflowIncident(
                incident_id=(
                    f"recovery-overflow:{message.connection_id}:{message.receive_sequence}"
                ),
                exchange=message.exchange,
                market_type=message.market_type,
                connection_id=message.connection_id,
                occurred_at=self._utc_now(),
                dropped_message_id=message.message_id,
                dropped_count=self.buffer.dropped_count,
                buffer_capacity=self.buffer.capacity,
            )
        )
        return "dropped"

    async def recover(
        self,
        *,
        subscriptions: Collection[StreamSubscription],
        last_closed_open_times: dict[tuple[str, str, str], datetime],
    ) -> GapRecoveryResult:
        """Run a repair transaction while concurrent WebSocket frames queue."""
        self._recovering = True
        try:
            return await self._service.recover(
                subscriptions=subscriptions,
                last_closed_open_times=last_closed_open_times,
                buffer=self.buffer,
            )
        finally:
            self._recovering = False

    def _utc_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("gap-recovery clock must return a UTC-aware datetime")
        return value.astimezone(UTC)
