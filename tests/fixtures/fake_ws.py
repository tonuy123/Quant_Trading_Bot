"""Deterministic fake Binance public WebSocket adapter for network-free tests.

Implements the shape of ``PublicMarketDataFeed`` (see
``packages/market_data/adapters/protocols.py``) entirely from a scripted step
list.  It never imports a networking library, never opens a socket, and never
touches credentials.  All timing is controllable through injected clocks and
sleep hooks, so disconnect and recovery behavior is fully deterministic.

Supported scenarios: normal delivery, control acknowledgement, delayed events,
duplicate and out-of-order frames, malformed raw payloads, mid-stream
disconnect, connection-failure exhaustion, and control rejection.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from packages.market_data.adapters.value_types import (
    ConnectionSnapshot,
    RawMarketMessage,
    StreamSubscription,
)


@dataclass(frozen=True)
class WsScriptStep:
    """One deterministic step in a fake WebSocket session script."""

    kind: Literal["data", "raw", "ack", "reject", "disconnect", "delay"]
    payload: object = None
    reason: str = ""
    seconds: float = 0.0


class FakeWebSocketAdapter:
    """Scripted public market-data feed with no network I/O."""

    def __init__(
        self,
        steps: Collection[WsScriptStep] = (),
        *,
        connect_failures: int = 0,
        auto_ack: bool = False,
        clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], int] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if connect_failures < 0:
            raise ValueError("connect_failures must be non-negative")
        self._steps = list(steps)
        self._connect_failures = connect_failures
        self._auto_ack = auto_ack
        self._clock = clock or (lambda: datetime.now(UTC))
        self._monotonic_clock = monotonic_clock or (lambda: 0)
        self._sleep = sleep or asyncio.sleep
        self._connection_id: str | None = None
        self._connected_at: datetime | None = None
        self._receive_sequence = 0
        self._last_frame_received_at: datetime | None = None
        self._state: str = "stopped"
        self._active_subscriptions: dict[str, StreamSubscription] = {}
        self._pending_subscriptions: dict[str, StreamSubscription] = {}
        self._control_errors: dict[int, str] = {}
        self._next_request_id = 1
        self._connect_count = 0
        self._step_index = 0
        self._closing = False
        self._subscription_observer: Callable[[frozenset[StreamSubscription]], None] | None = None

    # ------------------------------------------------------------------
    # PublicMarketDataFeed shape
    # ------------------------------------------------------------------

    async def connect(self) -> ConnectionSnapshot:
        """Open a scripted connection, failing the first N attempts."""
        if self._connect_count < self._connect_failures:
            self._connect_count += 1
            self._state = "backing_off"
            raise ConnectionError("fake connect failure")
        self._connect_count += 1
        self._connection_id = f"fake-ws-{self._connect_count}"
        self._connected_at = self._utc_now()
        self._receive_sequence = 0
        self._step_index = 0
        self._active_subscriptions = {}
        self._pending_subscriptions = {}
        self._state = "subscribing"
        return self._snapshot()

    async def subscribe(self, items: Collection[StreamSubscription]) -> None:
        """Queue provider subscription requests; optionally acknowledge them."""
        for subscription in items:
            request_id = self._next_request_id
            self._next_request_id += 1
            self._pending_subscriptions[subscription.key] = subscription
            self._control_errors.pop(request_id, None)
        if self._auto_ack:
            self._ack_all_pending()

    async def unsubscribe(self, items: Collection[StreamSubscription]) -> None:
        """Remove subscriptions from the acknowledged set."""
        for subscription in items:
            self._active_subscriptions.pop(subscription.key, None)
        if self._subscription_observer is not None:
            self._subscription_observer(self.active_subscriptions)

    async def raw_messages(self) -> AsyncIterator[RawMarketMessage]:
        """Yield scripted raw frames; a scripted step is consumed exactly once."""
        if self._connection_id is None:
            raise RuntimeError("not connected")
        while self._step_index < len(self._steps):
            step = self._steps[self._step_index]
            self._step_index += 1
            if self._closing:
                return
            if step.kind == "delay":
                await self._sleep(step.seconds)
                continue
            if step.kind == "ack":
                self._ack_all_pending()
                continue
            if step.kind == "reject":
                self._reject_all_pending(step.reason or "fake control rejection")
                continue
            if step.kind == "disconnect":
                raise ConnectionError(step.reason or "fake disconnect")
            self._receive_sequence += 1
            received_at = self._utc_now()
            self._last_frame_received_at = received_at
            payload_bytes = (
                step.payload
                if isinstance(step.payload, bytes)
                else (
                    step.payload.encode("utf-8")
                    if isinstance(step.payload, str)
                    else json.dumps(step.payload, separators=(",", ":")).encode("utf-8")
                )
            )
            yield RawMarketMessage(
                exchange="binance",
                market_type="spot",
                connection_id=self._connection_id,
                stream_name=self._infer_stream_name(step.payload),
                payload_bytes=payload_bytes,
                received_at=received_at,
                received_monotonic_ns=self._monotonic_clock(),
                receive_sequence=self._receive_sequence,
                source_timestamp_unit="ms",
            )
            if self._state == "subscribing":
                self._state = "streaming"

    async def snapshot(self) -> ConnectionSnapshot:
        """Return the current scripted connection snapshot."""
        return self._snapshot()

    async def close(self, reason: str) -> None:
        """Close the scripted connection and retain no credential state."""
        del reason
        self._closing = True
        self._state = "stopped"

    # ------------------------------------------------------------------
    # Adapter-visible state used by supervisors and coordinators
    # ------------------------------------------------------------------

    def set_subscription_observer(
        self,
        observer: Callable[[frozenset[StreamSubscription]], None] | None,
    ) -> None:
        """Observe provider-acknowledged neutral subscription state only."""
        self._subscription_observer = observer

    @property
    def connection_id(self) -> str | None:
        """Return the opaque scripted connection identifier."""
        return self._connection_id

    @property
    def state(self) -> str:
        """Return the current scripted lifecycle state."""
        return self._state

    @property
    def active_subscriptions(self) -> frozenset[StreamSubscription]:
        """Return only scripted-acknowledged subscriptions."""
        return frozenset(self._active_subscriptions.values())

    @property
    def pending_subscriptions(self) -> frozenset[StreamSubscription]:
        """Return subscriptions awaiting a scripted acknowledgement."""
        return frozenset(self._pending_subscriptions.values())

    @property
    def connect_count(self) -> int:
        """Return the number of scripted connection attempts."""
        return self._connect_count

    @property
    def control_errors(self) -> dict[int, str]:
        """Return safe provider rejection text per scripted control request."""
        return dict(self._control_errors)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ack_all_pending(self) -> None:
        if not self._pending_subscriptions:
            return
        self._active_subscriptions.update(self._pending_subscriptions)
        self._pending_subscriptions.clear()
        if self._subscription_observer is not None:
            self._subscription_observer(self.active_subscriptions)

    def _reject_all_pending(self, reason: str) -> None:
        if not self._pending_subscriptions:
            return
        self._control_errors[self._next_request_id - 1] = reason
        self._pending_subscriptions.clear()

    def _snapshot(self) -> ConnectionSnapshot:
        return ConnectionSnapshot(
            connection_id=self._connection_id or "",
            state=self._state,
            connected_at=self._connected_at,
            last_frame_received_at=self._last_frame_received_at,
            active_subscriptions=self.active_subscriptions,
            reconnect_attempt=0,
            is_gap_recovery_pending=False,
        )

    @staticmethod
    def _infer_stream_name(payload: object) -> str:
        if not isinstance(payload, dict):
            return "unknown"
        stream = payload.get("stream")
        if isinstance(stream, str) and stream:
            return stream.lower()
        data = payload.get("data") if "data" in payload else payload
        if not isinstance(data, dict):
            return "unknown"
        event_name = data.get("e")
        symbol = data.get("s")
        if not isinstance(symbol, str) or not symbol:
            return "unknown"
        if event_name == "kline":
            kline = data.get("k")
            interval = kline.get("i") if isinstance(kline, dict) else None
            if isinstance(interval, str):
                return f"{symbol.lower()}@kline_{interval}"
            return "unknown"
        if event_name == "trade":
            return f"{symbol.lower()}@trade"
        if event_name == "24hrTicker":
            return f"{symbol.lower()}@ticker"
        return "unknown"

    def _utc_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("fake WebSocket clock must return a UTC-aware datetime")
        return value.astimezone(UTC)
