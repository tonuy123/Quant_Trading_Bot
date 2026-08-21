"""Public Binance Spot WebSocket adapter and deterministic control contract.

The adapter accepts only exchange-neutral subscriptions and exposes captured
provider frames.  It deliberately does not normalize payloads, persist data,
or access any private Binance endpoint.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
import urllib.parse
import uuid
from collections.abc import AsyncIterator, Callable, Collection
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import websockets

from packages.market_data.adapters.value_types import (
    ConnectionSnapshot,
    ConnectionState,
    MarketSymbol,
    MarketType,
    RawMarketMessage,
    StreamSubscription,
)
from packages.market_data.services.rate_limit import WebSocketRateBudget

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_WS_COMBINED_URL = "wss://stream.binance.com:9443/stream"
MAX_STREAMS_PER_CONNECTION = 500
MAX_CONTROL_MESSAGES_PER_SECOND = 4
MAX_CONTROL_PARAMS_PER_REQUEST = 100
PING_INTERVAL_SECONDS = 20.0
MAX_MESSAGE_SIZE = 32_768
MAX_RECONNECT_ATTEMPTS = 10
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 60.0
BACKOFF_MULTIPLIER = 2.0


class WebSocketControlBudgetExceeded(RuntimeError):
    """A control request would consume reserved heartbeat/reconnect headroom."""


class BinanceSubscriptionRejected(RuntimeError):
    """Binance rejected a correlated public subscription request."""


class BinanceSymbolMapper:
    """Translate exchange-neutral Spot symbols to Binance public identifiers."""

    @staticmethod
    def to_binance_ws_stream(subscription: StreamSubscription) -> str:
        """Return the provider stream name for a neutral subscription."""
        symbol = BinanceSymbolMapper.to_binance_symbol(
            subscription.symbol,
            subscription.market_type,
        ).lower()
        if subscription.kind == "trade":
            suffix = "trade"
        elif subscription.kind == "ticker":
            suffix = "ticker"
        elif subscription.kind == "kline" and subscription.interval:
            suffix = f"kline_{subscription.interval}"
        else:
            raise ValueError("unsupported Binance public stream subscription")
        return f"{symbol}@{suffix}"

    @staticmethod
    def to_binance_symbol(symbol: MarketSymbol, market_type: MarketType) -> str:
        """Return Binance's concatenated Spot symbol form."""
        if market_type != "spot":
            raise ValueError("only Binance Spot public market data is supported")
        return f"{symbol.base}{symbol.quote}"

    @staticmethod
    def from_binance_symbol(binance_symbol: str) -> MarketSymbol:
        """Convert a public Binance symbol to a canonical neutral symbol."""
        normalized = binance_symbol.strip().upper()
        for quote in ("USDT", "USDC", "BUSD", "BTC", "ETH", "BNB", "USD", "EUR"):
            if normalized.endswith(quote) and len(normalized) > len(quote):
                return MarketSymbol(base=normalized[: -len(quote)], quote=quote)
        if len(normalized) < 5:
            raise ValueError("Binance symbol is too short to infer a quote asset")
        return MarketSymbol(base=normalized[:-4], quote=normalized[-4:])


@dataclass
class ConnectionStateMachine:
    """Track public WebSocket lifecycle and bounded exponential backoff."""

    state: ConnectionState = "stopped"
    reconnect_attempt: int = 0
    last_state_change: datetime = field(default_factory=lambda: datetime.now(UTC))
    next_retry_at: datetime | None = None

    def transition_to(self, new_state: ConnectionState, reason: str = "") -> None:
        """Record a legal lifecycle-state change without exposing transport data."""
        del reason
        if self.state != new_state:
            self.state = new_state
            self.last_state_change = datetime.now(UTC)

    def can_reconnect(self) -> bool:
        """Return whether the finite fast-attempt budget remains."""
        return self.reconnect_attempt < MAX_RECONNECT_ATTEMPTS

    def increment_reconnect(self) -> None:
        """Consume one reconnect attempt."""
        self.reconnect_attempt += 1

    def reset_reconnect(self) -> None:
        """Reset failure accounting after actual fresh market input."""
        self.reconnect_attempt = 0

    def calculate_backoff(self) -> float:
        """Return full-jitter backoff for the currently consumed attempt count."""
        return calculate_backoff(self.reconnect_attempt)

    def set_retry_time(self, delay: float) -> None:
        """Set an aware retry deadline without truncating subsecond jitter."""
        self.next_retry_at = datetime.now(UTC) + timedelta(seconds=delay)

    def clear_retry_time(self) -> None:
        """Clear retry metadata after confirmed streaming resumes."""
        self.next_retry_at = None


@dataclass(frozen=True)
class _ControlRequest:
    method: str
    streams: tuple[str, ...]
    subscriptions: tuple[StreamSubscription, ...]


class BinanceWebSocketAdapter:
    """Public-only Binance Spot WebSocket adapter.

    Subscription calls send bounded control batches.  A stream becomes active
    only after a correlated Binance ``{result: null, id: ...}`` acknowledgement
    is received by :meth:`raw_messages`.
    """

    def __init__(
        self,
        ws_url: str = BINANCE_WS_URL,
        combined_ws_url: str = BINANCE_WS_COMBINED_URL,
        max_streams: int = MAX_STREAMS_PER_CONNECTION,
        ping_interval: float = PING_INTERVAL_SECONDS,
        max_control_params: int = MAX_CONTROL_PARAMS_PER_REQUEST,
        rate_budget: WebSocketRateBudget | None = None,
    ) -> None:
        self._validate_public_url(ws_url)
        self._validate_public_url(combined_ws_url)
        if max_streams < 1 or max_control_params < 1:
            raise ValueError("stream and control batch limits must be positive")
        self._ws_url = ws_url
        self._combined_ws_url = combined_ws_url
        self._max_streams = max_streams
        self._max_control_params = max_control_params
        self._ping_interval = ping_interval
        self._clock: Callable[[], datetime] = lambda: datetime.now(UTC)
        self._monotonic_clock: Callable[[], int] = time.monotonic_ns
        self._rate_budget = rate_budget or WebSocketRateBudget(
            monotonic_clock=time.monotonic,
            max_control_messages_per_window=MAX_CONTROL_MESSAGES_PER_SECOND,
        )
        self._connection_id: str | None = None
        self._connected_at: datetime | None = None
        self._ws: Any | None = None
        self._state = ConnectionStateMachine()
        self._receive_sequence = 0
        self._last_frame_received_at: datetime | None = None
        self._active_subscriptions: dict[str, StreamSubscription] = {}
        self._pending_subscriptions: dict[str, StreamSubscription] = {}
        self._pending_unsubscribes: set[str] = set()
        self._control_requests: dict[int, _ControlRequest] = {}
        self._control_errors: dict[int, str] = {}
        self._next_request_id = 1
        self._closing = False
        self._lock = asyncio.Lock()
        self._subscription_observer: Callable[[frozenset[StreamSubscription]], None] | None = None

    def set_clock(self, clock: Callable[[], datetime]) -> None:
        """Inject an aware UTC clock for deterministic lifecycle tests."""
        self._clock = clock

    def set_monotonic_clock(self, clock: Callable[[], int]) -> None:
        """Inject a monotonic nanosecond clock for deterministic raw metadata."""
        self._monotonic_clock = clock

    def set_subscription_observer(
        self,
        observer: Callable[[frozenset[StreamSubscription]], None] | None,
    ) -> None:
        """Observe provider-acknowledged neutral subscription state only."""
        self._subscription_observer = observer

    @property
    def connection_id(self) -> str | None:
        """Return the opaque public-connection identifier."""
        return self._connection_id

    @property
    def state(self) -> ConnectionState:
        """Return current transport lifecycle state."""
        return self._state.state

    @property
    def active_subscriptions(self) -> frozenset[StreamSubscription]:
        """Return only Binance-acknowledged subscriptions."""
        return frozenset(self._active_subscriptions.values())

    @property
    def pending_subscriptions(self) -> frozenset[StreamSubscription]:
        """Return subscriptions awaiting a correlated provider acknowledgement."""
        return frozenset(self._pending_subscriptions.values())

    def control_error(self, request_id: int) -> str | None:
        """Return safe provider rejection text for a completed control request."""
        return self._control_errors.get(request_id)

    async def connect(self) -> ConnectionSnapshot:
        """Open one public combined-stream connection without credentials."""
        async with self._lock:
            if self._state.state not in {"stopped", "backing_off"}:
                raise RuntimeError(f"cannot connect from state: {self._state.state}")
            if not self._rate_budget.reserve_connection_attempt():
                raise WebSocketControlBudgetExceeded("public connection attempt budget exhausted")
            self._closing = False
            self._state.transition_to("connecting", "connect requested")
            self._connection_id = str(uuid.uuid4())
            self._connected_at = self._utc_now()
            self._receive_sequence = 0
            try:
                self._ws = await websockets.connect(
                    self._combined_ws_url,
                    max_size=MAX_MESSAGE_SIZE,
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_interval * 1.5,
                )
            except Exception:
                self._state.transition_to("backing_off", "connect failed")
                self._state.increment_reconnect()
                if self._state.can_reconnect():
                    delay = self._state.calculate_backoff()
                    self._state.set_retry_time(delay)
                raise
            self._state.transition_to("subscribing", "socket opened")
            return self._make_snapshot()

    async def subscribe(self, items: Collection[StreamSubscription]) -> None:
        """Send bounded public ``SUBSCRIBE`` batches and await asynchronous ACKs."""
        await self._send_control("SUBSCRIBE", items)

    async def unsubscribe(self, items: Collection[StreamSubscription]) -> None:
        """Send bounded public ``UNSUBSCRIBE`` batches and await asynchronous ACKs."""
        await self._send_control("UNSUBSCRIBE", items)

    async def _send_control(
        self,
        method: str,
        items: Collection[StreamSubscription],
    ) -> None:
        if not items:
            return
        if self._ws is None:
            raise RuntimeError("not connected")
        pairs = sorted(
            (
                (BinanceSymbolMapper.to_binance_ws_stream(subscription), subscription)
                for subscription in set(items)
            ),
            key=lambda pair: pair[0],
        )
        if method == "SUBSCRIBE":
            projected = len(
                set(self._active_subscriptions)
                .union(self._pending_subscriptions)
                .union(stream for stream, _ in pairs)
            )
            if projected > self._max_streams:
                raise ValueError("subscription exceeds configured connection stream limit")
        for start in range(0, len(pairs), self._max_control_params):
            batch = pairs[start : start + self._max_control_params]
            if not self._rate_budget.reserve_control_message():
                raise WebSocketControlBudgetExceeded("public WebSocket control headroom exhausted")
            request_id = self._next_request_id
            self._next_request_id += 1
            streams = tuple(stream for stream, _ in batch)
            subscriptions = tuple(subscription for _, subscription in batch)
            self._control_requests[request_id] = _ControlRequest(
                method=method,
                streams=streams,
                subscriptions=subscriptions,
            )
            if method == "SUBSCRIBE":
                self._pending_subscriptions.update(dict(batch))
            else:
                self._pending_unsubscribes.update(streams)
            await self._ws.send(
                json.dumps({"method": method, "params": list(streams), "id": request_id})
            )

    async def raw_messages(self) -> AsyncIterator[RawMarketMessage]:
        """Yield raw bytes and safe transport metadata before event normalization.

        Combined envelopes, raw event objects, and malformed market frames are
        all yielded so the canonical pipeline can capture/quarantine them.
        Control acknowledgements are handled locally and are not market events.
        """
        if self._ws is None:
            raise RuntimeError("not connected")
        try:
            async for raw_data in self._ws:
                if self._closing:
                    return
                self._receive_sequence += 1
                received_at = self._utc_now()
                self._last_frame_received_at = received_at
                payload_bytes = raw_data.encode("utf-8") if isinstance(raw_data, str) else raw_data
                if not isinstance(payload_bytes, bytes):
                    payload_bytes = bytes(payload_bytes)
                stream_name = "unknown"
                try:
                    decoded = json.loads(payload_bytes.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    decoded = None
                if isinstance(decoded, dict):
                    if self._is_control_message(decoded):
                        self._handle_control_message(decoded)
                        continue
                    stream_name = self._stream_name_for_payload(decoded)
                message = RawMarketMessage(
                    exchange="binance",
                    market_type="spot",
                    connection_id=self._connection_id or "",
                    stream_name=stream_name,
                    payload_bytes=payload_bytes,
                    received_at=received_at,
                    received_monotonic_ns=self._monotonic_clock(),
                    receive_sequence=self._receive_sequence,
                    source_timestamp_unit="ms",
                )
                if self._state.state == "subscribing" and self._active_subscriptions:
                    self._state.transition_to("streaming", "first market frame")
                    self._state.reset_reconnect()
                    self._state.clear_retry_time()
                yield message
        except websockets.ConnectionClosed as error:
            await self._handle_disconnect(error.reason or "connection closed")
            raise ConnectionError("public Binance WebSocket connection closed") from error

    def _is_control_message(self, payload: dict[str, Any]) -> bool:
        return "id" in payload and ("result" in payload or "code" in payload)

    def _handle_control_message(self, payload: dict[str, Any]) -> None:
        request_id = payload.get("id")
        if isinstance(request_id, bool) or not isinstance(request_id, int):
            return
        request = self._control_requests.pop(request_id, None)
        if request is None:
            return
        if "code" in payload or payload.get("result") is not None:
            detail = str(payload.get("msg", "Binance public control request rejected"))
            self._control_errors[request_id] = detail
            if request.method == "SUBSCRIBE":
                for stream in request.streams:
                    self._pending_subscriptions.pop(stream, None)
            else:
                self._pending_unsubscribes.difference_update(request.streams)
            return
        if request.method == "SUBSCRIBE":
            for stream, subscription in zip(request.streams, request.subscriptions, strict=True):
                self._pending_subscriptions.pop(stream, None)
                self._active_subscriptions[stream] = subscription
        else:
            for stream in request.streams:
                self._pending_unsubscribes.discard(stream)
                self._active_subscriptions.pop(stream, None)
        if self._subscription_observer is not None:
            self._subscription_observer(self.active_subscriptions)

    def _stream_name_for_payload(self, envelope: dict[str, Any]) -> str:
        stream = envelope.get("stream")
        if isinstance(stream, str) and stream:
            return stream.lower()
        payload = envelope.get("data") if "data" in envelope else envelope
        if not isinstance(payload, dict):
            return "unknown"
        event_name = payload.get("e")
        if event_name == "kline" and isinstance(payload.get("k"), dict):
            symbol = payload["k"].get("s")
            interval = payload["k"].get("i")
            suffix = f"kline_{interval}" if isinstance(interval, str) else "kline"
        elif event_name == "trade":
            symbol = payload.get("s")
            suffix = "trade"
        elif event_name == "24hrTicker":
            symbol = payload.get("s")
            suffix = "ticker"
        else:
            return "unknown"
        if not isinstance(symbol, str) or not symbol:
            return "unknown"
        candidate = f"{symbol.lower()}@{suffix}"
        # Raw ``/ws/<stream>`` frames have no combined-envelope stream field.
        # Preserve the inferred provider stream here; the neutral pipeline then
        # validates it against active subscriptions before accepting an event.
        return candidate

    async def _handle_disconnect(self, reason: str) -> None:
        self._state.transition_to("backing_off", reason)
        self._state.increment_reconnect()
        if self._state.can_reconnect():
            self._state.set_retry_time(self._state.calculate_backoff())

    async def snapshot(self) -> ConnectionSnapshot:
        """Return safe, neutral current connection metadata."""
        return self._make_snapshot()

    async def close(self, reason: str) -> None:
        """Close only the public socket and retain no credential state."""
        self._closing = True
        async with self._lock:
            if self._ws is not None:
                await self._ws.close()
                self._ws = None
            self._state.transition_to("stopped", reason)

    def _make_snapshot(self) -> ConnectionSnapshot:
        return ConnectionSnapshot(
            connection_id=self._connection_id or "",
            state=self._state.state,
            connected_at=self._connected_at,
            last_frame_received_at=self._last_frame_received_at,
            active_subscriptions=self.active_subscriptions,
            reconnect_attempt=self._state.reconnect_attempt,
            is_gap_recovery_pending=False,
        )

    def _utc_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Binance WebSocket clock must return a UTC-aware datetime")
        return value.astimezone(UTC)

    @staticmethod
    def _validate_public_url(url: str) -> None:
        parsed = urllib.parse.urlparse(url)
        forbidden = ("listenkey", "userdata", "apikey", "api_key", "signature")
        if parsed.scheme != "wss" or not parsed.hostname:
            raise ValueError("Binance public WebSocket endpoint must use wss")
        if any(token in url.lower() for token in forbidden):
            raise ValueError("private Binance WebSocket configuration is not supported")


def calculate_backoff(
    attempt: int,
    base: float = INITIAL_BACKOFF_SECONDS,
    cap: float = MAX_BACKOFF_SECONDS,
    multiplier: float = BACKOFF_MULTIPLIER,
    jitter: bool = True,
) -> float:
    """Return capped exponential backoff with optional full jitter."""
    if attempt < 0 or base <= 0 or cap <= 0 or multiplier < 1:
        raise ValueError("backoff inputs are invalid")
    delay = min(cap, base * (multiplier**attempt))
    return random.uniform(0.0, delay) if jitter else delay


def is_retry_safe(exception: Exception) -> bool:
    """Return true only for transient transport errors, never arbitrary failures."""
    return isinstance(exception, (ConnectionError, OSError, TimeoutError, asyncio.TimeoutError))
