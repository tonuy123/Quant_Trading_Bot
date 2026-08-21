"""Value types for market data adapters.

These are frozen dataclasses representing exchange-neutral concepts
that never leak provider-specific field names or symbols across
the adapter boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

# =============================================================================
# Literal types
# =============================================================================

ExchangeId = Literal["binance"]
MarketType = Literal["spot"]
StreamKind = Literal["trade", "ticker", "kline"]
IngestionSource = Literal["websocket", "rest_gap_recovery", "rest_snapshot"]
ConnectionState = Literal[
    "stopped",
    "connecting",
    "subscribing",
    "streaming",
    "backing_off",
    "rotating",
    "stopping",
]
GapRecoverability = Literal["closed_candles", "snapshot_only", "none"]


# =============================================================================
# MarketSymbol - Exchange-neutral symbol representation
# =============================================================================


@dataclass(frozen=True)
class MarketSymbol:
    """Exchange-neutral symbol representation.

    The canonical form is BASE/QUOTE (e.g., BTC/USDT).
    The adapter translates this to provider-specific symbols.
    """

    base: str
    quote: str

    def __post_init__(self) -> None:
        base = self.base.strip().upper()
        quote = self.quote.strip().upper()
        if not base or not quote:
            raise ValueError("base and quote must be non-empty")
        if not base.isalnum() or not quote.isalnum():
            raise ValueError("base and quote must be alphanumeric")
        object.__setattr__(self, "base", base)
        object.__setattr__(self, "quote", quote)

    @property
    def canonical(self) -> str:
        """Return canonical BASE/QUOTE representation."""
        return f"{self.base}/{self.quote}"

    def __str__(self) -> str:
        return self.canonical

    def __repr__(self) -> str:
        return f"MarketSymbol(base={self.base!r}, quote={self.quote!r})"


# =============================================================================
# StreamSubscription - Subscription request
# =============================================================================


@dataclass(frozen=True)
class StreamSubscription:
    """A subscription request for a specific stream.

    This is the exchange-neutral subscription unit used across
    the adapter boundary. The adapter translates this to
    provider-specific stream names.
    """

    exchange: ExchangeId
    market_type: MarketType
    symbol: MarketSymbol
    kind: StreamKind
    interval: str | None = None  # e.g., "1m", "5m", "1h" for kline

    def __post_init__(self) -> None:
        if self.exchange != "binance":
            raise ValueError("only public binance subscriptions are supported")
        if self.market_type != "spot":
            raise ValueError("only spot market subscriptions are supported")
        if self.kind == "kline" and self.interval is None:
            raise ValueError("kline subscriptions require an interval")
        if self.kind != "kline" and self.interval is not None:
            raise ValueError("only kline subscriptions may define an interval")
        if self.interval is not None and not self.interval:
            raise ValueError("interval must be non-empty")

    @property
    def key(self) -> str:
        """Unique key for this subscription."""
        parts = [self.exchange, self.market_type, self.symbol.canonical, self.kind]
        if self.interval:
            parts.append(self.interval)
        return ":".join(parts)

    def __str__(self) -> str:
        base = f"{self.exchange}:{self.market_type}:{self.symbol.canonical}:{self.kind}"
        if self.interval:
            base += f":{self.interval}"
        return base

    def __repr__(self) -> str:
        return (
            f"StreamSubscription(exchange={self.exchange!r}, "
            f"market_type={self.market_type!r}, symbol={self.symbol!r}, "
            f"kind={self.kind!r}, interval={self.interval!r})"
        )


# =============================================================================
# RawMarketMessage - Raw ingress message with metadata
# =============================================================================


@dataclass(frozen=True)
class RawMarketMessage:
    """Raw market message captured at ingress.

    Contains the exact provider payload bytes and metadata.
    This is the ingress boundary type - it preserves provider
    shape for replay and debug purposes.
    """

    exchange: ExchangeId
    market_type: MarketType
    connection_id: str
    stream_name: str  # Provider stream name (e.g., "btcusdt@trade")
    payload_bytes: bytes
    received_at: datetime  # UTC-aware wall time
    received_monotonic_ns: int  # Monotonic nanoseconds at receive
    receive_sequence: int  # Per-connection sequence number
    source_timestamp_unit: Literal["ms", "us", "unknown"] = "unknown"

    def __post_init__(self) -> None:
        if self.received_at.tzinfo is None or self.received_at.utcoffset() is None:
            raise ValueError("received_at must be UTC-aware")
        if self.receive_sequence < 1:
            raise ValueError("receive_sequence must be positive")
        if self.received_monotonic_ns < 0:
            raise ValueError("received_monotonic_ns must be non-negative")

    @property
    def payload_text(self) -> str:
        """Decode payload as text."""
        return self.payload_bytes.decode("utf-8")

    @property
    def message_id(self) -> str:
        """Unique message ID for deduplication."""
        return f"{self.connection_id}:{self.receive_sequence}"


# =============================================================================
# ConnectionSnapshot - Current connection state
# =============================================================================


@dataclass(frozen=True)
class ConnectionSnapshot:
    """Snapshot of connection state at a point in time."""

    connection_id: str
    state: ConnectionState
    connected_at: datetime | None = None
    last_frame_received_at: datetime | None = None
    active_subscriptions: frozenset[StreamSubscription] = field(default_factory=frozenset)
    reconnect_attempt: int = 0
    is_gap_recovery_pending: bool = False

    def __post_init__(self) -> None:
        if self.connected_at is not None and (
            self.connected_at.tzinfo is None or self.connected_at.utcoffset() is None
        ):
            raise ValueError("connected_at must be UTC-aware")
        if self.last_frame_received_at is not None and (
            self.last_frame_received_at.tzinfo is None
            or self.last_frame_received_at.utcoffset() is None
        ):
            raise ValueError("last_frame_received_at must be UTC-aware")

    @property
    def is_healthy(self) -> bool:
        """Check if connection is in a healthy streaming state."""
        return self.state == "streaming" and self.last_frame_received_at is not None
