"""Canonical, exchange-neutral Market Data event contracts.

Legacy DTOs in packages.market_data.contracts.base remain only for backward
compatibility. New ingestion code emits the immutable schemas in this module.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import ClassVar, Literal

from packages.market_data.adapters.value_types import (
    ConnectionState,
    ExchangeId,
    IngestionSource,
    MarketSymbol,
    MarketType,
    StreamKind,
)


def _require_utc(name: str, value: datetime | None) -> None:
    """Reject absent or naive timestamps at the canonical event boundary."""
    if value is None or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be UTC-aware")


def _require_finite(name: str, value: Decimal, *, positive: bool = False) -> None:
    """Validate a financial Decimal without accepting non-finite values."""
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if positive and value <= Decimal("0"):
        raise ValueError(f"{name} must be positive")


def _next_candle_boundary(open_time: datetime, interval: str) -> datetime:
    """Return the exact exclusive close boundary for a supported interval."""
    if len(interval) < 2:
        raise ValueError("candle interval is invalid")
    try:
        amount = int(interval[:-1])
    except ValueError as error:
        raise ValueError("candle interval is invalid") from error
    if amount < 1:
        raise ValueError("candle interval is invalid")
    seconds_by_unit = {"s": 1, "m": 60, "h": 3_600, "d": 86_400, "w": 604_800}
    unit = interval[-1]
    if unit in seconds_by_unit:
        return open_time + timedelta(seconds=amount * seconds_by_unit[unit])
    if (
        unit != "M"
        or open_time.day != 1
        or any((open_time.hour, open_time.minute, open_time.second, open_time.microsecond))
    ):
        raise ValueError("unsupported candle interval boundary")
    next_month = open_time.month + amount
    year = open_time.year + (next_month - 1) // 12
    month = (next_month - 1) % 12 + 1
    return open_time.replace(year=year, month=month)


@dataclass(frozen=True, kw_only=True)
class MarketEvent:
    """Common immutable provenance envelope for canonical market events."""

    event_id: str
    exchange: ExchangeId
    market_type: MarketType
    symbol: MarketSymbol | None
    source: IngestionSource
    occurred_at: datetime
    exchange_event_at: datetime | None
    received_at: datetime
    published_at: datetime | None = None
    connection_id: str | None = None
    receive_sequence: int | None = None
    raw_message_id: str | None = None
    quality_flags: frozenset[str] = field(default_factory=frozenset)
    schema_version: int = field(default=1, init=False)

    EVENT_TYPE: ClassVar[str] = "market.unknown"

    def __post_init__(self) -> None:
        """Validate fields shared by all event kinds."""
        if not self.event_id:
            raise ValueError("event_id must be non-empty")
        _require_utc("occurred_at", self.occurred_at)
        _require_utc("received_at", self.received_at)
        if self.exchange_event_at is not None:
            _require_utc("exchange_event_at", self.exchange_event_at)
        if self.published_at is not None:
            _require_utc("published_at", self.published_at)

    @property
    def event_type(self) -> str:
        """Return the stable event type identifier."""
        return self.EVENT_TYPE


@dataclass(frozen=True, kw_only=True)
class TradeEvent(MarketEvent):
    """A public individual-trade event."""

    trade_id: str
    price: Decimal
    quantity: Decimal
    quote_quantity: Decimal
    is_buyer_maker: bool
    first_aggregated_trade_id: str | None = None
    last_aggregated_trade_id: str | None = None

    EVENT_TYPE: ClassVar[str] = "market.trade"

    def __post_init__(self) -> None:
        """Validate a trade's canonical invariants."""
        super().__post_init__()
        if self.symbol is None:
            raise ValueError("trade symbol is required")
        if not self.trade_id:
            raise ValueError("trade_id must be non-empty")
        _require_finite("price", self.price, positive=True)
        _require_finite("quantity", self.quantity, positive=True)
        _require_finite("quote_quantity", self.quote_quantity, positive=True)


@dataclass(frozen=True, kw_only=True)
class TickerEvent(MarketEvent):
    """A rolling-window public ticker state update."""

    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    last_price: Decimal
    last_quantity: Decimal
    window_open_at: datetime
    window_close_at: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    first_trade_id: str
    last_trade_id: str
    trade_count: int

    EVENT_TYPE: ClassVar[str] = "market.ticker"

    def __post_init__(self) -> None:
        """Validate ticker identity, time and financial invariants."""
        super().__post_init__()
        if self.symbol is None:
            raise ValueError("ticker symbol is required")
        for name, value in (
            ("bid_price", self.bid_price),
            ("ask_price", self.ask_price),
            ("last_price", self.last_price),
            ("open_price", self.open_price),
            ("high_price", self.high_price),
            ("low_price", self.low_price),
        ):
            _require_finite(name, value, positive=True)
        for name, value in (
            ("bid_quantity", self.bid_quantity),
            ("ask_quantity", self.ask_quantity),
            ("last_quantity", self.last_quantity),
            ("base_volume", self.base_volume),
            ("quote_volume", self.quote_volume),
        ):
            _require_finite(name, value)
            if value < Decimal("0"):
                raise ValueError(f"{name} must be non-negative")
        if self.bid_price > self.ask_price:
            raise ValueError("bid_price must not exceed ask_price")
        if self.high_price < max(self.open_price, self.last_price, self.low_price):
            raise ValueError("high_price is inconsistent with ticker prices")
        if self.low_price > min(self.open_price, self.last_price, self.high_price):
            raise ValueError("low_price is inconsistent with ticker prices")
        _require_utc("window_open_at", self.window_open_at)
        _require_utc("window_close_at", self.window_close_at)
        if self.window_close_at < self.window_open_at:
            raise ValueError("ticker window_close_at precedes window_open_at")
        if self.trade_count < 0:
            raise ValueError("trade_count must be non-negative")


@dataclass(frozen=True, kw_only=True)
class CandleClosedEvent(MarketEvent):
    """An exchange-confirmed, closed OHLCV interval."""

    interval: str
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    base_volume: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    is_closed: Literal[True] = True

    EVENT_TYPE: ClassVar[str] = "market.candle.closed"

    def __post_init__(self) -> None:
        """Validate exact closed-candle invariants."""
        super().__post_init__()
        if self.symbol is None:
            raise ValueError("candle symbol is required")
        if not self.interval:
            raise ValueError("candle interval is required")
        if not self.is_closed:
            raise ValueError("canonical candle event must be closed")
        _require_utc("open_time", self.open_time)
        _require_utc("close_time", self.close_time)
        if self.close_time <= self.open_time:
            raise ValueError("candle close_time must follow open_time")
        if _next_candle_boundary(self.open_time, self.interval) != self.close_time:
            raise ValueError("candle close_time does not match its interval boundary")
        for name, value in (
            ("open_price", self.open_price),
            ("high_price", self.high_price),
            ("low_price", self.low_price),
            ("close_price", self.close_price),
        ):
            _require_finite(name, value, positive=True)
        if self.high_price < max(self.open_price, self.close_price, self.low_price):
            raise ValueError("high_price is inconsistent with OHLC")
        if self.low_price > min(self.open_price, self.close_price, self.high_price):
            raise ValueError("low_price is inconsistent with OHLC")
        for name, value in (
            ("base_volume", self.base_volume),
            ("quote_volume", self.quote_volume),
            ("taker_buy_base_volume", self.taker_buy_base_volume),
            ("taker_buy_quote_volume", self.taker_buy_quote_volume),
        ):
            _require_finite(name, value)
            if value < Decimal("0"):
                raise ValueError(f"{name} must be non-negative")
        if self.trade_count < 0:
            raise ValueError("trade_count must be non-negative")


@dataclass(frozen=True, kw_only=True)
class ConnectionStatusChanged(MarketEvent):
    """A safe connection lifecycle transition."""

    previous_state: ConnectionState
    current_state: ConnectionState
    reason_code: str
    reconnect_attempt: int
    next_retry_at: datetime | None
    endpoint_label: str
    subscriptions_affected: int

    EVENT_TYPE: ClassVar[str] = "market.connection.status_changed"

    def __post_init__(self) -> None:
        """Validate connection status metadata."""
        super().__post_init__()
        if not self.reason_code or not self.endpoint_label:
            raise ValueError("reason_code and endpoint_label are required")
        if self.reconnect_attempt < 0 or self.subscriptions_affected < 0:
            raise ValueError("connection counters must be non-negative")
        if self.next_retry_at is not None:
            _require_utc("next_retry_at", self.next_retry_at)


@dataclass(frozen=True, kw_only=True)
class DataGapDetected(MarketEvent):
    """An observable possible or confirmed market-data loss interval."""

    gap_id: str
    stream_kind: StreamKind
    interval: str | None
    gap_start_at: datetime
    gap_end_at: datetime | None
    detection_basis: Literal[
        "connection_interruption",
        "missing_kline_slot",
        "sequence_discontinuity",
        "recovery_failure",
    ]
    certainty: Literal["potential", "confirmed"]
    recoverability: Literal["closed_candles", "snapshot_only", "none"]
    last_known_cursor: str | None
    affected_subscription_count: int

    EVENT_TYPE: ClassVar[str] = "market.data_gap.detected"

    def __post_init__(self) -> None:
        """Validate gap boundaries and state."""
        super().__post_init__()
        if not self.gap_id:
            raise ValueError("gap_id must be non-empty")
        _require_utc("gap_start_at", self.gap_start_at)
        if self.gap_end_at is not None:
            _require_utc("gap_end_at", self.gap_end_at)
            if self.gap_end_at < self.gap_start_at:
                raise ValueError("gap_end_at must not precede gap_start_at")
        if self.stream_kind == "kline" and not self.interval:
            raise ValueError("kline gaps require an interval")
        if self.affected_subscription_count < 1:
            raise ValueError("affected_subscription_count must be positive")


@dataclass(frozen=True, kw_only=True)
class MarketDataStale(MarketEvent):
    """A per-subscription stale-data episode or controlled reminder."""

    stream_kind: StreamKind
    interval: str | None
    last_valid_received_at: datetime | None
    stale_for_ms: int
    threshold_ms: int
    connection_state: ConnectionState
    reason_code: Literal[
        "no_valid_frame",
        "connection_down",
        "recovery_pending",
        "rate_limit_cooldown",
        "clock_untrusted",
    ]

    EVENT_TYPE: ClassVar[str] = "market.data.stale"

    def __post_init__(self) -> None:
        """Validate stale event fields."""
        super().__post_init__()
        if self.last_valid_received_at is not None:
            _require_utc("last_valid_received_at", self.last_valid_received_at)
        if self.stream_kind == "kline" and not self.interval:
            raise ValueError("kline stale event requires an interval")
        if self.stale_for_ms < 0 or self.threshold_ms < 1:
            raise ValueError("staleness values are invalid")
