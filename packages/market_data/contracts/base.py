"""Base market data contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from packages.domain.enums import Timeframe


@dataclass
class MarketDataSource:
    """Represents a source of market data."""

    name: str
    exchange: str
    source_type: str  # websocket, rest, file
    is_connected: bool = False
    last_update: datetime | None = None
    latency_ms: int = 0


@dataclass
class TickerData:
    """Real-time ticker data."""

    symbol: str
    bid_price: Decimal
    ask_price: Decimal
    last_price: Decimal
    volume_24h: Decimal
    quote_volume_24h: Decimal | None = None
    high_24h: Decimal | None = None
    low_24h: Decimal | None = None
    change_24h: Decimal | None = None
    change_percent_24h: Decimal | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    exchange: str = "unknown"

    @property
    def spread(self) -> Decimal:
        """Calculate bid-ask spread."""
        return self.ask_price - self.bid_price

    @property
    def spread_percent(self) -> Decimal:
        """Calculate spread as percentage."""
        if self.last_price == 0:
            return Decimal("0")
        return (self.spread / self.last_price) * Decimal("100")


@dataclass
class TradeData:
    """Individual trade data."""

    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    quote_quantity: Decimal
    timestamp: datetime
    is_buyer_maker: bool
    is_is_trade: bool = False
    exchange: str = "unknown"


@dataclass
class CandleData:
    """OHLCV candle data."""

    symbol: str
    timeframe: Timeframe
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None
    trades_count: int = 0
    is_closed: bool = True
    exchange: str = "unknown"

    @property
    def typical_price(self) -> Decimal:
        """Calculate typical price (HLC average)."""
        return (self.high + self.low + self.close) / Decimal("3")

    @property
    def hlc_range(self) -> Decimal:
        """Calculate HLC range."""
        return self.high - self.low


@dataclass
class OrderBookData:
    """Order book data."""

    symbol: str
    bids: list[tuple[Decimal, Decimal]]  # (price, quantity)
    asks: list[tuple[Decimal, Decimal]]  # (price, quantity)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    last_update_id: int | None = None
    exchange: str = "unknown"

    @property
    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        """Get best bid (highest price)."""
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        """Get best ask (lowest price)."""
        return self.asks[0] if self.asks else None

    @property
    def mid_price(self) -> Decimal | None:
        """Calculate mid price."""
        if self.best_bid and self.best_ask:
            return (self.best_bid[0] + self.best_ask[0]) / Decimal("2")
        return None

    @property
    def spread(self) -> Decimal | None:
        """Calculate spread."""
        if self.best_bid and self.best_ask:
            return self.best_ask[0] - self.best_bid[0]
        return None


@dataclass
class MarketDataSubscription:
    """Subscription to market data."""

    subscription_id: str
    symbol: str
    data_type: str  # ticker, trade, candle, orderbook
    timeframe: Timeframe | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    is_active: bool = True
