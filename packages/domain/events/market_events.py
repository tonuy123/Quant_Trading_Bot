"""Market data events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent
from packages.domain.value_objects import Symbol

if TYPE_CHECKING:
    from packages.domain.enums import Timeframe


@dataclass(frozen=True)
class MarketTick(DomainEvent):
    """Event published when a new market tick is received.

    This is the most granular market data event, published
    on every trade that occurs on the exchange.
    """

    symbol: Symbol
    price: Decimal
    quantity: Decimal
    trade_id: str
    event_type: ClassVar[str] = "market_tick"
    is_buyer_maker: bool = False
    exchange_timestamp: datetime | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "symbol": str(self.symbol),
            "price": str(self.price),
            "quantity": str(self.quantity),
            "trade_id": self.trade_id,
            "is_buyer_maker": self.is_buyer_maker,
            "exchange_timestamp": self.exchange_timestamp.isoformat()
            if self.exchange_timestamp
            else None,
        }


@dataclass(frozen=True)
class CandleClosed(DomainEvent):
    """Event published when a candle is closed.

    Candles close when:
    - The timeframe period ends
    - The exchange confirms the close

    This is the primary event for strategy evaluation.
    """

    symbol: Symbol
    timeframe: Timeframe
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    event_type: ClassVar[str] = "candle_closed"
    trades_count: int = 0

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "symbol": str(self.symbol),
            "timeframe": str(self.timeframe),
            "open_time": self.open_time.isoformat(),
            "close_time": self.close_time.isoformat(),
            "open": str(self.open_price),
            "high": str(self.high_price),
            "low": str(self.low_price),
            "close": str(self.close_price),
            "volume": str(self.volume),
            "trades_count": self.trades_count,
        }


@dataclass(frozen=True)
class PriceUpdated(DomainEvent):
    """Event published when the best bid/ask price is updated."""

    symbol: Symbol
    bid_price: Decimal
    ask_price: Decimal
    event_type: ClassVar[str] = "price_updated"
    bid_quantity: Decimal | None = None
    ask_quantity: Decimal | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "symbol": str(self.symbol),
            "bid_price": str(self.bid_price),
            "ask_price": str(self.ask_price),
            "bid_quantity": str(self.bid_quantity) if self.bid_quantity else None,
            "ask_quantity": str(self.ask_quantity) if self.ask_quantity else None,
        }
