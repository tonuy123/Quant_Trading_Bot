"""Tick entity - Represents a single market trade tick."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from packages.domain.entities.base import Entity
from packages.domain.value_objects import EntityId, Price, Quantity, Symbol

if TYPE_CHECKING:
    pass


@dataclass
class Tick(Entity):
    """Represents a single market trade tick.

    A tick is the most granular market data:
    - Price and quantity of a single trade
    - Timestamp
    - Trade direction (buyer/seller initiated)
    """

    symbol: Symbol
    price: Price
    quantity: Quantity
    timestamp: datetime
    trade_id: str
    id: EntityId | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    is_buyer_maker: bool = False  # True if buyer was the maker (price moved down)
    quote_quantity: Quantity | None = None
    is_is_trade: bool = False  # Is this a block trade?

    def _validate(self) -> None:
        """Validate tick data."""
        if self.price.value <= 0:
            raise ValueError("Price must be positive")
        if self.quantity.value <= 0:
            raise ValueError("Quantity must be positive")
        if self.timestamp > datetime.utcnow():
            raise ValueError("Timestamp cannot be in the future")

    @property
    def quote_value(self) -> Decimal:
        """Calculate quote value (price * quantity)."""
        return self.price.value * self.quantity.value

    @property
    def side(self) -> str:
        """Infer trade side from is_buyer_maker.

        Note: This is approximate - buyer_maker means the buyer was the taker
        (initiated the aggressor side), which means the price moved down.
        """
        if self.is_buyer_maker:
            return "SELL"  # Price dropped, so it was a sell-initiated trade
        return "BUY"  # Price rose, so it was a buy-initiated trade

    def to_array(self) -> tuple[datetime, Price, Quantity]:
        """Convert to tuple for time series."""
        return (self.timestamp, self.price, self.quantity)
