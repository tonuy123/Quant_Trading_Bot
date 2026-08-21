"""Position entity - Represents an open trading position."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from packages.domain.entities.base import Entity
from packages.domain.enums import PositionSide
from packages.domain.value_objects import EntityId, Money, Price, Quantity, Symbol


@dataclass
class Position(Entity):
    """Represents an open trading position.

    A position tracks the current state of a held asset:
    - Entry price and quantity
    - Current market value
    - Unrealized and realized PnL
    - Risk metrics
    """

    symbol: Symbol
    side: PositionSide
    quantity: Quantity
    entry_price: Price
    id: EntityId | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    current_price: Price | None = None
    average_fill_price: Price | None = None
    realized_pnl: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    unrealized_pnl: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    commission_paid: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    entry_time: datetime = field(default_factory=datetime.utcnow)
    exit_time: datetime | None = None
    strategy_id: str | None = None
    order_ids: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)

    def _validate(self) -> None:
        """Validate position state."""
        if self.quantity.value <= 0:
            raise ValueError("Position quantity must be positive")

    @property
    def is_long(self) -> bool:
        """Check if position is long."""
        return self.side == PositionSide.LONG

    @property
    def is_short(self) -> bool:
        """Check if position is short."""
        return self.side == PositionSide.SHORT

    @property
    def is_open(self) -> bool:
        """Check if position is still open."""
        return self.exit_time is None

    @property
    def notional_value(self) -> Money:
        """Calculate current notional value."""
        if self.current_price is None:
            return Money(Decimal("0"), self.entry_price.quote_currency)
        return Money(
            self.quantity.value * self.current_price.value, self.current_price.quote_currency
        )

    @property
    def entry_value(self) -> Money:
        """Calculate entry notional value."""
        return Money(self.quantity.value * self.entry_price.value, self.entry_price.quote_currency)

    def update_price(self, price: Price) -> None:
        """Update current market price and recalculate PnL."""
        self.current_price = price
        self._calculate_unrealized_pnl()
        self.mark_updated()

    def _calculate_unrealized_pnl(self) -> None:
        """Calculate unrealized PnL based on current price."""
        if self.current_price is None:
            return

        if self.is_long:
            pnl_value = (self.current_price.value - self.entry_price.value) * self.quantity.value
        else:  # Short
            pnl_value = (self.entry_price.value - self.current_price.value) * self.quantity.value

        self.unrealized_pnl = Money(pnl_value, self.current_price.quote_currency)

    def add_order(self, order_id: str) -> None:
        """Associate an order with this position."""
        if order_id not in self.order_ids:
            self.order_ids.append(order_id)
            self.mark_updated()

    def close(self, exit_price: Price) -> None:
        """Close the position at given price."""
        self.current_price = exit_price
        self.exit_time = datetime.utcnow()
        self._calculate_realized_pnl()
        self._calculate_unrealized_pnl()
        self.mark_updated()

    def _calculate_realized_pnl(self) -> None:
        """Calculate realized PnL when position closes."""
        if self.current_price is None:
            return

        if self.is_long:
            pnl_value = (self.current_price.value - self.entry_price.value) * self.quantity.value
        else:
            pnl_value = (self.entry_price.value - self.current_price.value) * self.quantity.value

        self.realized_pnl = Money(
            pnl_value - self.commission_paid.value, self.current_price.quote_currency
        )

    def add_commission(self, commission: Money) -> None:
        """Add commission to position."""
        self.commission_paid = Money(
            self.commission_paid.value + commission.value, commission.currency
        )
        self.mark_updated()
