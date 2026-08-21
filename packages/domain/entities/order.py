"""Order entity - Represents a trading order."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal

from packages.domain.entities.base import Entity
from packages.domain.enums import OrderSide, OrderStatus, OrderType, TimeInForce
from packages.domain.value_objects import EntityId, Money, Price, Quantity, Symbol


@dataclass
class Order(Entity):
    """Represents a trading order.

    An order goes through a lifecycle:
    - PENDING: Order created, awaiting submission
    - SUBMITTED: Order sent to exchange
    - PARTIALLY_FILLED: Some quantity filled
    - FILLED: Fully filled
    - CANCELLED: User cancelled
    - REJECTED: Exchange rejected
    - EXPIRED: Order expired (e.g., time-in-force)
    """

    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Quantity
    id: EntityId | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    price: Price | None = None
    stop_price: Price | None = None
    time_in_force: TimeInForce | None = None
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: Quantity = field(default_factory=lambda: Quantity(Decimal("0")))
    average_fill_price: Price | None = None
    exchange_order_id: str | None = None
    client_order_id: str | None = None
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    cancelled_at: datetime | None = None
    rejection_reason: str | None = None
    commission: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    strategy_id: str | None = None
    tags: list[str] = field(default_factory=list)

    def _validate(self) -> None:
        """Validate order state."""
        if self.quantity.value <= 0:
            raise ValueError("Order quantity must be positive")
        if self.price is not None and self.price.value <= 0:
            raise ValueError("Order price must be positive")
        if self.order_type == OrderType.LIMIT and self.price is None:
            raise ValueError("Limit order must have a price")
        if self.order_type == OrderType.STOP and self.stop_price is None:
            raise ValueError("Stop order must have a stop price")
        if self.order_type == OrderType.STOP_LIMIT and (
            self.price is None or self.stop_price is None
        ):
            raise ValueError("Stop-limit order must have both price and stop price")

    @property
    def remaining_quantity(self) -> Quantity:
        """Calculate remaining quantity to fill."""
        return Quantity(self.quantity.value - self.filled_quantity.value)

    @property
    def is_filled(self) -> bool:
        """Check if order is fully filled."""
        return self.status == OrderStatus.FILLED

    @property
    def is_terminal(self) -> bool:
        """Check if order is in a terminal state."""
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }

    @property
    def fill_percentage(self) -> Decimal:
        """Calculate fill percentage."""
        if self.quantity.value == 0:
            return Decimal("0")
        return (self.filled_quantity.value / self.quantity.value) * Decimal("100")

    def submit(self) -> None:
        """Mark order as submitted to exchange."""
        self.status = OrderStatus.SUBMITTED
        self.submitted_at = datetime.utcnow()
        self.mark_updated()

    def partial_fill(self, quantity: Quantity, price: Price) -> None:
        """Record a partial fill."""
        self.filled_quantity = Quantity(self.filled_quantity.value + quantity.value)
        self.average_fill_price = price  # TODO: calculate weighted average
        self.status = OrderStatus.PARTIALLY_FILLED
        self.mark_updated()

    def fill(self, quantity: Quantity, price: Price) -> None:
        """Record a complete fill."""
        self.filled_quantity = self.quantity
        self.average_fill_price = price
        self.status = OrderStatus.FILLED
        self.filled_at = datetime.utcnow()
        self.mark_updated()

    def cancel(self, reason: str | None = None) -> None:
        """Cancel the order."""
        if self.is_terminal:
            raise ValueError("Cannot cancel order in terminal state")
        self.status = OrderStatus.CANCELLED
        self.cancelled_at = datetime.utcnow()
        self.rejection_reason = reason
        self.mark_updated()

    def reject(self, reason: str) -> None:
        """Reject the order."""
        self.status = OrderStatus.REJECTED
        self.rejection_reason = reason
        self.mark_updated()

    def expire(self) -> None:
        """Expire the order (time-in-force)."""
        self.status = OrderStatus.EXPIRED
        self.cancelled_at = datetime.utcnow()
        self.mark_updated()
