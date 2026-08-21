"""Order lifecycle events."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent
from packages.domain.value_objects import Symbol

if TYPE_CHECKING:
    from packages.domain.enums import OrderSide, OrderStatus, OrderType


@dataclass(frozen=True)
class OrderSubmitted(DomainEvent):
    """Event published when an order is submitted to the exchange."""

    event_type: ClassVar[str] = "order_submitted"

    order_id: str
    client_order_id: str | None
    exchange_order_id: str | None
    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    price: Decimal | None = None
    stop_price: Decimal | None = None
    strategy_id: str | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "symbol": str(self.symbol),
            "side": str(self.side),
            "order_type": str(self.order_type),
            "quantity": str(self.quantity),
            "price": str(self.price) if self.price else None,
            "stop_price": str(self.stop_price) if self.stop_price else None,
            "strategy_id": self.strategy_id,
        }


@dataclass(frozen=True)
class OrderUpdated(DomainEvent):
    """Event published when an order is updated."""

    event_type: ClassVar[str] = "order_updated"

    order_id: str
    exchange_order_id: str
    previous_status: OrderStatus
    new_status: OrderStatus
    filled_quantity: Decimal | None = None
    average_fill_price: Decimal | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "order_id": self.order_id,
            "exchange_order_id": self.exchange_order_id,
            "previous_status": str(self.previous_status),
            "new_status": str(self.new_status),
            "filled_quantity": str(self.filled_quantity) if self.filled_quantity else None,
            "average_fill_price": str(self.average_fill_price) if self.average_fill_price else None,
        }
