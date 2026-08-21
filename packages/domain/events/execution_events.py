"""Execution and fill events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent
from packages.domain.value_objects import Symbol

if TYPE_CHECKING:
    from packages.domain.enums import OrderSide


@dataclass(frozen=True)
class FillReceived(DomainEvent):
    """Event published when a fill is received from the exchange."""

    event_type: ClassVar[str] = "fill_received"

    order_id: str
    exchange_order_id: str
    symbol: Symbol
    side: OrderSide
    quantity: Decimal
    price: Decimal
    quote_quantity: Decimal
    commission: Decimal
    commission_currency: str
    trade_id: str
    exchange_timestamp: datetime | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "order_id": self.order_id,
            "exchange_order_id": self.exchange_order_id,
            "symbol": str(self.symbol),
            "side": str(self.side),
            "quantity": str(self.quantity),
            "price": str(self.price),
            "quote_quantity": str(self.quote_quantity),
            "commission": str(self.commission),
            "commission_currency": self.commission_currency,
            "trade_id": self.trade_id,
            "exchange_timestamp": self.exchange_timestamp.isoformat()
            if self.exchange_timestamp
            else None,
        }


@dataclass(frozen=True)
class OrderFilled(DomainEvent):
    """Event published when an order is completely filled."""

    event_type: ClassVar[str] = "order_filled"

    order_id: str
    exchange_order_id: str
    symbol: Symbol
    side: OrderSide
    filled_quantity: Decimal
    average_fill_price: Decimal
    total_commission: Decimal
    commission_currency: str
    filled_at: datetime

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "order_id": self.order_id,
            "exchange_order_id": self.exchange_order_id,
            "symbol": str(self.symbol),
            "side": str(self.side),
            "filled_quantity": str(self.filled_quantity),
            "average_fill_price": str(self.average_fill_price),
            "total_commission": str(self.total_commission),
            "commission_currency": self.commission_currency,
            "filled_at": self.filled_at.isoformat(),
        }


@dataclass(frozen=True)
class OrderCancelled(DomainEvent):
    """Event published when an order is cancelled."""

    event_type: ClassVar[str] = "order_cancelled"

    order_id: str
    exchange_order_id: str
    symbol: Symbol
    cancelled_quantity: Decimal
    filled_quantity: Decimal
    reason: str | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "order_id": self.order_id,
            "exchange_order_id": self.exchange_order_id,
            "symbol": str(self.symbol),
            "cancelled_quantity": str(self.cancelled_quantity),
            "filled_quantity": str(self.filled_quantity),
            "reason": self.reason,
        }
