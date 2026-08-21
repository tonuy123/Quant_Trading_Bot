"""OrderIntent value object - Represents intent to place an order (before risk check)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from packages.domain.enums import OrderSide, OrderType, TimeInForce
    from packages.domain.value_objects import Price, Quantity, Symbol


@dataclass(frozen=True)
class OrderIntent:
    """Represents an intent to place an order.

    OrderIntent is the input to the Risk Manager - it describes
    what the strategy wants to do, BEFORE the risk check.
    If risk approves, the intent becomes an Order.

    This separation ensures:
    - Risk can reject before any exchange call
    - Clear audit trail of what was requested
    - Strategy cannot bypass risk controls
    """

    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Quantity
    price: Price | None = None
    stop_price: Price | None = None
    time_in_force: TimeInForce | None = None
    strategy_id: str | None = None
    strategy_name: str | None = None
    signal_id: str | None = None
    context: dict[str, Any] | None = None  # Additional context for risk analysis
    requested_at: datetime | None = None

    def __post_init__(self) -> None:
        """Set defaults."""
        if self.requested_at is None:
            object.__setattr__(self, "requested_at", datetime.utcnow())

    def __str__(self) -> str:
        """String representation."""
        parts = [f"{self.side.value} {self.quantity} {self.symbol} @ {self.order_type.value}"]
        if self.price:
            parts.append(f"price={self.price}")
        if self.stop_price:
            parts.append(f"stop={self.stop_price}")
        return " ".join(parts)

    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"OrderIntent(symbol={self.symbol!r}, side={self.side!r}, "
            f"type={self.order_type!r}, quantity={self.quantity!r}, "
            f"price={self.price!r}, strategy_id={self.strategy_id!r})"
        )

    @property
    def notional_value(self) -> Decimal:
        """Calculate notional value for risk calculations."""
        if self.price:
            return self.quantity.value * self.price.value
        return Decimal("0")

    @property
    def estimated_commission(self) -> Decimal:
        """Estimate commission for this order."""
        # Rough estimate: 0.1% taker fee
        return self.notional_value * Decimal("0.001")

    def with_context(self, key: str, value: object) -> OrderIntent:
        """Create copy with additional context."""
        new_context = dict(self.context or {})
        new_context[key] = value
        return OrderIntent(
            symbol=self.symbol,
            side=self.side,
            order_type=self.order_type,
            quantity=self.quantity,
            price=self.price,
            stop_price=self.stop_price,
            time_in_force=self.time_in_force,
            strategy_id=self.strategy_id,
            strategy_name=self.strategy_name,
            signal_id=self.signal_id,
            context=new_context,
            requested_at=self.requested_at,
        )
