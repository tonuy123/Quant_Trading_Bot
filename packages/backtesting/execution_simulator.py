"""Execution simulator for backtesting."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from packages.domain.entities.order import Order


class ExecutionSimulator:
    """Simulates order execution for backtesting.

    The simulator:
    1. Receives order intents
    2. Simulates fills based on market data
    3. Applies slippage and commission
    4. Returns simulated fills
    """

    def __init__(
        self,
        commission: Decimal = Decimal("0.001"),
        slippage: Decimal = Decimal("0.0005"),
    ) -> None:
        """Initialize simulator.

        Args:
            commission: Commission rate
            slippage: Slippage rate
        """
        self.commission = commission
        self.slippage = slippage

    def simulate_fill(
        self,
        order: Order,
        current_price: Decimal,
        timestamp: datetime,
    ) -> dict[str, object] | None:
        """Simulate fill for an order.

        Args:
            order: Order to fill
            current_price: Current market price
            timestamp: Fill timestamp

        Returns:
            Fill data if filled.
        """
        # Check if order should fill based on type
        if order.order_type == "MARKET":
            fill_price = self._apply_slippage(current_price, order.side.value)
            return self._create_fill(order, fill_price, timestamp)

        if order.order_type == "LIMIT":
            if order.price is None:
                return None
            if order.side.value == "BUY" and current_price <= order.price.value:
                fill_price = min(current_price, order.price.value)
                return self._create_fill(order, fill_price, timestamp)
            if order.side.value == "SELL" and current_price >= order.price.value:
                fill_price = max(current_price, order.price.value)
                return self._create_fill(order, fill_price, timestamp)

        return None

    def _apply_slippage(self, price: Decimal, side: str) -> Decimal:
        """Apply slippage to price."""
        slippage_amount = price * self.slippage
        if side == "BUY":
            return price + slippage_amount
        return price - slippage_amount

    def _create_fill(self, order: Order, price: Decimal, timestamp: datetime) -> dict[str, Any]:
        """Create fill data."""
        quote_value = price * order.quantity.value
        commission = quote_value * self.commission

        return {
            "order_id": str(order.id),
            "symbol": str(order.symbol),
            "side": order.side.value,
            "quantity": order.quantity.value,
            "price": price,
            "quote_quantity": quote_value,
            "commission": commission,
            "timestamp": timestamp,
        }
