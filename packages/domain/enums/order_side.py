"""Order-side enumeration."""

from __future__ import annotations

from enum import StrEnum


class OrderSide(StrEnum):
    """Side of an order."""

    BUY = "BUY"
    SELL = "SELL"

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_buy(self) -> bool:
        """Check if buy side."""
        return self == OrderSide.BUY

    @property
    def is_sell(self) -> bool:
        """Check if sell side."""
        return self == OrderSide.SELL

    def opposite(self) -> OrderSide:
        """Get the opposite side."""
        return OrderSide.SELL if self == OrderSide.BUY else OrderSide.BUY
