"""Position-side enumeration."""

from __future__ import annotations

from enum import StrEnum


class PositionSide(StrEnum):
    """Side of a position."""

    LONG = "LONG"  # Bought, profit from price increase
    SHORT = "SHORT"  # Borrowed and sold, profit from price decrease
    BOTH = "BOTH"  # USDT futures with both long and short

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_long(self) -> bool:
        """Check if long side."""
        return self == PositionSide.LONG

    @property
    def is_short(self) -> bool:
        """Check if short side."""
        return self == PositionSide.SHORT

    @classmethod
    def from_order_side(cls, side: str) -> PositionSide:
        """Derive position side from order side."""
        if side == "BUY":
            return cls.LONG
        elif side == "SELL":
            return cls.SHORT
        raise ValueError(f"Unknown order side: {side}")
