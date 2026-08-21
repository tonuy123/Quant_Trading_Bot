"""Quantity value object - Represents trade quantities."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class Quantity:
    """Represents a trade quantity.

    Quantity is different from Money because:
    - Different assets have different precision requirements
    - e.g., BTC uses 8 decimals, but some tokens use 0 decimals
    """

    value: Decimal
    precision: int = 8  # Decimal places

    def __post_init__(self) -> None:
        """Validate and normalize the quantity value."""
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, "value", Decimal(str(self.value)))

        # Round down for quantities (can't give more than you have)
        precision_factor = Decimal("0." + "0" * self.precision + "1")
        object.__setattr__(
            self, "value", self.value.quantize(precision_factor, rounding=ROUND_DOWN)
        )

    def __str__(self) -> str:
        """String representation."""
        return f"{self.value}"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"Quantity({self.value!r}, precision={self.precision})"

    def __eq__(self, other: object) -> bool:
        """Compare by value."""
        if not isinstance(other, Quantity):
            return NotImplemented
        return self.value == other.value

    def __hash__(self) -> int:
        """Hash based on value."""
        return hash(self.value)

    def __lt__(self, other: Quantity) -> bool:
        """Less than comparison."""
        return self.value < other.value

    def __le__(self, other: Quantity) -> bool:
        """Less than or equal comparison."""
        return self.value <= other.value

    def __gt__(self, other: Quantity) -> bool:
        """Greater than comparison."""
        return self.value > other.value

    def __ge__(self, other: Quantity) -> bool:
        """Greater than or equal comparison."""
        return self.value >= other.value

    def __add__(self, other: Quantity) -> Quantity:
        """Add quantities."""
        return Quantity(self.value + other.value, max(self.precision, other.precision))

    def __sub__(self, other: Quantity) -> Quantity:
        """Subtract quantities."""
        if self.value < other.value:
            raise ValueError("Cannot subtract larger quantity from smaller")
        return Quantity(self.value - other.value, max(self.precision, other.precision))

    def __mul__(self, other: Decimal | float | int) -> Quantity:
        """Multiply quantity by scalar."""
        if isinstance(other, float):
            other = Decimal(str(other))
        elif isinstance(other, int):
            other = Decimal(other)
        return Quantity(self.value * other, self.precision)

    def __truediv__(self, other: Decimal | float | int) -> Quantity:
        """Divide quantity by scalar."""
        if isinstance(other, float):
            other = Decimal(str(other))
        elif isinstance(other, int):
            other = Decimal(other)
        return Quantity(self.value / other, self.precision)

    def is_zero(self) -> bool:
        """Check if quantity is zero."""
        return self.value == Decimal("0")

    def is_positive(self) -> bool:
        """Check if quantity is positive."""
        return self.value > Decimal("0")

    @classmethod
    def zero(cls, precision: int = 8) -> Quantity:
        """Create zero quantity."""
        return cls(Decimal("0"), precision)

    @classmethod
    def from_float(cls, value: float, precision: int = 8) -> Quantity:
        """Create from float."""
        return cls(Decimal(str(value)), precision)

    def round_to_precision(self, precision: int) -> Quantity:
        """Return new quantity with different precision."""
        return Quantity(self.value, precision)

    def floor(self) -> int:
        """Return floor as integer."""
        return int(self.value.to_integral_value(rounding="ROUND_DOWN"))

    def format(self) -> str:
        """Format as string, removing trailing zeros."""
        return f"{self.value.normalize()}"
