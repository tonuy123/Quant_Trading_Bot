"""Money value object - Represents monetary values with currency."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class Money:
    """Represents a monetary value with currency.

    Money is a value object that ensures:
    - Immutable monetary representation
    - Currency-specific operations
    - Proper rounding for financial calculations
    """

    value: Decimal
    currency: str = "USDT"

    def __post_init__(self) -> None:
        """Validate and normalize the monetary value."""
        # Convert from various numeric types
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, "value", Decimal(str(self.value)))

        # Round to reasonable precision (8 decimal places for crypto)
        object.__setattr__(
            self, "value", self.value.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        )

    def __str__(self) -> str:
        """String representation: value currency."""
        return f"{self.value} {self.currency}"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"Money({self.value!r}, {self.currency!r})"

    def __eq__(self, other: object) -> bool:
        """Compare by value and currency."""
        if not isinstance(other, Money):
            return NotImplemented
        return self.value == other.value and self.currency == other.currency

    def __hash__(self) -> int:
        """Hash based on value and currency."""
        return hash((self.value, self.currency))

    def __add__(self, other: Money) -> Money:
        """Add two money values of the same currency."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot add {self.currency} to {other.currency}")
        return Money(self.value + other.value, self.currency)

    def __sub__(self, other: Money) -> Money:
        """Subtract two money values of the same currency."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot subtract {other.currency} from {self.currency}")
        return Money(self.value - other.value, self.currency)

    def __mul__(self, other: Decimal | float | int) -> Money:
        """Multiply money by a scalar."""
        if isinstance(other, float):
            other = Decimal(str(other))
        elif isinstance(other, int):
            other = Decimal(other)
        return Money(self.value * other, self.currency)

    def __truediv__(self, other: Decimal | float | int) -> Money:
        """Divide money by a scalar."""
        if isinstance(other, float):
            other = Decimal(str(other))
        elif isinstance(other, int):
            other = Decimal(other)
        return Money(self.value / other, self.currency)

    def __lt__(self, other: Money) -> bool:
        """Less than comparison."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot compare {self.currency} to {other.currency}")
        return self.value < other.value

    def __le__(self, other: Money) -> bool:
        """Less than or equal comparison."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot compare {self.currency} to {other.currency}")
        return self.value <= other.value

    def __gt__(self, other: Money) -> bool:
        """Greater than comparison."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot compare {self.currency} to {other.currency}")
        return self.value > other.value

    def __ge__(self, other: Money) -> bool:
        """Greater than or equal comparison."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot compare {self.currency} to {other.currency}")
        return self.value >= other.value

    def abs(self) -> Money:
        """Return absolute value."""
        return Money(abs(self.value), self.currency)

    def is_zero(self) -> bool:
        """Check if value is zero."""
        return self.value == Decimal("0")

    def is_positive(self) -> bool:
        """Check if value is positive."""
        return self.value > Decimal("0")

    def is_negative(self) -> bool:
        """Check if value is negative."""
        return self.value < Decimal("0")

    @classmethod
    def zero(cls, currency: str = "USDT") -> Money:
        """Create zero money."""
        return cls(Decimal("0"), currency)

    @classmethod
    def from_float(cls, value: float, currency: str = "USDT") -> Money:
        """Create from float."""
        return cls(Decimal(str(value)), currency)

    def format(self, decimals: int = 2) -> str:
        """Format as string with specified decimal places."""
        formatted = self.value.quantize(Decimal("0." + "0" * decimals), rounding=ROUND_HALF_UP)
        return f"{formatted} {self.currency}"
