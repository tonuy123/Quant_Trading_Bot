"""Price value object - Represents a price with base and quote currency."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

from packages.domain.value_objects.money import Money

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class Price:
    """Represents a price with base and quote currency.

    Price differs from Money in that it represents:
    - The price of one asset in terms of another
    - e.g., BTC/USDT = 50000.00
    """

    value: Decimal
    base_currency: str  # e.g., "BTC"
    quote_currency: str  # e.g., "USDT"

    def __post_init__(self) -> None:
        """Validate and normalize the price value."""
        if not isinstance(self.value, Decimal):
            object.__setattr__(self, "value", Decimal(str(self.value)))

        # High precision for prices (8 decimal places)
        object.__setattr__(
            self, "value", self.value.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        )

    def __str__(self) -> str:
        """String representation: value base/quote."""
        return f"{self.value} {self.base_currency}/{self.quote_currency}"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"Price({self.value!r}, {self.base_currency!r}, {self.quote_currency!r})"

    def __eq__(self, other: object) -> bool:
        """Compare by value and currency pair."""
        if not isinstance(other, Price):
            return NotImplemented
        return (
            self.value == other.value
            and self.base_currency == other.base_currency
            and self.quote_currency == other.quote_currency
        )

    def __hash__(self) -> int:
        """Hash based on value and currency pair."""
        return hash((self.value, self.base_currency, self.quote_currency))

    def __lt__(self, other: Price) -> bool:
        """Less than comparison."""
        self._ensure_same_pair(other)
        return self.value < other.value

    def __le__(self, other: Price) -> bool:
        """Less than or equal comparison."""
        self._ensure_same_pair(other)
        return self.value <= other.value

    def __gt__(self, other: Price) -> bool:
        """Greater than comparison."""
        self._ensure_same_pair(other)
        return self.value > other.value

    def __ge__(self, other: Price) -> bool:
        """Greater than or equal comparison."""
        self._ensure_same_pair(other)
        return self.value >= other.value

    def __add__(self, other: Price) -> Price:
        """Add two prices of the same pair."""
        self._ensure_same_pair(other)
        return Price(self.value + other.value, self.base_currency, self.quote_currency)

    def __sub__(self, other: Price) -> Price:
        """Subtract two prices of the same pair."""
        self._ensure_same_pair(other)
        return Price(self.value - other.value, self.base_currency, self.quote_currency)

    def _ensure_same_pair(self, other: Price) -> None:
        """Ensure both prices are for the same pair."""
        if self.base_currency != other.base_currency or self.quote_currency != other.quote_currency:
            raise ValueError(
                f"Cannot compare {self.base_currency}/{self.quote_currency} to {other.base_currency}/{other.quote_currency}"
            )

    @property
    def symbol(self) -> str:
        """Get the trading pair symbol."""
        return f"{self.base_currency}{self.quote_currency}"

    def to_money(self) -> Money:
        """Convert price to Money (uses quote currency)."""
        return Money(self.value, self.quote_currency)

    def invert(self) -> Price:
        """Invert the price (1/price)."""
        return Price(
            (Decimal("1") / self.value).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP),
            self.quote_currency,
            self.base_currency,
        )

    @classmethod
    def from_float(cls, value: float, base: str, quote: str) -> Price:
        """Create from float."""
        return cls(Decimal(str(value)), base, quote)

    def format(self, decimals: int = 2) -> str:
        """Format as string with specified decimal places."""
        formatted = self.value.quantize(Decimal("0." + "0" * decimals), rounding=ROUND_HALF_UP)
        return f"{formatted} {self.base_currency}/{self.quote_currency}"
