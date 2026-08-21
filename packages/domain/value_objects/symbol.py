"""Symbol value object - Represents a trading pair symbol."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class Symbol:
    """Represents a trading pair symbol.

    Examples:
        - BTCUSDT (Binance)
        - BTC-USDT (standard format)
        - BTC/USDT (trading format)
    """

    value: str
    base: str = ""  # e.g., "BTC"
    quote: str = ""  # e.g., "USDT"
    separator: str = ""

    def __post_init__(self) -> None:
        """Normalize symbol format."""
        # Uppercase
        object.__setattr__(self, "value", self.value.upper())

        # Extract base and quote if not provided
        if not self.base or not self.quote:
            # Try common quote currencies first
            quotes = ["USDT", "USDC", "BUSD", "BTC", "ETH", "BNB", "USD", "EUR", "GBP"]
            base_found = False

            for quote in quotes:
                if self.value.endswith(quote) and len(self.value) > len(quote):
                    base = self.value[: -len(quote)]
                    if base.isalpha():  # Ensure base is alphabetic
                        object.__setattr__(self, "base", base.upper())
                        object.__setattr__(self, "quote", quote.upper())
                        base_found = True
                        break

            if not base_found:
                # Fallback: assume last 3-4 chars are quote
                for sep_len in [4, 3]:
                    if len(self.value) > sep_len:
                        possible_quote = self.value[-sep_len:]
                        possible_base = self.value[:-sep_len]
                        if possible_base.isalpha() and possible_quote.isalpha():
                            object.__setattr__(self, "base", possible_base.upper())
                            object.__setattr__(self, "quote", possible_quote.upper())
                            break

    def __str__(self) -> str:
        """String representation."""
        if self.separator:
            return f"{self.base}{self.separator}{self.quote}"
        return self.value

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"Symbol({self.value!r}, base={self.base!r}, quote={self.quote!r})"

    def __eq__(self, other: object) -> bool:
        """Compare by value (case-insensitive)."""
        if not isinstance(other, Symbol):
            if isinstance(other, str):
                return self.value == other.upper()
            return NotImplemented
        return self.value == other.value

    def __hash__(self) -> int:
        """Hash based on value."""
        return hash(self.value)

    @property
    def is_inverse(self) -> bool:
        """Check if this is an inverse contract (quote is settlement currency)."""
        return self.quote in ["BTC", "ETH", "USD"]

    @property
    def settlement_currency(self) -> str:
        """Get the settlement currency."""
        return self.quote

    @property
    def price_precision(self) -> int:
        """Typical price precision for this symbol."""
        # Most crypto pairs use 2-8 decimal places for price
        return 8

    @property
    def quantity_precision(self) -> int:
        """Typical quantity precision for this symbol."""
        # BTC pairs typically use 6 decimals, others vary
        if self.quote == "BTC":
            return 6
        return 8

    @classmethod
    def from_string(cls, value: str, separator: str = "") -> Symbol:
        """Create from string."""
        return cls(value, "", "", separator)

    @classmethod
    def btcusdt(cls) -> Symbol:
        """Common symbol."""
        return cls("BTCUSDT", "BTC", "USDT")

    @classmethod
    def ethusdt(cls) -> Symbol:
        """Common symbol."""
        return cls("ETHUSDT", "ETH", "USDT")

    def standardize(self, exchange: str | None = None) -> Symbol:
        """Standardize symbol format for an exchange."""
        # Different exchanges use different separators
        formats = {
            "binance": "",
            "okx": "-",
            "bybit": "-",
            "coinbase": "-",
        }
        sep = formats.get(exchange or "", "")
        new_value = f"{self.base}{sep}{self.quote}"
        return Symbol(new_value, self.base, self.quote, sep)
