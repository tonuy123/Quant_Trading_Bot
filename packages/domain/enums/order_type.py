"""Order-type enumeration."""

from __future__ import annotations

from enum import StrEnum


class OrderType(StrEnum):
    """Type of order."""

    # Basic types
    MARKET = "MARKET"  # Execute immediately at market price
    LIMIT = "LIMIT"  # Execute at specified price or better

    # Stop orders
    STOP = "STOP"  # Trigger when price crosses stop
    STOP_LIMIT = "STOP_LIMIT"  # Stop that becomes limit order

    # Special types (for future use)
    TAKE_PROFIT = "TAKE_PROFIT"  # Same as stop but for profit
    TAKE_PROFIT_LIMIT = "TAKE_PROFIT_LIMIT"  # TP with limit

    # Advanced
    TRAILING_STOP = "TRAILING_STOP"  # Dynamic stop based on price movement

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_market(self) -> bool:
        """Check if market order."""
        return self == OrderType.MARKET

    @property
    def is_limit(self) -> bool:
        """Check if limit order."""
        return self == OrderType.LIMIT

    @property
    def is_stop(self) -> bool:
        """Check if stop order."""
        return self in {OrderType.STOP, OrderType.STOP_LIMIT}

    @property
    def requires_price(self) -> bool:
        """Check if order requires a limit price."""
        return self in {OrderType.LIMIT, OrderType.STOP_LIMIT, OrderType.TAKE_PROFIT_LIMIT}

    @property
    def requires_stop_price(self) -> bool:
        """Check if order requires a stop price."""
        return self in {
            OrderType.STOP,
            OrderType.STOP_LIMIT,
            OrderType.TAKE_PROFIT,
            OrderType.TAKE_PROFIT_LIMIT,
        }
