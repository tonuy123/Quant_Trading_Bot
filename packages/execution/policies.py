"""Order execution policies."""

from abc import ABC, abstractmethod
from decimal import Decimal


class ExecutionPolicy(ABC):
    """Base class for execution policies."""

    @abstractmethod
    def should_fill(
        self,
        order_price: Decimal,
        market_price: Decimal,
        side: str,
    ) -> bool:
        """Check if market price should fill order."""
        ...


class MarketPolicy(ExecutionPolicy):
    """Market orders fill at current price."""

    def should_fill(
        self,
        order_price: Decimal,
        market_price: Decimal,
        side: str,
    ) -> bool:
        return True


class LimitPolicy(ExecutionPolicy):
    """Limit orders fill at limit price or better."""

    def should_fill(
        self,
        order_price: Decimal,
        market_price: Decimal,
        side: str,
    ) -> bool:
        if side == "BUY":
            return market_price <= order_price
        return market_price >= order_price


class StopPolicy(ExecutionPolicy):
    """Stop orders trigger when price crosses."""

    def should_fill(
        self,
        order_price: Decimal,
        market_price: Decimal,
        side: str,
    ) -> bool:
        if side == "BUY":
            return market_price >= order_price
        return market_price <= order_price
