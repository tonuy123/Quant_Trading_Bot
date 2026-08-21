"""Base risk policy."""

from abc import ABC, abstractmethod
from decimal import Decimal

from packages.risk.contracts import RiskMetrics


class RiskPolicy(ABC):
    """Abstract base for risk policies."""

    name: str = "base"

    @abstractmethod
    def check(
        self,
        order_quantity: Decimal,
        order_price: Decimal,
        metrics: RiskMetrics,
    ) -> tuple[bool, str | None]:
        """Check if order passes this policy."""
        ...
