"""Max drawdown policy."""

from __future__ import annotations

from decimal import Decimal

from packages.risk.contracts import RiskMetrics
from packages.risk.policies.base import RiskPolicy


class MaxDrawdownPolicy(RiskPolicy):
    """Policy to limit maximum drawdown."""

    name: str = "max_drawdown"

    def __init__(self, max_drawdown_pct: Decimal = Decimal("20")) -> None:
        """Initialize policy."""
        self.max_drawdown_pct = max_drawdown_pct

    def check(
        self,
        order_quantity: Decimal,
        order_price: Decimal,
        metrics: RiskMetrics,
    ) -> tuple[bool, str | None]:
        """Check if drawdown is within limits."""
        if metrics.max_drawdown > self.max_drawdown_pct:
            return (
                False,
                f"Max drawdown {metrics.max_drawdown:.2f}% exceeds limit {self.max_drawdown_pct}%",
            )

        return True, None
