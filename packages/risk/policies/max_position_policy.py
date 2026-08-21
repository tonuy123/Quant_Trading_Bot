"""Max position size policy."""

from __future__ import annotations

from decimal import Decimal

from packages.risk.contracts import RiskMetrics
from packages.risk.policies.base import RiskPolicy


class MaxPositionSizePolicy(RiskPolicy):
    """Policy to limit position size."""

    name: str = "max_position_size"

    def __init__(self, max_position_size_pct: Decimal = Decimal("10")) -> None:
        """Initialize policy."""
        self.max_position_size_pct = max_position_size_pct

    def check(
        self,
        order_quantity: Decimal,
        order_price: Decimal,
        metrics: RiskMetrics,
    ) -> tuple[bool, str | None]:
        """Check if position size is within limits."""
        notional = order_quantity * order_price
        position_pct = (
            (notional / metrics.total_equity) * 100 if metrics.total_equity > 0 else Decimal("0")
        )

        if position_pct > self.max_position_size_pct:
            return (
                False,
                f"Position size {position_pct:.2f}% exceeds max {self.max_position_size_pct}%",
            )

        return True, None
