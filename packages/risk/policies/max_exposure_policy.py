"""Max exposure policy."""

from __future__ import annotations

from decimal import Decimal

from packages.risk.contracts import RiskMetrics
from packages.risk.policies.base import RiskPolicy


class MaxExposurePolicy(RiskPolicy):
    """Policy to limit total exposure."""

    name: str = "max_exposure"

    def __init__(self, max_exposure_pct: Decimal = Decimal("100")) -> None:
        """Initialize policy."""
        self.max_exposure_pct = max_exposure_pct

    def check(
        self,
        order_quantity: Decimal,
        order_price: Decimal,
        metrics: RiskMetrics,
    ) -> tuple[bool, str | None]:
        """Check if total exposure is within limits."""
        new_notional = order_quantity * order_price
        total_exposure = metrics.position_value + new_notional
        exposure_pct = (
            (total_exposure / metrics.total_equity) * 100
            if metrics.total_equity > 0
            else Decimal("0")
        )

        if exposure_pct > self.max_exposure_pct:
            return False, f"Total exposure {exposure_pct:.2f}% exceeds max {self.max_exposure_pct}%"

        return True, None
