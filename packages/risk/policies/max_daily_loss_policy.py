"""Max daily loss policy."""

from __future__ import annotations

from decimal import Decimal

from packages.risk.contracts import RiskMetrics
from packages.risk.policies.base import RiskPolicy


class MaxDailyLossPolicy(RiskPolicy):
    """Policy to limit daily loss."""

    name: str = "max_daily_loss"

    def __init__(self, max_daily_loss_pct: Decimal = Decimal("5")) -> None:
        """Initialize policy."""
        self.max_daily_loss_pct = max_daily_loss_pct

    def check(
        self,
        order_quantity: Decimal,
        order_price: Decimal,
        metrics: RiskMetrics,
    ) -> tuple[bool, str | None]:
        """Check if daily loss is within limits."""
        if metrics.daily_pnl < 0 and abs(metrics.daily_pnl) > metrics.max_daily_loss:
            loss_pct = (abs(metrics.daily_pnl) / metrics.total_equity) * 100
            if loss_pct > self.max_daily_loss_pct:
                return False, "Daily loss exceeds limit"

        return True, None
