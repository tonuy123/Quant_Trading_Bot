"""Risk manager service."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from packages.domain.value_objects import OrderIntent
from packages.risk.contracts import RiskCheckResult, RiskMetrics
from packages.risk.policies import (
    MaxDailyLossPolicy,
    MaxDrawdownPolicy,
    MaxExposurePolicy,
    MaxPositionSizePolicy,
    RiskPolicy,
)

if TYPE_CHECKING:
    pass


class RiskManager:
    """Risk manager orchestrates all risk policies."""

    def __init__(
        self,
        unit_of_work_factory: Any | None = None,
        policies: list[RiskPolicy] | None = None,
    ) -> None:
        """Initialize risk manager."""
        self._uow_factory = unit_of_work_factory
        self._policies = policies or [
            MaxPositionSizePolicy(),
            MaxDrawdownPolicy(),
            MaxDailyLossPolicy(),
            MaxExposurePolicy(),
        ]

    async def check_order(self, intent: OrderIntent) -> RiskCheckResult:
        """Check if an order passes risk controls."""
        metrics = await self._get_metrics()
        checks_passed = []
        checks_failed = []

        for policy in self._policies:
            passed, reason = policy.check(
                intent.quantity.value,
                intent.price.value if intent.price else Decimal("0"),
                metrics,
            )

            if passed:
                checks_passed.append(policy.name)
            else:
                checks_failed.append(f"{policy.name}: {reason}")

        approved = len(checks_failed) == 0

        return RiskCheckResult(
            approved=approved,
            order_intent=intent,
            reason=checks_failed[0] if checks_failed else None,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            risk_metrics_at_check=metrics,
        )

    async def _get_metrics(self) -> RiskMetrics:
        """Get current risk metrics."""
        return RiskMetrics(
            portfolio_id="default",
            total_equity=Decimal("10000"),
            available_balance=Decimal("10000"),
            position_value=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            realized_pnl=Decimal("0"),
            max_drawdown=Decimal("0"),
            daily_pnl=Decimal("0"),
            max_daily_loss=Decimal("500"),
            max_position_size_pct=Decimal("10"),
            current_position_size_pct=Decimal("0"),
            leverage=Decimal("1"),
        )
