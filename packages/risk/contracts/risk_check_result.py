"""Risk check result - Outcome of a risk check."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from packages.domain.value_objects import OrderIntent
    from packages.risk.contracts.risk_metrics import RiskMetrics


@dataclass
class RiskCheckResult:
    """Result of a risk check."""

    approved: bool
    order_intent: OrderIntent
    reason: str | None = None
    risk_score: Decimal | None = None
    adjusted_quantity: Decimal | None = None
    adjusted_price: Decimal | None = None
    checks_passed: list[str] = field(default_factory=list)
    checks_failed: list[str] = field(default_factory=list)
    risk_metrics_at_check: RiskMetrics | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def was_modified(self) -> bool:
        """Check if order was modified by risk."""
        return self.adjusted_quantity is not None or self.adjusted_price is not None

    @property
    def rejection_reasons(self) -> list[str]:
        """Get rejection reasons."""
        return self.checks_failed
