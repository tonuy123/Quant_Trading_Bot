"""Risk-decision enumeration."""

from __future__ import annotations

from enum import StrEnum


class RiskDecision(StrEnum):
    """Decision from risk management."""

    APPROVED = "APPROVED"  # Order can proceed
    REJECTED = "REJECTED"  # Order cannot proceed
    REDUCED = "REDUCED"  # Order size reduced
    MODIFIED = "MODIFIED"  # Order parameters modified

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_approved(self) -> bool:
        """Check if order is approved."""
        return self == RiskDecision.APPROVED

    @property
    def is_rejected(self) -> bool:
        """Check if order is rejected."""
        return self == RiskDecision.REJECTED

    @property
    def allows_execution(self) -> bool:
        """Check if order can be executed."""
        return self in {RiskDecision.APPROVED, RiskDecision.REDUCED, RiskDecision.MODIFIED}

    @property
    def reason(self) -> str:
        """Get default reason for this decision."""
        reasons = {
            RiskDecision.APPROVED: "All risk checks passed",
            RiskDecision.REJECTED: "Risk check failed",
            RiskDecision.REDUCED: "Position size reduced to comply with risk limits",
            RiskDecision.MODIFIED: "Order parameters modified by risk management",
        }
        return reasons[self]
