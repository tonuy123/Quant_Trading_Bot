"""Risk limit - Configuration for a risk limit."""

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class RiskLimit:
    """Configuration for a risk limit."""

    name: str
    max_value: Decimal
    min_value: Decimal | None = None
    enabled: bool = True
    action_on_breach: str = "reject"  # reject, reduce, warn

    def is_within_limits(self, value: Decimal) -> bool:
        """Check if value is within limits."""
        if not self.enabled:
            return True
        if value > self.max_value:
            return False
        if self.min_value is not None and value < self.min_value:
            return False
        return True
