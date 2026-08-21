"""Risk metrics - Portfolio risk tracking."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass
class RiskMetrics:
    """Current risk metrics for a portfolio."""

    portfolio_id: str
    total_equity: Decimal
    available_balance: Decimal
    position_value: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    max_drawdown: Decimal
    daily_pnl: Decimal
    max_daily_loss: Decimal
    max_position_size_pct: Decimal
    current_position_size_pct: Decimal
    leverage: Decimal
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def position_utilization(self) -> Decimal:
        """Calculate position utilization percentage."""
        if self.max_position_size_pct == 0:
            return Decimal("0")
        return (self.current_position_size_pct / self.max_position_size_pct) * 100

    @property
    def drawdown_utilization(self) -> Decimal:
        """Calculate drawdown utilization percentage."""
        if self.max_drawdown == 0:
            return Decimal("0")
        return (
            (abs(self.daily_pnl) / self.max_daily_loss) * 100
            if self.max_daily_loss
            else Decimal("0")
        )
