"""Portfolio events."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class PnLUpdated(DomainEvent):
    """Event published when portfolio PnL is updated."""

    event_type: ClassVar[str] = "pnl_updated"

    portfolio_id: str
    current_equity: Decimal
    total_pnl: Decimal
    daily_pnl: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    max_drawdown: Decimal | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "portfolio_id": self.portfolio_id,
            "current_equity": str(self.current_equity),
            "total_pnl": str(self.total_pnl),
            "daily_pnl": str(self.daily_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "realized_pnl": str(self.realized_pnl),
            "max_drawdown": str(self.max_drawdown) if self.max_drawdown else None,
        }


@dataclass(frozen=True)
class PortfolioCreated(DomainEvent):
    """Event published when a new portfolio is created."""

    event_type: ClassVar[str] = "portfolio_created"

    portfolio_id: str
    portfolio_name: str
    initial_equity: Decimal
    currency: str

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "portfolio_id": self.portfolio_id,
            "portfolio_name": self.portfolio_name,
            "initial_equity": str(self.initial_equity),
            "currency": self.currency,
        }


@dataclass(frozen=True)
class DrawdownAlert(DomainEvent):
    """Event published when drawdown exceeds threshold."""

    event_type: ClassVar[str] = "drawdown_alert"

    portfolio_id: str
    current_drawdown: Decimal
    max_allowed_drawdown: Decimal
    current_equity: Decimal

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "portfolio_id": self.portfolio_id,
            "current_drawdown": str(self.current_drawdown),
            "max_allowed_drawdown": str(self.max_allowed_drawdown),
            "current_equity": str(self.current_equity),
        }
