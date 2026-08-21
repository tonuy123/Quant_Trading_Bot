"""Risk management events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent
from packages.domain.value_objects import OrderIntent

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class RiskApproved(DomainEvent):
    """Event published when an order intent passes risk checks."""

    event_type: ClassVar[str] = "risk_approved"

    order_intent: OrderIntent
    approved_quantity: Decimal
    approved_price: Decimal | None = None
    risk_score: Decimal | None = None
    checks_passed: list[str] | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "symbol": str(self.order_intent.symbol),
            "side": str(self.order_intent.side),
            "order_type": str(self.order_intent.order_type),
            "requested_quantity": str(self.order_intent.quantity.value),
            "approved_quantity": str(self.approved_quantity),
            "approved_price": str(self.approved_price) if self.approved_price else None,
            "strategy_id": self.order_intent.strategy_id,
        }


@dataclass(frozen=True)
class RiskRejected(DomainEvent):
    """Event published when an order intent fails risk checks."""

    event_type: ClassVar[str] = "risk_rejected"

    order_intent: OrderIntent
    reason: str
    rejected_at: datetime
    risk_metrics: dict[str, Any] | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "symbol": str(self.order_intent.symbol),
            "side": str(self.order_intent.side),
            "order_type": str(self.order_intent.order_type),
            "quantity": str(self.order_intent.quantity.value),
            "reason": self.reason,
            "rejected_at": self.rejected_at.isoformat(),
            "strategy_id": self.order_intent.strategy_id,
        }


@dataclass(frozen=True)
class RiskLimitExceeded(DomainEvent):
    """Event published when a risk limit is exceeded."""

    event_type: ClassVar[str] = "risk_limit_exceeded"

    limit_type: str  # e.g., "max_position_size", "max_drawdown"
    current_value: Decimal
    limit_value: Decimal
    portfolio_id: str | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "limit_type": self.limit_type,
            "current_value": str(self.current_value),
            "limit_value": str(self.limit_value),
            "portfolio_id": self.portfolio_id,
        }
