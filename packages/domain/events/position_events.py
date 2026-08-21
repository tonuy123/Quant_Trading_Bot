"""Position events."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent
from packages.domain.value_objects import Symbol

if TYPE_CHECKING:
    from packages.domain.enums import PositionSide


@dataclass(frozen=True)
class PositionOpened(DomainEvent):
    """Event published when a new position is opened."""

    event_type: ClassVar[str] = "position_opened"

    position_id: str
    symbol: Symbol
    side: PositionSide
    quantity: Decimal
    entry_price: Decimal
    strategy_id: str | None = None
    order_id: str | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "position_id": self.position_id,
            "symbol": str(self.symbol),
            "side": str(self.side),
            "quantity": str(self.quantity),
            "entry_price": str(self.entry_price),
            "strategy_id": self.strategy_id,
            "order_id": self.order_id,
        }


@dataclass(frozen=True)
class PositionUpdated(DomainEvent):
    """Event published when a position is updated."""

    event_type: ClassVar[str] = "position_updated"

    position_id: str
    symbol: Symbol
    unrealized_pnl: Decimal
    current_price: Decimal
    quantity: Decimal | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "position_id": self.position_id,
            "symbol": str(self.symbol),
            "unrealized_pnl": str(self.unrealized_pnl),
            "current_price": str(self.current_price),
            "quantity": str(self.quantity) if self.quantity else None,
        }


@dataclass(frozen=True)
class PositionClosed(DomainEvent):
    """Event published when a position is closed."""

    event_type: ClassVar[str] = "position_closed"

    position_id: str
    symbol: Symbol
    side: PositionSide
    exit_price: Decimal
    exit_quantity: Decimal
    realized_pnl: Decimal
    commission_paid: Decimal
    holding_period_seconds: float | None = None
    strategy_id: str | None = None
    order_id: str | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "position_id": self.position_id,
            "symbol": str(self.symbol),
            "side": str(self.side),
            "exit_price": str(self.exit_price),
            "exit_quantity": str(self.exit_quantity),
            "realized_pnl": str(self.realized_pnl),
            "commission_paid": str(self.commission_paid),
            "holding_period_seconds": self.holding_period_seconds,
            "strategy_id": self.strategy_id,
            "order_id": self.order_id,
        }
