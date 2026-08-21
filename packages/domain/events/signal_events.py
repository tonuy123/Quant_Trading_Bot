"""Signal generation events."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar

from packages.domain.events.base import DomainEvent
from packages.domain.value_objects import Symbol

if TYPE_CHECKING:
    from packages.domain.enums import SignalType


@dataclass(frozen=True)
class SignalGenerated(DomainEvent):
    """Event published when a trading signal is generated.

    A signal indicates a trading opportunity identified by a strategy.
    The signal must pass through risk management before execution.
    """

    event_type: ClassVar[str] = "signal_generated"

    signal_id: str
    strategy_id: str
    strategy_name: str
    symbol: Symbol
    signal_type: SignalType
    strength: Decimal = Decimal("1.0")  # Signal strength 0-1
    confidence: Decimal = Decimal("1.0")  # Confidence level 0-1
    current_price: Decimal | None = None
    target_price: Decimal | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    metadata: dict[str, Any] | None = None

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "signal_id": self.signal_id,
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "symbol": str(self.symbol),
            "signal_type": str(self.signal_type),
            "strength": str(self.strength),
            "confidence": str(self.confidence),
            "current_price": str(self.current_price) if self.current_price else None,
            "target_price": str(self.target_price) if self.target_price else None,
            "stop_loss": str(self.stop_loss) if self.stop_loss else None,
            "take_profit": str(self.take_profit) if self.take_profit else None,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class SignalExpired(DomainEvent):
    """Event published when a signal expires without execution."""

    event_type: ClassVar[str] = "signal_expired"

    signal_id: str
    strategy_id: str
    symbol: Symbol
    reason: str

    def _to_data(self) -> dict[str, Any]:
        """Get event data."""
        return {
            "signal_id": self.signal_id,
            "strategy_id": self.strategy_id,
            "symbol": str(self.symbol),
            "reason": self.reason,
        }
