"""Trading signal from a strategy."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass
class Signal:
    """Trading signal from a strategy."""

    signal_id: str
    strategy_id: str
    strategy_name: str
    symbol: str
    signal_type: str  # BUY, SELL, NEUTRAL
    strength: Decimal = Decimal("1.0")
    confidence: Decimal = Decimal("1.0")
    current_price: Decimal | None = None
    target_price: Decimal | None = None
    stop_loss: Decimal | None = None
    take_profit: Decimal | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: datetime | None = None

    @property
    def is_buy(self) -> bool:
        """Check if buy signal."""
        return self.signal_type == "BUY"

    @property
    def is_sell(self) -> bool:
        """Check if sell signal."""
        return self.signal_type == "SELL"

    @property
    def is_expired(self) -> bool:
        """Check if signal has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at
