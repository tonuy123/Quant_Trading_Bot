"""Execution contracts."""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass
class ExecutionResult:
    """Result of an order execution attempt."""

    success: bool
    order_id: str | None = None
    exchange_order_id: str | None = None
    status: str | None = None
    filled_quantity: Decimal | None = None
    average_fill_price: Decimal | None = None
    commission: Decimal | None = None
    commission_currency: str | None = None
    message: str | None = None
    error_code: str | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_filled(self) -> bool:
        """Check if fully filled."""
        return self.success and self.filled_quantity is not None

    @property
    def is_partial_fill(self) -> bool:
        """Check if partially filled."""
        return False  # Implement if needed


@dataclass
class FillData:
    """Data for an individual fill."""

    order_id: str
    exchange_order_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    quote_quantity: Decimal
    commission: Decimal
    commission_currency: str
    trade_id: str
    timestamp: datetime
