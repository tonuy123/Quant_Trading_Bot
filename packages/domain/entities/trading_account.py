"""Trading account entity - Represents an exchange trading account."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from packages.domain.entities.base import Entity
from packages.domain.value_objects import EntityId, Money

if TYPE_CHECKING:
    from packages.domain.enums import ExchangeMode


@dataclass
class TradingAccount(Entity):
    """Represents a trading account on an exchange.

    This is the root aggregate for account-level operations:
    - Balance management
    - Order management
    - Position tracking
    - Mode switching (paper/live)
    """

    exchange_name: str
    account_name: str
    balance: Money
    available_balance: Money
    exchange_mode: ExchangeMode
    id: EntityId | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    locked_balance: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    api_key_hash: str | None = None
    permissions: list[str] = field(default_factory=list)
    rate_limit_remaining: int = 100
    rate_limit_reset_at: datetime | None = None
    is_active: bool = True
    last_sync_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def _validate(self) -> None:
        """Validate account state."""
        if self.balance.value < 0:
            raise ValueError("Balance cannot be negative")
        if self.available_balance.value < 0:
            raise ValueError("Available balance cannot be negative")
        if self.locked_balance.value < 0:
            raise ValueError("Locked balance cannot be negative")

    @property
    def total_balance(self) -> Money:
        """Total balance including locked."""
        return Money(self.balance.value + self.locked_balance.value, self.balance.currency)

    def lock_balance(self, amount: Money) -> None:
        """Lock balance for pending orders."""
        if amount.value > self.available_balance.value:
            raise ValueError("Insufficient available balance")
        self.balance = Money(self.balance.value - amount.value, self.balance.currency)
        self.locked_balance = Money(
            self.locked_balance.value + amount.value, self.locked_balance.currency
        )
        self.mark_updated()

    def unlock_balance(self, amount: Money) -> None:
        """Unlock balance from cancelled/filled orders."""
        if amount.value > self.locked_balance.value:
            raise ValueError("Insufficient locked balance")
        self.locked_balance = Money(
            self.locked_balance.value - amount.value, self.locked_balance.currency
        )
        self.balance = Money(self.balance.value + amount.value, self.balance.currency)
        self.mark_updated()

    def deduct_balance(self, amount: Money) -> None:
        """Deduct from locked balance (order filled)."""
        if amount.value > self.locked_balance.value:
            raise ValueError("Insufficient locked balance")
        self.locked_balance = Money(
            self.locked_balance.value - amount.value, self.locked_balance.currency
        )
        self.mark_updated()

    def add_balance(self, amount: Money) -> None:
        """Add to available balance (deposit or withdrawal release)."""
        self.balance = Money(self.balance.value + amount.value, self.balance.currency)
        self.mark_updated()

    def update_rate_limit(self, remaining: int, reset_at: datetime) -> None:
        """Update rate limit status."""
        self.rate_limit_remaining = remaining
        self.rate_limit_reset_at = reset_at
        self.mark_updated()

    def sync(self, balance: Money, available: Money) -> None:
        """Sync account state from exchange."""
        self.balance = balance
        self.available_balance = available
        self.locked_balance = Money(
            self.balance.value - self.available_balance.value, self.balance.currency
        )
        self.last_sync_at = datetime.utcnow()
        self.mark_updated()
