"""Portfolio entity - Represents a trading portfolio."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from packages.domain.entities.base import Entity
from packages.domain.value_objects import EntityId, Money

if TYPE_CHECKING:
    from packages.domain.entities.position import Position


@dataclass
class Portfolio(Entity):
    """Represents a trading portfolio holding multiple positions.

    The portfolio tracks:
    - Total equity (cash + positions value)
    - Cash balance
    - Open positions
    - Historical performance
    - Risk metrics
    """

    name: str
    initial_equity: Money
    cash_balance: Money
    id: EntityId | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    current_equity: Money = field(init=False)
    positions: dict[str, Position] = field(default_factory=dict)  # symbol -> Position
    daily_pnl: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    total_pnl: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    total_commission: Money = field(default_factory=lambda: Money(Decimal("0"), "USDT"))
    trading_date: datetime = field(default_factory=datetime.utcnow)
    peak_equity: Money | None = None
    equity_curve: list[tuple[datetime, Money]] = field(default_factory=list)
    max_drawdown: Decimal = field(default_factory=lambda: Decimal("0"))
    strategy_id: str | None = None

    def __post_init__(self) -> None:
        """Initialize computed fields."""
        self.current_equity = self.initial_equity
        self.peak_equity = self.initial_equity

    @property
    def total_unrealized_pnl(self) -> Money:
        """Sum of unrealized PnL across all positions."""
        total = Decimal("0")
        currency = self.cash_balance.currency
        for position in self.positions.values():
            total += position.unrealized_pnl.value
        return Money(total, currency)

    @property
    def total_realized_pnl(self) -> Money:
        """Sum of realized PnL across all positions."""
        total = Decimal("0")
        currency = self.cash_balance.currency
        for position in self.positions.values():
            total += position.realized_pnl.value
        return Money(total, currency)

    @property
    def position_value(self) -> Money:
        """Total value of all positions."""
        total = Decimal("0")
        currency = self.cash_balance.currency
        for position in self.positions.values():
            total += position.notional_value.value
        return Money(total, currency)

    @property
    def leverage(self) -> Decimal:
        """Calculate current leverage (position value / equity)."""
        if self.current_equity.value == 0:
            return Decimal("0")
        return self.position_value.value / self.current_equity.value

    @property
    def open_positions_count(self) -> int:
        """Number of open positions."""
        return len(self.positions)

    def add_position(self, position: Position) -> None:
        """Add a new position to the portfolio."""
        symbol = str(position.symbol)
        if symbol in self.positions:
            raise ValueError(f"Position for {symbol} already exists")
        self.positions[symbol] = position
        self.mark_updated()

    def remove_position(self, symbol: str) -> Position:
        """Remove a position from the portfolio."""
        if symbol not in self.positions:
            raise ValueError(f"Position for {symbol} not found")
        position = self.positions.pop(symbol)
        self.mark_updated()
        return position

    def get_position(self, symbol: str) -> Position | None:
        """Get a position by symbol."""
        return self.positions.get(symbol)

    def update_equity(self) -> None:
        """Recalculate total equity."""
        self.current_equity = Money(
            self.cash_balance.value + self.total_unrealized_pnl.value, self.cash_balance.currency
        )

        # Update peak equity
        if self.peak_equity is None or self.current_equity.value > self.peak_equity.value:
            self.peak_equity = self.current_equity

        # Calculate drawdown
        if self.peak_equity and self.peak_equity.value > 0:
            self.max_drawdown = (
                (self.peak_equity.value - self.current_equity.value) / self.peak_equity.value
            ) * Decimal("100")

        # Record equity curve
        self.equity_curve.append((datetime.utcnow(), self.current_equity))

        self.mark_updated()

    def record_trade(
        self, symbol: str, quantity: Decimal, price: Decimal, side: str, commission: Money
    ) -> None:
        """Record a trade affecting cash balance."""
        notional = quantity * price
        currency = self.cash_balance.currency

        if side == "BUY":
            self.cash_balance = Money(self.cash_balance.value - notional, currency)
        else:
            self.cash_balance = Money(self.cash_balance.value + notional, currency)

        self.total_commission = Money(self.total_commission.value + commission.value, currency)
        self.mark_updated()
