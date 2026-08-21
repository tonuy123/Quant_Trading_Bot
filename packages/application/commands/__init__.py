"""Command definitions - Write operations that change state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from packages.domain.enums import OrderSide, OrderType, TimeInForce
from packages.domain.value_objects import Price, Quantity, Symbol


class Command:
    """Base class for all commands.

    Commands represent intent to change state.
    They are immutable and contain all data needed for execution.

    Subclasses are dataclasses. They must declare ``command_id`` after their
    required fields and ``timestamp`` at the end:

        @dataclass
        class PlaceOrderCommand(Command):
            symbol: Symbol
            ...
            command_id: str
            timestamp: datetime | None = None
    """

    command_id: str
    timestamp: datetime | None

    def __init__(self, command_id: str, timestamp: datetime | None = None) -> None:
        self.command_id = command_id
        self.timestamp = timestamp if timestamp is not None else datetime.utcnow()


@dataclass
class PlaceOrderCommand(Command):
    """Command to place a new order."""

    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Quantity
    command_id: str
    timestamp: datetime | None = None
    price: Price | None = None
    stop_price: Price | None = None
    time_in_force: TimeInForce | None = None
    strategy_id: str | None = None
    client_order_id: str | None = None


@dataclass
class CancelOrderCommand(Command):
    """Command to cancel an existing order."""

    order_id: str
    symbol: Symbol
    command_id: str
    timestamp: datetime | None = None
    reason: str | None = None


@dataclass
class UpdateOrderCommand(Command):
    """Command to update an existing order."""

    order_id: str
    symbol: Symbol
    command_id: str
    timestamp: datetime | None = None
    new_quantity: Quantity | None = None
    new_price: Price | None = None


@dataclass
class ClosePositionCommand(Command):
    """Command to close a position."""

    symbol: Symbol
    position_id: str
    command_id: str
    timestamp: datetime | None = None
    quantity: Quantity | None = None  # None = close all
    reason: str | None = None


@dataclass
class CreatePortfolioCommand(Command):
    """Command to create a new portfolio."""

    name: str
    initial_equity: Decimal
    command_id: str
    timestamp: datetime | None = None
    currency: str = "USDT"
    strategy_id: str | None = None


@dataclass
class UpdateRiskLimitsCommand(Command):
    """Command to update risk limits."""

    portfolio_id: str
    command_id: str
    timestamp: datetime | None = None
    max_position_size: Decimal | None = None
    max_drawdown: Decimal | None = None
    max_daily_loss: Decimal | None = None
    max_leverage: Decimal | None = None


@dataclass
class EnableStrategyCommand(Command):
    """Command to enable a trading strategy."""

    strategy_id: str
    command_id: str
    timestamp: datetime | None = None
    symbols: list[Symbol] | None = None


@dataclass
class DisableStrategyCommand(Command):
    """Command to disable a trading strategy."""

    strategy_id: str
    command_id: str
    timestamp: datetime | None = None
    reason: str | None = None


@dataclass
class SwitchExchangeModeCommand(Command):
    """Command to switch exchange operating mode."""

    exchange_name: str
    mode: str  # fake, paper, live
    command_id: str
    timestamp: datetime | None = None
