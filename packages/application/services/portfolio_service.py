"""Portfolio service - Orchestrates portfolio operations."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from packages.domain.entities.portfolio import Portfolio
from packages.domain.entities.position import Position
from packages.domain.enums import PositionSide
from packages.domain.errors import PositionAlreadyExistsError, PositionNotFoundError
from packages.domain.interfaces import EventPublisher, UnitOfWorkFactory
from packages.domain.value_objects import EntityId, Money, Price, Quantity, Symbol

if TYPE_CHECKING:
    pass


@dataclass
class PortfolioSnapshot:
    """Snapshot of portfolio state."""

    portfolio_id: str
    name: str
    equity: Decimal
    cash: Decimal
    position_value: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    max_drawdown: Decimal
    open_positions: int


class PortfolioService:
    """Service for managing portfolios.

    Handles portfolio lifecycle and position management.
    """

    def __init__(
        self,
        unit_of_work_factory: UnitOfWorkFactory,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize portfolio service.

        Args:
            unit_of_work_factory: Factory for database transactions
            event_publisher: Event distribution
        """
        self._uow_factory = unit_of_work_factory
        self._events = event_publisher

    async def create_portfolio(
        self,
        name: str,
        initial_equity: Decimal,
        currency: str = "USDT",
        strategy_id: str | None = None,
    ) -> Portfolio:
        """Create a new portfolio.

        Args:
            name: Portfolio name
            initial_equity: Starting equity
            currency: Settlement currency
            strategy_id: Associated strategy

        Returns:
            Created portfolio.
        """
        portfolio = Portfolio(
            id=None,  # Will be assigned
            name=name,
            initial_equity=Money(initial_equity, currency),
            cash_balance=Money(initial_equity, currency),
            strategy_id=strategy_id,
        )

        async with self._uow_factory.transaction() as uow:
            await uow.portfolios.create(portfolio)

        return portfolio

    async def get_portfolio(self, portfolio_id: str) -> Portfolio | None:
        """Get portfolio by ID.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Portfolio if found.
        """
        async with self._uow_factory.transaction() as uow:
            return await uow.portfolios.get_by_id(EntityId(portfolio_id))

    async def get_snapshot(self, portfolio_id: str) -> PortfolioSnapshot | None:
        """Get portfolio snapshot.

        Args:
            portfolio_id: Portfolio ID

        Returns:
            Portfolio snapshot.
        """
        portfolio = await self.get_portfolio(portfolio_id)
        if not portfolio:
            return None

        return PortfolioSnapshot(
            portfolio_id=str(portfolio.id),
            name=portfolio.name,
            equity=portfolio.current_equity.value,
            cash=portfolio.cash_balance.value,
            position_value=portfolio.position_value.value,
            unrealized_pnl=portfolio.total_unrealized_pnl.value,
            realized_pnl=portfolio.total_realized_pnl.value,
            max_drawdown=portfolio.max_drawdown,
            open_positions=portfolio.open_positions_count,
        )

    async def open_position(
        self,
        portfolio_id: str,
        symbol: str,
        side: PositionSide,
        quantity: Decimal,
        entry_price: Decimal,
        strategy_id: str | None = None,
        order_id: str | None = None,
    ) -> Position:
        """Open a new position.

        Args:
            portfolio_id: Portfolio ID
            symbol: Trading symbol
            side: Position side
            quantity: Position quantity
            entry_price: Entry price
            strategy_id: Strategy ID
            order_id: Opening order ID

        Returns:
            Created position.
        """
        async with self._uow_factory.transaction() as uow:
            # Check for existing position
            existing = await uow.positions.get_by_symbol(Symbol(symbol))
            if existing:
                raise PositionAlreadyExistsError(symbol, str(side))

            # Create position
            sym = Symbol(symbol)
            position = Position(
                id=None,
                symbol=sym,
                side=side,
                quantity=Quantity(quantity),
                entry_price=Price(entry_price, sym.base, sym.quote),
                current_price=Price(entry_price, sym.base, sym.quote),
                average_fill_price=Price(entry_price, sym.base, sym.quote),
                strategy_id=strategy_id,
            )
            if order_id:
                position.add_order(order_id)

            await uow.positions.create(position)

            # Update portfolio
            portfolio = await uow.portfolios.get_by_id(EntityId(portfolio_id))
            if portfolio:
                portfolio.add_position(position)
                await uow.portfolios.update(portfolio)

        from packages.domain.events import PositionOpened

        await self._events.publish(
            PositionOpened(
                position_id=str(position.id),
                symbol=position.symbol,
                side=position.side,
                quantity=position.quantity.value,
                entry_price=position.entry_price.value,
                strategy_id=strategy_id,
                order_id=order_id,
            )
        )

        return position

    async def close_position(
        self,
        portfolio_id: str,
        symbol: str,
        exit_price: Decimal,
        quantity: Decimal | None = None,
    ) -> Position:
        """Close a position.

        Args:
            portfolio_id: Portfolio ID
            symbol: Trading symbol
            exit_price: Exit price
            quantity: Quantity to close (None = all)

        Returns:
            Closed position.
        """
        async with self._uow_factory.transaction() as uow:
            position = await uow.positions.get_by_symbol(Symbol(symbol))
            if not position:
                raise PositionNotFoundError(symbol=symbol)

            close_qty = quantity or position.quantity.value
            position.close(Price(exit_price, position.symbol.base, position.symbol.quote))

            await uow.positions.update(position)

        from packages.domain.events import PositionClosed

        holding_seconds = None
        if position.exit_time and position.entry_time:
            holding_seconds = (position.exit_time - position.entry_time).total_seconds()

        await self._events.publish(
            PositionClosed(
                position_id=str(position.id),
                symbol=position.symbol,
                side=position.side,
                exit_price=exit_price,
                exit_quantity=close_qty,
                realized_pnl=position.realized_pnl.value,
                commission_paid=position.commission_paid.value,
                holding_period_seconds=holding_seconds,
                strategy_id=position.strategy_id,
            )
        )

        return position

    async def update_position_price(
        self,
        symbol: str,
        current_price: Decimal,
    ) -> Position | None:
        """Update position with current market price.

        Args:
            symbol: Trading symbol
            current_price: Current market price

        Returns:
            Updated position.
        """
        async with self._uow_factory.transaction() as uow:
            position = await uow.positions.get_by_symbol(Symbol(symbol))
            if not position:
                return None

            position.update_price(Price(current_price, position.symbol.base, position.symbol.quote))
            await uow.positions.update(position)

        from packages.domain.events import PositionUpdated

        await self._events.publish(
            PositionUpdated(
                position_id=str(position.id),
                symbol=position.symbol,
                unrealized_pnl=position.unrealized_pnl.value,
                current_price=current_price,
            )
        )

        return position
