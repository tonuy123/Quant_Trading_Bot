"""Portfolio manager service."""

from __future__ import annotations

from decimal import Decimal

from packages.domain.entities.portfolio import Portfolio
from packages.domain.entities.position import Position
from packages.domain.interfaces import UnitOfWorkFactory
from packages.domain.value_objects import EntityId, Price, Symbol


class PortfolioManager:
    """Manages portfolios and positions."""

    def __init__(self, unit_of_work_factory: UnitOfWorkFactory) -> None:
        """Initialize manager."""
        self._uow_factory = unit_of_work_factory

    async def get_portfolio(self, portfolio_id: str) -> Portfolio | None:
        """Get portfolio by ID."""
        async with self._uow_factory.transaction() as uow:
            return await uow.portfolios.get_by_id(EntityId(portfolio_id))

    async def get_position(self, symbol: str) -> Position | None:
        """Get open position for symbol."""
        async with self._uow_factory.transaction() as uow:
            return await uow.positions.get_by_symbol(Symbol(symbol))

    async def get_all_positions(self) -> list[Position]:
        """Get all open positions."""
        async with self._uow_factory.transaction() as uow:
            return await uow.positions.get_all_open()

    async def update_position_price(self, symbol: str, price: Decimal) -> None:
        """Update position with current price."""
        async with self._uow_factory.transaction() as uow:
            position = await uow.positions.get_by_symbol(Symbol(symbol))
            if position:
                position.update_price(Price(price, position.symbol.base, position.symbol.quote))
                await uow.positions.update(position)
