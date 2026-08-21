"""Unit of Work interface - Contract for transactional operations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import TracebackType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from packages.domain.interfaces.repositories import (
        CandleRepository,
        OrderRepository,
        PortfolioRepository,
        PositionRepository,
        SignalRepository,
    )


class UnitOfWork(ABC):
    """Abstract Unit of Work interface.

    Unit of Work manages a transactional scope, ensuring:
    - Changes are atomically committed or rolled back
    - Repositories share the same database session
    - Resources are properly cleaned up

    Usage:
        async with uow:
            order = await uow.orders.get_by_id(order_id)
            order.status = OrderStatus.FILLED
            await uow.orders.update(order)
            await uow.commit()
    """

    @abstractmethod
    async def __aenter__(self) -> UnitOfWork:
        """Enter async context."""
        ...

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context."""
        ...

    @property
    @abstractmethod
    def orders(self) -> OrderRepository:
        """Get order repository."""
        ...

    @property
    @abstractmethod
    def positions(self) -> PositionRepository:
        """Get position repository."""
        ...

    @property
    @abstractmethod
    def portfolios(self) -> PortfolioRepository:
        """Get portfolio repository."""
        ...

    @property
    @abstractmethod
    def candles(self) -> CandleRepository:
        """Get candle repository."""
        ...

    @property
    @abstractmethod
    def signals(self) -> SignalRepository:
        """Get signal repository."""
        ...

    @abstractmethod
    async def commit(self) -> None:
        """Commit all changes in this unit of work."""
        ...

    @abstractmethod
    async def rollback(self) -> None:
        """Rollback all changes in this unit of work."""
        ...

    @abstractmethod
    async def flush(self) -> None:
        """Flush changes to database without committing."""
        ...


class UnitOfWorkFactory(ABC):
    """Factory for creating Unit of Work instances."""

    @abstractmethod
    def create(self) -> UnitOfWork:
        """Create a new Unit of Work.

        Returns:
            New Unit of Work instance.
        """
        ...

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[UnitOfWork]:
        """Context manager for a complete transaction.

        Usage:
            async with uow_factory.transaction() as uow:
                # operations

        Yields:
            A UnitOfWork bound to a new transaction.
        """
        uow = self.create()
        try:
            yield uow
        except BaseException:
            await uow.rollback()
            raise
        await uow.commit()
