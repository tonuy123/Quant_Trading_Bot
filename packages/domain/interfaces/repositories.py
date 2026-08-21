"""Repository interfaces - Contracts for data persistence."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from packages.domain.entities.candle import Candle
    from packages.domain.entities.order import Order
    from packages.domain.entities.portfolio import Portfolio
    from packages.domain.entities.position import Position
    from packages.domain.enums import OrderStatus, Timeframe
    from packages.domain.value_objects import EntityId, Symbol


class OrderRepository(ABC):
    """Abstract order repository interface.

    Handles persistence of Order entities.
    """

    @abstractmethod
    async def create(self, order: Order) -> Order:
        """Create a new order.

        Args:
            order: Order to create

        Returns:
            Created order.
        """
        ...

    @abstractmethod
    async def update(self, order: Order) -> Order:
        """Update an existing order.

        Args:
            order: Order to update

        Returns:
            Updated order.
        """
        ...

    @abstractmethod
    async def get_by_id(self, order_id: EntityId) -> Order | None:
        """Get order by ID.

        Args:
            order_id: Order ID

        Returns:
            Order if found.
        """
        ...

    @abstractmethod
    async def get_by_exchange_id(self, exchange_order_id: str) -> Order | None:
        """Get order by exchange order ID.

        Args:
            exchange_order_id: Exchange order ID

        Returns:
            Order if found.
        """
        ...

    @abstractmethod
    async def get_by_client_id(self, client_order_id: str) -> Order | None:
        """Get order by client order ID.

        Args:
            client_order_id: Client order ID

        Returns:
            Order if found.
        """
        ...

    @abstractmethod
    async def get_by_symbol(
        self,
        symbol: Symbol,
        status: OrderStatus | None = None,
        limit: int = 100,
    ) -> list[Order]:
        """Get orders by symbol.

        Args:
            symbol: Trading symbol
            status: Filter by status
            limit: Maximum results

        Returns:
            List of orders.
        """
        ...

    @abstractmethod
    async def get_active_orders(self, symbol: Symbol | None = None) -> list[Order]:
        """Get all active (non-terminal) orders.

        Args:
            symbol: Optional symbol filter

        Returns:
            List of active orders.
        """
        ...

    @abstractmethod
    async def get_by_strategy(self, strategy_id: str, limit: int = 100) -> list[Order]:
        """Get orders by strategy ID.

        Args:
            strategy_id: Strategy ID
            limit: Maximum results

        Returns:
            List of orders.
        """
        ...

    @abstractmethod
    async def get_by_date_range(
        self,
        start_time: datetime,
        end_time: datetime,
        symbol: Symbol | None = None,
        status: OrderStatus | None = None,
    ) -> list[Order]:
        """Get orders by date range.

        Args:
            start_time: Start of range
            end_time: End of range
            symbol: Optional symbol filter
            status: Optional status filter

        Returns:
            List of orders.
        """
        ...

    @abstractmethod
    async def count(self, status: OrderStatus | None = None) -> int:
        """Count orders.

        Args:
            status: Optional status filter

        Returns:
            Count of orders.
        """
        ...


class PositionRepository(ABC):
    """Abstract position repository interface.

    Handles persistence of Position entities.
    """

    @abstractmethod
    async def create(self, position: Position) -> Position:
        """Create a new position.

        Args:
            position: Position to create

        Returns:
            Created position.
        """
        ...

    @abstractmethod
    async def update(self, position: Position) -> Position:
        """Update an existing position.

        Args:
            position: Position to update

        Returns:
            Updated position.
        """
        ...

    @abstractmethod
    async def get_by_id(self, position_id: EntityId) -> Position | None:
        """Get position by ID.

        Args:
            position_id: Position ID

        Returns:
            Position if found.
        """
        ...

    @abstractmethod
    async def get_by_symbol(self, symbol: Symbol) -> Position | None:
        """Get open position by symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Position if found.
        """
        ...

    @abstractmethod
    async def get_all_open(self) -> list[Position]:
        """Get all open positions.

        Returns:
            List of open positions.
        """
        ...

    @abstractmethod
    async def get_closed(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> list[Position]:
        """Get closed positions.

        Args:
            start_time: Optional start time
            end_time: Optional end time
            limit: Maximum results

        Returns:
            List of closed positions.
        """
        ...

    @abstractmethod
    async def get_by_strategy(self, strategy_id: str) -> list[Position]:
        """Get positions by strategy ID.

        Args:
            strategy_id: Strategy ID

        Returns:
            List of positions.
        """
        ...

    @abstractmethod
    async def delete(self, position_id: EntityId) -> None:
        """Delete a position.

        Args:
            position_id: Position ID to delete
        """
        ...


class PortfolioRepository(ABC):
    """Abstract portfolio repository interface."""

    @abstractmethod
    async def create(self, portfolio: Portfolio) -> Portfolio:
        """Create a new portfolio."""
        ...

    @abstractmethod
    async def update(self, portfolio: Portfolio) -> Portfolio:
        """Update an existing portfolio."""
        ...

    @abstractmethod
    async def get_by_id(self, portfolio_id: EntityId) -> Portfolio | None:
        """Get portfolio by ID."""
        ...

    @abstractmethod
    async def get_by_name(self, name: str) -> Portfolio | None:
        """Get portfolio by name."""
        ...

    @abstractmethod
    async def get_all(self) -> list[Portfolio]:
        """Get all portfolios."""
        ...


class CandleRepository(ABC):
    """Abstract candle repository interface.

    Handles persistence of OHLCV candle data.
    Optimized for time-series queries.
    """

    @abstractmethod
    async def create(self, candle: Candle) -> Candle:
        """Create a new candle.

        Args:
            candle: Candle to create

        Returns:
            Created candle.
        """
        ...

    @abstractmethod
    async def create_many(self, candles: list[Candle]) -> list[Candle]:
        """Create multiple candles.

        Args:
            candles: Candles to create

        Returns:
            Created candles.
        """
        ...

    @abstractmethod
    async def get_candle(
        self,
        symbol: Symbol,
        timeframe: Timeframe,
        open_time: datetime,
    ) -> Candle | None:
        """Get a specific candle.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            open_time: Candle open time

        Returns:
            Candle if found.
        """
        ...

    @abstractmethod
    async def get_candles(
        self,
        symbol: Symbol,
        timeframe: Timeframe,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
    ) -> list[Candle]:
        """Get candles in time range.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            start_time: Start of range
            end_time: End of range
            limit: Maximum results

        Returns:
            List of candles.
        """
        ...

    @abstractmethod
    async def get_latest(
        self,
        symbol: Symbol,
        timeframe: Timeframe,
        count: int = 1,
    ) -> list[Candle]:
        """Get latest candles.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            count: Number of candles

        Returns:
            List of latest candles.
        """
        ...

    @abstractmethod
    async def delete_range(
        self,
        symbol: Symbol,
        timeframe: Timeframe,
        start_time: datetime,
        end_time: datetime,
    ) -> int:
        """Delete candles in time range.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            start_time: Start of range
            end_time: End of range

        Returns:
            Number of candles deleted.
        """
        ...


class SignalRepository(ABC):
    """Abstract signal repository interface."""

    @abstractmethod
    async def save(self, signal_id: str, signal_data: dict[str, Any]) -> None:
        """Save a trading signal."""
        ...

    @abstractmethod
    async def get(self, signal_id: str) -> dict[str, Any] | None:
        """Get a signal by ID."""
        ...

    @abstractmethod
    async def get_by_strategy(
        self,
        strategy_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Get signals by strategy."""
        ...
