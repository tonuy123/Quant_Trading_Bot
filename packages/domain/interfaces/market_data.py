"""Market data cache interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from packages.domain.entities.candle import Candle
    from packages.domain.enums import Timeframe
    from packages.domain.value_objects import Price, Symbol


class MarketDataCache(ABC):
    """Abstract market data cache interface.

    Provides caching layer for market data to reduce
    database/exchange queries.
    """

    @abstractmethod
    async def get_ticker(self, symbol: Symbol) -> dict[str, Any] | None:
        """Get cached ticker data.

        Args:
            symbol: Trading symbol

        Returns:
            Ticker data if cached.
        """
        ...

    @abstractmethod
    async def set_ticker(self, symbol: Symbol, data: dict[str, Any], ttl: int = 60) -> None:
        """Cache ticker data.

        Args:
            symbol: Trading symbol
            data: Ticker data to cache
            ttl: Time to live in seconds
        """
        ...

    @abstractmethod
    async def get_price(self, symbol: Symbol) -> Price | None:
        """Get cached price.

        Args:
            symbol: Trading symbol

        Returns:
            Price if cached.
        """
        ...

    @abstractmethod
    async def set_price(self, symbol: Symbol, price: Price, ttl: int = 5) -> None:
        """Cache price.

        Args:
            symbol: Trading symbol
            price: Price to cache
            ttl: Time to live in seconds
        """
        ...

    @abstractmethod
    async def get_candles(
        self,
        symbol: Symbol,
        timeframe: Timeframe,
        count: int = 100,
    ) -> list[Candle] | None:
        """Get cached candles.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            count: Number of candles

        Returns:
            Candles if cached.
        """
        ...

    @abstractmethod
    async def set_candles(
        self,
        symbol: Symbol,
        timeframe: Timeframe,
        candles: list[Candle],
        ttl: int = 60,
    ) -> None:
        """Cache candles.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            candles: Candles to cache
            ttl: Time to live in seconds
        """
        ...

    @abstractmethod
    async def invalidate_symbol(self, symbol: Symbol) -> None:
        """Invalidate all cached data for a symbol.

        Args:
            symbol: Trading symbol
        """
        ...

    @abstractmethod
    async def get_order_book(
        self,
        symbol: Symbol,
        depth: int = 10,
    ) -> dict[str, Any] | None:
        """Get cached order book.

        Args:
            symbol: Trading symbol
            depth: Order book depth

        Returns:
            Order book if cached.
        """
        ...

    @abstractmethod
    async def set_order_book(
        self,
        symbol: Symbol,
        order_book: dict[str, Any],
        ttl: int = 5,
    ) -> None:
        """Cache order book.

        Args:
            symbol: Trading symbol
            order_book: Order book to cache
            ttl: Time to live in seconds
        """
        ...

    @abstractmethod
    async def acquire_lock(
        self,
        lock_name: str,
        timeout: float = 10.0,
    ) -> bool:
        """Acquire a distributed lock.

        Args:
            lock_name: Lock name
            timeout: Lock timeout in seconds

        Returns:
            True if lock acquired.
        """
        ...

    @abstractmethod
    async def release_lock(self, lock_name: str) -> None:
        """Release a distributed lock.

        Args:
            lock_name: Lock name
        """
        ...
