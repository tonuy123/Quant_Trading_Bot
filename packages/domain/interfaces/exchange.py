"""Exchange adapter interface - Contract for exchange connectivity."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from packages.domain.entities.order import Order
    from packages.domain.enums import OrderSide, OrderType, TimeInForce
    from packages.domain.value_objects import Price, Quantity, Symbol


@dataclass
class OrderRequest:
    """Request to place an order on exchange."""

    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Quantity
    price: Price | None = None
    stop_price: Price | None = None
    time_in_force: TimeInForce | None = None
    client_order_id: str | None = None
    strategy_id: str | None = None


@dataclass
class OrderResponse:
    """Response from exchange after placing an order."""

    success: bool
    exchange_order_id: str | None = None
    client_order_id: str | None = None
    status: str | None = None
    message: str | None = None
    error_code: str | None = None


@dataclass
class TickerData:
    """Real-time ticker data."""

    symbol: Symbol
    bid_price: Decimal
    ask_price: Decimal
    last_price: Decimal
    volume_24h: Decimal
    timestamp: datetime


@dataclass
class BalanceData:
    """Account balance data."""

    currency: str
    free: Decimal
    locked: Decimal


class ExchangeAdapter(ABC):
    """Abstract exchange adapter interface.

    All exchange implementations must conform to this contract.
    This ensures strategies and execution logic are exchange-agnostic.
    """

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Get exchange name."""
        ...

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if connected to exchange."""
        ...

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to exchange."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        ...

    @abstractmethod
    async def place_order(self, request: OrderRequest) -> OrderResponse:
        """Place an order on exchange.

        Args:
            request: Order request details

        Returns:
            Order response from exchange.
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: Symbol) -> OrderResponse:
        """Cancel an order.

        Args:
            order_id: Exchange order ID
            symbol: Symbol of the order

        Returns:
            Cancellation response.
        """
        ...

    @abstractmethod
    async def get_order_status(self, order_id: str, symbol: Symbol) -> Order | None:
        """Get order status from exchange.

        Args:
            order_id: Exchange order ID
            symbol: Symbol of the order

        Returns:
            Order object if found.
        """
        ...

    @abstractmethod
    async def get_account_balances(self) -> list[BalanceData]:
        """Get all account balances.

        Returns:
            List of balance data.
        """
        ...

    @abstractmethod
    async def get_positions(self) -> list[dict[str, Any]]:
        """Get all open positions.

        Returns:
            List of position dictionaries.
        """
        ...

    @abstractmethod
    async def get_symbol_info(self, symbol: Symbol) -> dict[str, Any] | None:
        """Get symbol trading information.

        Args:
            symbol: Trading symbol

        Returns:
            Symbol info dict or None.
        """
        ...


class MarketDataProvider(ABC):
    """Abstract market data provider interface.

    Handles all market data ingestion: WebSocket streams, REST API,
    and historical data.
    """

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Get exchange name."""
        ...

    @abstractmethod
    async def connect(self) -> None:
        """Connect to market data feed."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from market data feed."""
        ...

    @abstractmethod
    def subscribe_ticker(self, symbol: Symbol) -> AsyncIterator[TickerData]:
        """Subscribe to ticker updates for a symbol.

        Args:
            symbol: Trading symbol

        Yields:
            TickerData updates.
        """
        ...

    @abstractmethod
    def subscribe_trades(self, symbol: Symbol) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to trade updates for a symbol.

        Args:
            symbol: Trading symbol

        Yields:
            Trade updates.
        """
        ...

    @abstractmethod
    def subscribe_candles(self, symbol: Symbol, timeframe: str) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to candle updates for a symbol.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe (e.g., "1m", "1h")

        Yields:
            Candle updates.
        """
        ...

    @abstractmethod
    async def get_historical_candles(
        self,
        symbol: Symbol,
        timeframe: str,
        start_time: datetime,
        end_time: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get historical candle data.

        Args:
            symbol: Trading symbol
            timeframe: Candle timeframe
            start_time: Start of time range
            end_time: End of time range
            limit: Maximum number of candles

        Returns:
            List of historical candles.
        """
        ...

    @abstractmethod
    async def get_historical_trades(
        self,
        symbol: Symbol,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get historical trade data.

        Args:
            symbol: Trading symbol
            start_time: Start of time range
            end_time: End of time range
            limit: Maximum number of trades

        Returns:
            List of historical trades.
        """
        ...
