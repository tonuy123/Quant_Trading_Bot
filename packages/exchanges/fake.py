"""Fake exchange for testing and backtesting."""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Any

from packages.domain.enums import OrderStatus
from packages.domain.interfaces.exchange import (
    BalanceData,
    ExchangeAdapter,
    MarketDataProvider,
    OrderRequest,
    OrderResponse,
    TickerData,
)
from packages.domain.value_objects import Symbol


class FakeExchange(ExchangeAdapter):
    """Fake exchange that simulates order execution.

    Used for:
    - Testing without real exchange
    - Backtesting
    - Development
    """

    def __init__(
        self,
        name: str = "fake",
        latency_ms: int = 100,
        fill_probability: float = 0.9,
    ) -> None:
        """Initialize fake exchange.

        Args:
            name: Exchange name
            latency_ms: Simulated latency
            fill_probability: Probability of immediate fill
        """
        self._name = name
        self._latency_ms = latency_ms
        self._fill_probability = fill_probability
        self._connected = False
        self._balances: dict[str, Decimal] = {"USDT": Decimal("10000")}
        self._orders: dict[str, OrderStatus] = {}
        self._order_counter = 0

    @property
    def exchange_name(self) -> str:
        """Get exchange name."""
        return self._name

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected

    async def connect(self) -> None:
        """Connect to exchange."""
        await asyncio.sleep(self._latency_ms / 1000)
        self._connected = True

    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        self._connected = False

    async def place_order(self, request: OrderRequest) -> OrderResponse:
        """Place a simulated order."""
        self._order_counter += 1
        exchange_order_id = f"FAKE_{self._order_counter}"

        # Simulate latency
        await asyncio.sleep(self._latency_ms / 1000)

        # Simulate random fill
        if random.random() < self._fill_probability:
            self._orders[exchange_order_id] = OrderStatus.FILLED
            return OrderResponse(
                success=True,
                exchange_order_id=exchange_order_id,
                status="FILLED",
                message="Order filled",
            )

        self._orders[exchange_order_id] = OrderStatus.SUBMITTED
        return OrderResponse(
            success=True,
            exchange_order_id=exchange_order_id,
            status="SUBMITTED",
            message="Order submitted",
        )

    async def cancel_order(self, order_id: str, symbol: Symbol) -> OrderResponse:
        """Cancel a simulated order."""
        await asyncio.sleep(self._latency_ms / 1000)
        self._orders[order_id] = OrderStatus.CANCELLED

        return OrderResponse(
            success=True,
            exchange_order_id=order_id,
            status="CANCELLED",
            message="Order cancelled",
        )

    async def get_order_status(self, order_id: str, symbol: Symbol) -> None:
        """Get order status from exchange (stub - not tracked by default)."""
        return None

    async def get_balance(self, currency: str) -> Decimal:
        """Get simulated balance."""
        return self._balances.get(currency, Decimal("0"))

    def set_balance(self, currency: str, balance: Decimal) -> None:
        """Set simulated balance."""
        self._balances[currency] = balance

    async def get_account_balances(self) -> list[BalanceData]:
        """Get all simulated balances."""
        return [
            BalanceData(currency=currency, free=free, locked=Decimal("0"))
            for currency, free in self._balances.items()
        ]

    async def get_positions(self) -> list[dict[str, Any]]:
        """Get all open positions (stub - no position tracking)."""
        return []

    async def get_symbol_info(self, symbol: Symbol) -> dict[str, Any] | None:
        """Get symbol trading information (stub)."""
        return None


class FakeMarketDataProvider(MarketDataProvider):
    """Fake market data provider for testing."""

    def __init__(
        self,
        exchange_name: str = "fake",
        base_price: Decimal = Decimal("50000"),
        volatility: Decimal = Decimal("0.001"),
    ) -> None:
        """Initialize fake provider.

        Args:
            exchange_name: Exchange name
            base_price: Starting price
            volatility: Price volatility per tick
        """
        self._exchange_name = exchange_name
        self._base_price = base_price
        self._volatility = volatility
        self._current_price = base_price
        self._connected = False

    @property
    def exchange_name(self) -> str:
        """Get exchange name."""
        return self._exchange_name

    async def connect(self) -> None:
        """Connect to data feed."""
        self._connected = True

    async def disconnect(self) -> None:
        """Disconnect from data feed."""
        self._connected = False

    async def subscribe_ticker(self, symbol: Symbol) -> AsyncIterator[TickerData]:
        """Subscribe to simulated ticker updates."""
        while self._connected:
            # Update price with random walk
            change = self._current_price * self._volatility * Decimal(str(random.random() - 0.5))
            self._current_price += change

            yield self._build_ticker(symbol)
            await asyncio.sleep(1)  # Tick every second

    async def subscribe_trades(self, symbol: Symbol) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to simulated trade updates (stub)."""
        if False:  # pragma: no cover - generator must remain async
            yield {}
        return

    async def subscribe_candles(
        self, symbol: Symbol, timeframe: str
    ) -> AsyncIterator[dict[str, Any]]:
        """Subscribe to simulated candle updates (stub)."""
        if False:  # pragma: no cover - generator must remain async
            yield {}
        return

    async def get_ticker(self, symbol: Symbol) -> TickerData | None:
        """Get current simulated ticker."""
        if not self._connected:
            return None

        return self._build_ticker(symbol)

    async def get_historical_candles(
        self,
        symbol: Symbol,
        timeframe: str,
        start_time: datetime,
        end_time: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get historical candle data (stub - no history stored)."""
        return []

    async def get_historical_trades(
        self,
        symbol: Symbol,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Get historical trade data (stub - no history stored)."""
        return []

    def _build_ticker(self, symbol: Symbol) -> TickerData:
        """Build a ticker at the current simulated price."""
        return TickerData(
            symbol=symbol,
            bid_price=self._current_price * Decimal("0.999"),
            ask_price=self._current_price * Decimal("1.001"),
            last_price=self._current_price,
            volume_24h=Decimal("1000"),
            timestamp=datetime.utcnow(),
        )

    def set_price(self, price: Decimal) -> None:
        """Set current price."""
        self._current_price = price
