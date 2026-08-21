"""Protocols for market data adapters.

These protocols define the exchange-neutral interface that
adapters must implement. No provider-specific types should
cross these boundaries.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Collection, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from packages.market_data.adapters.value_types import (
        ConnectionSnapshot,
        MarketSymbol,
        RawMarketMessage,
        StreamSubscription,
    )


@runtime_checkable
class PublicMarketDataFeed(Protocol):
    """Public market data WebSocket feed protocol.

    Adapters implementing this protocol handle WebSocket connections
    to exchange public market data streams. They must:
    - Translate exchange-neutral subscriptions to provider stream names
    - Capture raw bytes with metadata before parsing
    - Never expose API keys or credentials
    - Support graceful shutdown
    """

    async def connect(self) -> ConnectionSnapshot:
        """Establish WebSocket connection.

        Returns:
            ConnectionSnapshot with initial state.

        Raises:
            ConnectionError: If connection fails.
        """
        ...

    async def subscribe(self, items: Collection[StreamSubscription]) -> None:
        """Subscribe to market data streams.

        Args:
            items: Collection of stream subscriptions.

        Raises:
            SubscriptionError: If subscription fails.
        """
        ...

    async def unsubscribe(self, items: Collection[StreamSubscription]) -> None:
        """Unsubscribe from market data streams.

        Args:
            items: Collection of stream subscriptions to remove.
        """
        ...

    async def raw_messages(self) -> AsyncIterator[RawMarketMessage]:
        """Yield raw market messages as they arrive.

        Yields:
            RawMarketMessage with provider payload and metadata.

        Raises:
            ConnectionError: If connection is lost.
        """
        ...

    async def snapshot(self) -> ConnectionSnapshot:
        """Get current connection snapshot.

        Returns:
            Current connection state.
        """
        ...

    async def close(self, reason: str) -> None:
        """Close the connection gracefully.

        Args:
            reason: Reason for closing (for logging).
        """
        ...


class PublicMarketDataHistory(Protocol):
    """Public market data REST history protocol.

    Adapters implementing this protocol handle REST API calls
    for historical market data and gap recovery. They must:
    - Use public endpoints only (no authentication)
    - Respect rate limits
    - Translate provider responses to raw messages
    """

    async def get_server_time(self) -> datetime:
        """Get exchange server time.

        Returns:
            Current server time (UTC-aware).

        Raises:
            RateLimitError: If rate limited.
            RequestError: If request fails.
        """
        ...

    async def get_closed_klines(
        self,
        symbol: MarketSymbol,
        interval: str,
        start_inclusive: datetime,
        end_exclusive: datetime,
        page_limit: int = 1000,
    ) -> Sequence[RawMarketMessage]:
        """Get historical closed klines.

        Args:
            symbol: Trading symbol.
            interval: Kline interval (e.g., "1m", "5m").
            start_inclusive: Start of time range (inclusive).
            end_exclusive: End of time range (exclusive).
            page_limit: Maximum candles per page.

        Returns:
            Sequence of raw kline messages.

        Raises:
            RateLimitError: If rate limited.
            RequestError: If request fails.
        """
        ...

    async def get_ticker_snapshot(self, symbol: MarketSymbol) -> RawMarketMessage:
        """Get a current public ticker snapshot.

        A result represents present state only. It must be normalized with
        source rest_snapshot and is never a replay of missed ticker updates.
        """
        ...
