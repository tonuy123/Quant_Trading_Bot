"""Exchange adapter base - Canonical contracts re-exported from domain.

The canonical exchange contracts (ExchangeAdapter, MarketDataProvider,
OrderRequest, OrderResponse, TickerData, BalanceData) live in
``packages.domain.interfaces.exchange`` so the domain layer never depends
on infrastructure. This module re-exports them for infrastructure
convenience.
"""

from packages.domain.interfaces.exchange import (
    BalanceData,
    ExchangeAdapter,
    MarketDataProvider,
    OrderRequest,
    OrderResponse,
    TickerData,
)

__all__ = [
    "BalanceData",
    "ExchangeAdapter",
    "MarketDataProvider",
    "OrderRequest",
    "OrderResponse",
    "TickerData",
]
