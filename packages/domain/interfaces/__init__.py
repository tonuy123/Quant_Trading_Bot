"""Domain interfaces - Contracts that infrastructure must implement.

These interfaces define the boundaries between domain logic and external concerns.
Infrastructure implementations must conform to these contracts.
"""

from packages.domain.interfaces.clock import Clock, TimeProvider
from packages.domain.interfaces.event_publisher import DomainEventHandler, EventPublisher
from packages.domain.interfaces.exchange import (
    BalanceData,
    ExchangeAdapter,
    MarketDataProvider,
    OrderRequest,
    OrderResponse,
    TickerData,
)
from packages.domain.interfaces.market_data import MarketDataCache
from packages.domain.interfaces.repositories import (
    CandleRepository,
    OrderRepository,
    PortfolioRepository,
    PositionRepository,
    SignalRepository,
)
from packages.domain.interfaces.unit_of_work import UnitOfWork, UnitOfWorkFactory

__all__ = [
    "BalanceData",
    "CandleRepository",
    "Clock",
    "DomainEventHandler",
    "EventPublisher",
    "ExchangeAdapter",
    "MarketDataCache",
    "MarketDataProvider",
    "OrderRepository",
    "OrderRequest",
    "OrderResponse",
    "PortfolioRepository",
    "PositionRepository",
    "SignalRepository",
    "TickerData",
    "TimeProvider",
    "UnitOfWork",
    "UnitOfWorkFactory",
]
