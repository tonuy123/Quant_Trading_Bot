"""Query definitions - Read operations that don't change state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from packages.domain.enums import OrderStatus, PositionSide, Timeframe
from packages.domain.value_objects import EntityId, Symbol


@dataclass
class Query:
    """Base class for all queries.

    Queries represent read-only operations.
    They are immutable and contain all data needed for execution.
    """

    query_id: str


@dataclass
class GetOrderQuery(Query):
    """Query to get a single order."""

    query_id: str
    order_id: EntityId | None = None
    exchange_order_id: str | None = None
    client_order_id: str | None = None


@dataclass
class GetOrdersQuery(Query):
    """Query to get multiple orders."""

    query_id: str
    symbol: Symbol | None = None
    status: OrderStatus | None = None
    strategy_id: str | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: int = 100


@dataclass
class GetPositionQuery(Query):
    """Query to get a position."""

    query_id: str
    symbol: Symbol | None = None
    position_id: EntityId | None = None


@dataclass
class GetPositionsQuery(Query):
    """Query to get all positions."""

    query_id: str
    include_closed: bool = False
    strategy_id: str | None = None


@dataclass
class GetPortfolioQuery(Query):
    """Query to get portfolio details."""

    query_id: str
    portfolio_id: EntityId | None = None
    name: str | None = None


@dataclass
class GetAccountBalanceQuery(Query):
    """Query to get account balance."""

    query_id: str
    currency: str | None = None


@dataclass
class GetCandlesQuery(Query):
    """Query to get historical candles."""

    query_id: str
    symbol: Symbol
    timeframe: Timeframe
    start_time: datetime
    end_time: datetime
    limit: int = 1000


@dataclass
class GetTickerQuery(Query):
    """Query to get current ticker data."""

    query_id: str
    symbol: Symbol


@dataclass
class GetPnLReportQuery(Query):
    """Query to get PnL report."""

    query_id: str
    portfolio_id: EntityId
    start_time: datetime | None = None
    end_time: datetime | None = None
    period: str = "daily"  # daily, weekly, monthly


@dataclass
class GetRiskMetricsQuery(Query):
    """Query to get risk metrics."""

    query_id: str
    portfolio_id: EntityId


@dataclass
class GetWorkerStatusQuery(Query):
    """Query to get worker status."""

    query_id: str
    worker_name: str | None = None  # None = all workers
