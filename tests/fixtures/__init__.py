"""Test fixtures and configuration."""

import asyncio
from datetime import datetime
from decimal import Decimal

import pytest

from packages.domain.entities.order import Order
from packages.domain.entities.portfolio import Portfolio
from packages.domain.entities.position import Position
from packages.domain.enums import OrderSide, OrderStatus, OrderType, PositionSide
from packages.domain.value_objects import Money, Price, Quantity, Symbol


@pytest.fixture
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def sample_symbol():
    """Create sample symbol."""
    return Symbol("BTCUSDT", "BTC", "USDT")


@pytest.fixture
def sample_price():
    """Create sample price."""
    return Price(Decimal("50000"), "BTC", "USDT")


@pytest.fixture
def sample_quantity():
    """Create sample quantity."""
    return Quantity(Decimal("0.1"))


@pytest.fixture
def sample_money():
    """Create sample money."""
    return Money(Decimal("10000"), "USDT")


@pytest.fixture
def sample_order(sample_symbol, sample_quantity, sample_price):
    """Create sample order."""
    return Order(
        id=None,  # Will be assigned
        symbol=sample_symbol,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=sample_quantity,
        price=sample_price,
    )


@pytest.fixture
def sample_position(sample_symbol, sample_quantity, sample_price):
    """Create sample position."""
    return Position(
        id=None,
        symbol=sample_symbol,
        side=PositionSide.LONG,
        quantity=sample_quantity,
        entry_price=sample_price,
        current_price=sample_price,
    )


@pytest.fixture
def sample_portfolio(sample_money):
    """Create sample portfolio."""
    return Portfolio(
        id=None,
        name="Test Portfolio",
        initial_equity=sample_money,
        cash_balance=sample_money,
    )
