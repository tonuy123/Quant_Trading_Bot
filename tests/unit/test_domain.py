"""Unit tests for domain entities."""

from decimal import Decimal

from packages.domain.entities.order import Order
from packages.domain.entities.position import Position
from packages.domain.enums import OrderSide, OrderStatus, OrderType, PositionSide
from packages.domain.value_objects import Price, Quantity, Symbol


class TestOrder:
    """Tests for Order entity."""

    def test_order_creation(self):
        """Test order creation with valid data."""
        symbol = Symbol("BTCUSDT", "BTC", "USDT")
        order = Order(
            id=None,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Quantity(Decimal("0.1")),
            price=Price(Decimal("50000"), "BTC", "USDT"),
        )

        assert order.symbol == symbol
        assert order.side == OrderSide.BUY
        assert order.status == OrderStatus.PENDING

    def test_order_submit(self):
        """Test order submission."""
        symbol = Symbol("BTCUSDT", "BTC", "USDT")
        order = Order(
            id=None,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Quantity(Decimal("0.1")),
            price=Price(Decimal("50000"), "BTC", "USDT"),
        )

        order.submit()
        assert order.status == OrderStatus.SUBMITTED
        assert order.submitted_at is not None

    def test_order_fill(self):
        """Test order fill."""
        symbol = Symbol("BTCUSDT", "BTC", "USDT")
        order = Order(
            id=None,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Quantity(Decimal("0.1")),
        )

        order.submit()
        order.fill(
            Quantity(Decimal("0.1")),
            Price(Decimal("50000"), "BTC", "USDT"),
        )

        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity.value == Decimal("0.1")
        assert order.filled_at is not None

    def test_order_cancel(self):
        """Test order cancellation."""
        symbol = Symbol("BTCUSDT", "BTC", "USDT")
        order = Order(
            id=None,
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Quantity(Decimal("0.1")),
            price=Price(Decimal("50000"), "BTC", "USDT"),
        )

        order.submit()
        order.cancel()

        assert order.status == OrderStatus.CANCELLED
        assert order.cancelled_at is not None


class TestPosition:
    """Tests for Position entity."""

    def test_position_creation(self):
        """Test position creation."""
        symbol = Symbol("BTCUSDT", "BTC", "USDT")
        position = Position(
            id=None,
            symbol=symbol,
            side=PositionSide.LONG,
            quantity=Quantity(Decimal("0.1")),
            entry_price=Price(Decimal("50000"), "BTC", "USDT"),
        )

        assert position.symbol == symbol
        assert position.side == PositionSide.LONG
        assert position.quantity.value == Decimal("0.1")
        assert position.is_long
        assert not position.is_short

    def test_position_pnl_calculation(self):
        """Test unrealized PnL calculation."""
        symbol = Symbol("BTCUSDT", "BTC", "USDT")
        position = Position(
            id=None,
            symbol=symbol,
            side=PositionSide.LONG,
            quantity=Quantity(Decimal("0.1")),
            entry_price=Price(Decimal("50000"), "BTC", "USDT"),
        )

        # Price goes up
        position.update_price(Price(Decimal("55000"), "BTC", "USDT"))
        assert position.unrealized_pnl.value == Decimal("500")  # 0.1 * 5000

        # Price goes down
        position.update_price(Price(Decimal("45000"), "BTC", "USDT"))
        assert position.unrealized_pnl.value == Decimal("-500")  # 0.1 * -5000
