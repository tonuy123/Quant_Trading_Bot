"""Order service - Orchestrates order-related operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from packages.domain.entities.order import Order
from packages.domain.enums import OrderSide, OrderType
from packages.domain.errors import InvalidOrderStateError, OrderNotFoundError
from packages.domain.events import OrderCancelled, OrderSubmitted
from packages.domain.interfaces import (
    EventPublisher,
    ExchangeAdapter,
    OrderRequest,
    UnitOfWorkFactory,
)
from packages.domain.value_objects import EntityId, OrderIntent, Price, Quantity, Symbol
from packages.risk.services import RiskManager

if TYPE_CHECKING:
    from packages.domain.value_objects import Price, Quantity


@dataclass
class PlaceOrderResult:
    """Result of placing an order."""

    success: bool
    order: Order | None = None
    error: str | None = None


class OrderService:
    """Service for managing orders.

    Orchestrates the order lifecycle:
    1. Validate order intent
    2. Create order entity
    3. Submit to exchange via adapter
    4. Track order state
    5. Handle fills and updates
    """

    def __init__(
        self,
        unit_of_work_factory: UnitOfWorkFactory,
        exchange_adapter: ExchangeAdapter,
        risk_manager: RiskManager,
        event_publisher: EventPublisher,
    ) -> None:
        """Initialize order service.

        Args:
            unit_of_work_factory: Factory for database transactions
            exchange_adapter: Exchange connection
            risk_manager: Risk management service
            event_publisher: Event distribution
        """
        self._uow_factory = unit_of_work_factory
        self._exchange = exchange_adapter
        self._risk = risk_manager
        self._events = event_publisher

    async def place_order(
        self,
        symbol: Symbol,
        side: OrderSide,
        order_type: OrderType,
        quantity: Quantity,
        price: Price | None = None,
        strategy_id: str | None = None,
        **kwargs: object,
    ) -> PlaceOrderResult:
        """Place a new order.

        Args:
            symbol: Trading symbol
            side: BUY or SELL
            order_type: Order type
            quantity: Order quantity
            price: Limit price (for limit orders)
            strategy_id: Strategy placing the order

        Returns:
            PlaceOrderResult with order or error.
        """
        # Create order intent for risk check
        intent = OrderIntent(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            strategy_id=strategy_id,
            context=dict(kwargs) if kwargs else None,
        )

        # Submit to risk manager
        risk_result = await self._risk.check_order(intent)
        if not risk_result.approved:
            return PlaceOrderResult(
                success=False,
                error=f"Risk rejected: {risk_result.reason}",
            )

        # Create order entity
        order = Order(
            id=None,  # Will be assigned
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=(
                Quantity(risk_result.adjusted_quantity)
                if risk_result.adjusted_quantity is not None
                else quantity
            ),
            price=(
                Price(risk_result.adjusted_price, symbol.base, symbol.quote)
                if risk_result.adjusted_price is not None
                else price
            ),
            strategy_id=strategy_id,
        )

        # Submit to exchange
        exchange_response = await self._exchange.place_order(
            OrderRequest(
                symbol=order.symbol,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                stop_price=order.stop_price,
                time_in_force=order.time_in_force,
                client_order_id=order.client_order_id,
                strategy_id=strategy_id,
            )
        )
        if not exchange_response.success:
            return PlaceOrderResult(
                success=False,
                error=f"Exchange error: {exchange_response.message}",
            )

        # Update order with exchange ID
        order.exchange_order_id = exchange_response.exchange_order_id
        order.submit()

        # Persist order
        async with self._uow_factory.transaction() as uow:
            await uow.orders.create(order)

        # Publish event
        await self._events.publish(
            OrderSubmitted(
                order_id=str(order.id),
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                symbol=order.symbol,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity.value,
                price=order.price.value if order.price else None,
                strategy_id=strategy_id,
            )
        )

        return PlaceOrderResult(success=True, order=order)

    async def cancel_order(self, order_id: str, symbol: Symbol) -> bool:
        """Cancel an order.

        Args:
            order_id: Order ID
            symbol: Trading symbol

        Returns:
            True if cancelled.
        """
        async with self._uow_factory.transaction() as uow:
            order = await uow.orders.get_by_id(EntityId(order_id))
            if not order:
                raise OrderNotFoundError(order_id)

            if order.is_terminal:
                raise InvalidOrderStateError(
                    order_id,
                    str(order.status),
                    "cancel",
                )

            # Submit cancellation to exchange
            response = await self._exchange.cancel_order(order_id, symbol)
            if not response.success:
                return False

            order.cancel()

            # Update in database
            await uow.orders.update(order)

        # Publish event
        await self._events.publish(
            OrderCancelled(
                order_id=str(order.id),
                exchange_order_id=order.exchange_order_id or "",
                symbol=order.symbol,
                cancelled_quantity=order.remaining_quantity.value,
                filled_quantity=order.filled_quantity.value,
            )
        )

        return True

    async def get_order(self, order_id: str) -> Order | None:
        """Get an order by ID.

        Args:
            order_id: Order ID

        Returns:
            Order if found.
        """
        async with self._uow_factory.transaction() as uow:
            return await uow.orders.get_by_id(EntityId(order_id))

    async def get_active_orders(self, symbol: Symbol | None = None) -> list[Order]:
        """Get all active orders.

        Args:
            symbol: Optional symbol filter

        Returns:
            List of active orders.
        """
        async with self._uow_factory.transaction() as uow:
            return await uow.orders.get_active_orders(symbol)
