"""Order executor service."""

from __future__ import annotations

from typing import TYPE_CHECKING

from packages.domain.entities.order import Order
from packages.domain.enums import OrderStatus
from packages.domain.interfaces import ExchangeAdapter, OrderRequest, UnitOfWorkFactory
from packages.domain.value_objects import Symbol
from packages.execution.contracts import ExecutionResult

if TYPE_CHECKING:
    pass


class OrderExecutor:
    """Executes orders through exchange adapters.

    The executor:
    1. Receives validated orders
    2. Submits to exchange via adapter
    3. Tracks order state
    4. Handles fills and updates
    """

    def __init__(
        self,
        unit_of_work_factory: UnitOfWorkFactory,
        exchange_adapter: ExchangeAdapter,
    ) -> None:
        """Initialize executor.

        Args:
            unit_of_work_factory: Factory for database transactions
            exchange_adapter: Exchange connection
        """
        self._uow_factory = unit_of_work_factory
        self._exchange = exchange_adapter

    async def execute(self, order: Order) -> ExecutionResult:
        """Execute an order.

        Args:
            order: Order to execute

        Returns:
            Execution result.
        """
        try:
            # Submit to exchange
            request = OrderRequest(
                symbol=order.symbol,
                side=order.side,
                order_type=order.order_type,
                quantity=order.quantity,
                price=order.price,
                stop_price=order.stop_price,
                time_in_force=order.time_in_force,
                client_order_id=order.client_order_id,
                strategy_id=order.strategy_id,
            )
            response = await self._exchange.place_order(request)

            if not response.success:
                return ExecutionResult(
                    success=False,
                    order_id=str(order.id),
                    message=response.message,
                    error_code=response.error_code,
                )

            # Update order
            order.exchange_order_id = response.exchange_order_id
            order.status = OrderStatus.SUBMITTED

            # Persist
            async with self._uow_factory.transaction() as uow:
                await uow.orders.update(order)

            return ExecutionResult(
                success=True,
                order_id=str(order.id),
                exchange_order_id=response.exchange_order_id,
                status="SUBMITTED",
            )

        except Exception as e:
            return ExecutionResult(
                success=False,
                order_id=str(order.id),
                message=str(e),
            )

    async def cancel(self, order_id: str, symbol: Symbol) -> ExecutionResult:
        """Cancel an order.

        Args:
            order_id: Order ID
            symbol: Trading symbol

        Returns:
            Cancellation result.
        """
        try:
            response = await self._exchange.cancel_order(order_id, symbol)

            if not response.success:
                return ExecutionResult(
                    success=False,
                    order_id=order_id,
                    message=response.message,
                )

            return ExecutionResult(
                success=True,
                order_id=order_id,
                status="CANCELLED",
            )

        except Exception as e:
            return ExecutionResult(
                success=False,
                order_id=order_id,
                message=str(e),
            )

    async def get_status(self, order_id: str, symbol: Symbol) -> Order | None:
        """Get order status from exchange.

        Args:
            order_id: Exchange order ID
            symbol: Trading symbol

        Returns:
            Updated order if found.
        """
        return await self._exchange.get_order_status(order_id, symbol)
