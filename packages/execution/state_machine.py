"""Order state machine - Manages order lifecycle transitions."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, ClassVar

from packages.domain.enums import OrderStatus

if TYPE_CHECKING:
    from packages.domain.entities.order import Order


class OrderStateMachine:
    """State machine for order lifecycle.

    Defines valid state transitions for orders.
    Ensures orders follow the correct lifecycle.
    """

    # Valid transitions: current_state -> [allowed_next_states]
    TRANSITIONS: ClassVar[dict[OrderStatus, set[OrderStatus]]] = {
        OrderStatus.PENDING: {
            OrderStatus.SUBMITTED,
            OrderStatus.CANCELLED,
        },
        OrderStatus.SUBMITTED: {
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        },
        OrderStatus.ACCEPTED: {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.EXPIRED,
        },
        OrderStatus.PARTIALLY_FILLED: {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
        },
        OrderStatus.FILLED: set(),  # Terminal state
        OrderStatus.CANCELLED: set(),  # Terminal state
        OrderStatus.REJECTED: set(),  # Terminal state
        OrderStatus.EXPIRED: set(),  # Terminal state
    }

    @classmethod
    def can_transition(cls, current: OrderStatus, target: OrderStatus) -> bool:
        """Check if transition is valid.

        Args:
            current: Current order status
            target: Target order status

        Returns:
            True if transition is valid.
        """
        allowed = cls.TRANSITIONS.get(current, set())
        return target in allowed

    @classmethod
    def get_allowed_transitions(cls, current: OrderStatus) -> set[OrderStatus]:
        """Get allowed transitions from current state.

        Args:
            current: Current order status

        Returns:
            Set of allowed next states.
        """
        return cls.TRANSITIONS.get(current, set())

    @classmethod
    def transition(cls, order: Order, target: OrderStatus) -> bool:
        """Attempt to transition order to target state.

        Args:
            order: Order to transition
            target: Target state

        Returns:
            True if transition succeeded.

        Raises:
            ValueError: If transition is not valid.
        """
        if not cls.can_transition(order.status, target):
            allowed = cls.get_allowed_transitions(order.status)
            raise ValueError(
                f"Cannot transition order from {order.status} to {target}. Allowed: {allowed}"
            )

        order.status = target
        order.mark_updated()
        return True

    @classmethod
    def submit(cls, order: Order) -> None:
        """Submit order to exchange.

        Args:
            order: Order to submit
        """
        cls.transition(order, OrderStatus.SUBMITTED)
        order.submitted_at = order.updated_at

    @classmethod
    def partial_fill(cls, order: Order, quantity: Decimal, price: Decimal) -> None:
        """Record partial fill.

        Args:
            order: Order to fill
            quantity: Fill quantity
            price: Fill price
        """
        from packages.domain.value_objects import Price, Quantity

        cls.transition(order, OrderStatus.PARTIALLY_FILLED)
        order.partial_fill(Quantity(quantity), Price(price, "", ""))

    @classmethod
    def fill(cls, order: Order, quantity: Decimal, price: Decimal) -> None:
        """Record full fill.

        Args:
            order: Order to fill
            quantity: Fill quantity
            price: Fill price
        """
        from packages.domain.value_objects import Price, Quantity

        cls.transition(order, OrderStatus.FILLED)
        order.fill(Quantity(quantity), Price(price, "", ""))

    @classmethod
    def cancel(cls, order: Order, reason: str | None = None) -> None:
        """Cancel order.

        Args:
            order: Order to cancel
            reason: Cancellation reason
        """
        cls.transition(order, OrderStatus.CANCELLED)
        order.cancelled_at = order.updated_at
        order.rejection_reason = reason

    @classmethod
    def reject(cls, order: Order, reason: str) -> None:
        """Reject order.

        Args:
            order: Order to reject
            reason: Rejection reason
        """
        cls.transition(order, OrderStatus.REJECTED)
        order.reject(reason)

    @classmethod
    def expire(cls, order: Order) -> None:
        """Expire order.

        Args:
            order: Order to expire
        """
        cls.transition(order, OrderStatus.EXPIRED)
