"""Order-specific errors."""

from typing import Any

from packages.domain.errors.base import (
    DomainError,
    EntityNotFoundError,
    StateTransitionError,
    ValidationError,
)


class OrderError(DomainError):
    """Base class for order-related errors."""

    def __init__(
        self, message: str, order_id: str | None = None, details: dict[str, Any] | None = None
    ) -> None:
        """Initialize order error."""
        super().__init__(message, details)
        self.order_id = order_id


class OrderValidationError(OrderError, ValidationError):
    """Error raised when order validation fails."""

    def __init__(
        self,
        message: str,
        order_id: str | None = None,
        field: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize order validation error."""
        OrderError.__init__(self, message, order_id, details)
        self.field = field


class OrderNotFoundError(OrderError, EntityNotFoundError):
    """Error raised when an order cannot be found."""

    def __init__(self, order_id: str) -> None:
        """Initialize order not found error."""
        EntityNotFoundError.__init__(self, "Order", order_id)
        self.order_id = order_id


class InvalidOrderStateError(OrderError, StateTransitionError):
    """Error raised when an invalid order state transition is attempted."""

    def __init__(
        self,
        order_id: str,
        current_state: str,
        attempted_action: str,
        allowed_transitions: list[str] | None = None,
    ) -> None:
        """Initialize invalid order state error."""
        message = f"Cannot {attempted_action} order '{order_id}' in state '{current_state}'"
        if allowed_transitions:
            message += f". Allowed: {', '.join(allowed_transitions)}"
        OrderError.__init__(self, message, order_id)
        StateTransitionError.__init__(
            self, "Order", current_state, attempted_action, allowed_transitions
        )


class InsufficientBalanceError(OrderError):
    """Error raised when account has insufficient balance for order."""

    def __init__(self, order_id: str | None, required: str, available: str, currency: str) -> None:
        """Initialize insufficient balance error."""
        message = f"Insufficient {currency} balance: required {required}, available {available}"
        details = {"required": required, "available": available, "currency": currency}
        OrderError.__init__(self, message, order_id, details)


class OrderRejectedError(OrderError):
    """Error raised when order is rejected by exchange."""

    def __init__(self, order_id: str | None, exchange_code: str, reason: str) -> None:
        """Initialize order rejected error."""
        message = f"Order rejected by exchange: {reason} (code: {exchange_code})"
        details = {"exchange_code": exchange_code, "reason": reason}
        OrderError.__init__(self, message, order_id, details)
        self.exchange_code = exchange_code
        self.reason = reason
