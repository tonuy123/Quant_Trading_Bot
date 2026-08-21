"""Domain errors - Custom exceptions for domain operations."""

from packages.domain.errors.base import (
    DomainError,
    EntityNotFoundError,
    StateTransitionError,
    ValidationError,
)
from packages.domain.errors.order_errors import (
    InvalidOrderStateError,
    OrderError,
    OrderNotFoundError,
    OrderValidationError,
)
from packages.domain.errors.position_errors import (
    PositionAlreadyExistsError,
    PositionError,
    PositionNotFoundError,
)
from packages.domain.errors.risk_errors import (
    RiskError,
    RiskLimitExceededError,
    RiskRejectedError,
)

__all__ = [
    "DomainError",
    "EntityNotFoundError",
    "InvalidOrderStateError",
    "OrderError",
    "OrderNotFoundError",
    "OrderValidationError",
    "PositionAlreadyExistsError",
    "PositionError",
    "PositionNotFoundError",
    "RiskError",
    "RiskLimitExceededError",
    "RiskRejectedError",
    "StateTransitionError",
    "ValidationError",
]
