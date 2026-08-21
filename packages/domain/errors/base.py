"""Base domain error classes."""

from typing import Any


class DomainError(Exception):
    """Base class for all domain errors.

    Domain errors represent violations of business rules.
    They are not technical errors but expected failure conditions
    that should be handled gracefully.
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        """Initialize domain error."""
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        """String representation."""
        return self.message

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
        }


class ValidationError(DomainError):
    """Error raised when validation fails.

    Used for:
    - Invalid entity state
    - Invalid input parameters
    - Business rule violations
    """

    def __init__(
        self, message: str, field: str | None = None, details: dict[str, Any] | None = None
    ) -> None:
        """Initialize validation error."""
        super().__init__(message, details)
        self.field = field

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = super().to_dict()
        result["field"] = self.field
        return result


class EntityNotFoundError(DomainError):
    """Error raised when an entity cannot be found.

    Used when:
    - Order ID doesn't exist
    - Position doesn't exist
    - Portfolio doesn't exist
    """

    def __init__(self, entity_type: str, entity_id: str) -> None:
        """Initialize not found error."""
        message = f"{entity_type} with ID '{entity_id}' not found"
        super().__init__(message)
        self.entity_type = entity_type
        self.entity_id = entity_id

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = super().to_dict()
        result["entity_type"] = self.entity_type
        result["entity_id"] = self.entity_id
        return result


class StateTransitionError(DomainError):
    """Error raised when an invalid state transition is attempted.

    Examples:
    - Cancelling a filled order
    - Filling a cancelled order
    - Closing an already closed position
    """

    def __init__(
        self,
        entity_type: str,
        current_state: str,
        attempted_action: str,
        allowed_transitions: list[str] | None = None,
    ) -> None:
        """Initialize state transition error."""
        message = f"Cannot {attempted_action} {entity_type} in state '{current_state}'"
        if allowed_transitions:
            message += f". Allowed transitions: {', '.join(allowed_transitions)}"
        super().__init__(message)
        self.entity_type = entity_type
        self.current_state = current_state
        self.attempted_action = attempted_action
        self.allowed_transitions = allowed_transitions or []

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result = super().to_dict()
        result.update(
            {
                "entity_type": self.entity_type,
                "current_state": self.current_state,
                "attempted_action": self.attempted_action,
                "allowed_transitions": self.allowed_transitions,
            }
        )
        return result
