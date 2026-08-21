"""Position-specific errors."""

from typing import Any

from packages.domain.errors.base import DomainError, EntityNotFoundError, ValidationError


class PositionError(DomainError):
    """Base class for position-related errors."""

    def __init__(
        self,
        message: str,
        symbol: str | None = None,
        position_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize position error."""
        super().__init__(message, details)
        self.symbol = symbol
        self.position_id = position_id


class PositionNotFoundError(PositionError, EntityNotFoundError):
    """Error raised when a position cannot be found."""

    def __init__(self, symbol: str | None = None, position_id: str | None = None) -> None:
        """Initialize position not found error."""
        if position_id:
            message = f"Position with ID '{position_id}' not found"
        elif symbol:
            message = f"Position for symbol '{symbol}' not found"
        else:
            message = "Position not found"
        PositionError.__init__(self, message, symbol, position_id)
        EntityNotFoundError.__init__(self, "Position", position_id or symbol or "unknown")


class PositionAlreadyExistsError(PositionError, ValidationError):
    """Error raised when trying to open a position that already exists."""

    def __init__(self, symbol: str, side: str) -> None:
        """Initialize position already exists error."""
        message = f"Position for {symbol} already exists (side: {side})"
        details = {"symbol": symbol, "side": side}
        PositionError.__init__(self, message, symbol, details=details)


class PositionSizeExceededError(PositionError):
    """Error raised when position size would exceed limits."""

    def __init__(self, symbol: str, requested_size: str, max_size: str, reason: str) -> None:
        """Initialize position size exceeded error."""
        message = f"Position size for {symbol} exceeds limit: requested {requested_size}, max {max_size}. Reason: {reason}"
        details = {
            "requested_size": requested_size,
            "max_size": max_size,
            "reason": reason,
        }
        PositionError.__init__(self, message, symbol, details=details)


class InvalidPositionSideError(PositionError, ValidationError):
    """Error raised when position side is invalid."""

    def __init__(self, symbol: str, side: str, reason: str) -> None:
        """Initialize invalid position side error."""
        message = f"Invalid position side '{side}' for {symbol}: {reason}"
        details = {"side": side, "reason": reason}
        PositionError.__init__(self, message, symbol, details=details)
