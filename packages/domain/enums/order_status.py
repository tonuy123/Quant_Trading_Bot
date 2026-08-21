"""Order-status enumeration."""

from __future__ import annotations

from enum import StrEnum


class OrderStatus(StrEnum):
    """Status of an order throughout its lifecycle."""

    # Initial states
    PENDING = "PENDING"  # Order created, not yet submitted
    SUBMITTED = "SUBMITTED"  # Sent to exchange
    ACCEPTED = "ACCEPTED"  # Exchange acknowledged

    # Fill states
    PARTIALLY_FILLED = "PARTIALLY_FILLED"  # Some quantity filled
    FILLED = "FILLED"  # Completely filled

    # Terminal states
    CANCELLED = "CANCELLED"  # User/system cancelled
    REJECTED = "REJECTED"  # Exchange rejected
    EXPIRED = "EXPIRED"  # Time-in-force expired

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_terminal(self) -> bool:
        """Check if this is a terminal (final) state."""
        return self in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }

    @property
    def is_active(self) -> bool:
        """Check if order is still active."""
        return not self.is_terminal

    @property
    def is_working(self) -> bool:
        """Check if order is a working order (limit, stop, etc.)."""
        return self in {
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
        }

    @property
    def is_filled(self) -> bool:
        """Check if order is filled."""
        return self == OrderStatus.FILLED

    @property
    def can_cancel(self) -> bool:
        """Check if order can be cancelled."""
        return self in {
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
        }
