"""Time-in-force enumeration."""

from __future__ import annotations

from enum import StrEnum


class TimeInForce(StrEnum):
    """Time-in-force options for orders."""

    GTC = "GTC"  # Good Till Canceled - default for most exchanges
    GTD = "GTD"  # Good Till Date - expires at specified time
    IOC = "IOC"  # Immediate Or Cancel - fill immediately or cancel
    FOK = "FOK"  # Fill Or Kill - must fill completely immediately
    PO = "PO"  # Post Only - only if it would be maker

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_day(self) -> bool:
        """Check if order is day order (cancels at end of day)."""
        return False  # Not applicable to these enums

    @property
    def allows_partial_fill(self) -> bool:
        """Check if order allows partial fills."""
        return self in {TimeInForce.GTC, TimeInForce.GTD, TimeInForce.PO}

    @property
    def requires_full_fill(self) -> bool:
        """Check if order must be fully filled or cancelled."""
        return self in {TimeInForce.FOK}

    @property
    def is_maker_only(self) -> bool:
        """Check if order must be maker only."""
        return self == TimeInForce.PO
