"""Signal-type enumeration."""

from __future__ import annotations

from enum import StrEnum


class SignalType(StrEnum):
    """Type of trading signal."""

    # Entry signals
    ENTRY_LONG = "ENTRY_LONG"  # Signal to open long position
    ENTRY_SHORT = "ENTRY_SHORT"  # Signal to open short position

    # Exit signals
    EXIT_LONG = "EXIT_LONG"  # Signal to close long position
    EXIT_SHORT = "EXIT_SHORT"  # Signal to close short position

    # Neutral
    NEUTRAL = "NEUTRAL"  # No signal
    CLOSE_ALL = "CLOSE_ALL"  # Close all positions

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def is_entry(self) -> bool:
        """Check if this is an entry signal."""
        return self in {SignalType.ENTRY_LONG, SignalType.ENTRY_SHORT}

    @property
    def is_exit(self) -> bool:
        """Check if this is an exit signal."""
        return self in {SignalType.EXIT_LONG, SignalType.EXIT_SHORT}

    @property
    def is_long(self) -> bool:
        """Check if this is a long signal."""
        return self in {SignalType.ENTRY_LONG, SignalType.EXIT_SHORT}

    @property
    def is_short(self) -> bool:
        """Check if this is a short signal."""
        return self in {SignalType.ENTRY_SHORT, SignalType.EXIT_LONG}

    @property
    def direction(self) -> int:
        """Get numeric direction: 1 for long, -1 for short, 0 for neutral."""
        if self in {SignalType.ENTRY_LONG, SignalType.EXIT_SHORT}:
            return 1
        elif self in {SignalType.ENTRY_SHORT, SignalType.EXIT_LONG}:
            return -1
        return 0

    @classmethod
    def from_direction(cls, direction: int) -> SignalType:
        """Create signal from direction."""
        if direction > 0:
            return cls.ENTRY_LONG
        elif direction < 0:
            return cls.ENTRY_SHORT
        return cls.NEUTRAL
