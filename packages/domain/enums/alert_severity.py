"""Alert-severity enumeration."""

from __future__ import annotations

from enum import StrEnum


class AlertSeverity(StrEnum):
    """Severity level for alerts."""

    DEBUG = "DEBUG"  # Debug information
    INFO = "INFO"  # Informational message
    WARNING = "WARNING"  # Warning condition
    ERROR = "ERROR"  # Error condition
    CRITICAL = "CRITICAL"  # Critical condition requiring immediate action

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def level(self) -> int:
        """Get numeric level for comparison."""
        levels = {
            AlertSeverity.DEBUG: 0,
            AlertSeverity.INFO: 1,
            AlertSeverity.WARNING: 2,
            AlertSeverity.ERROR: 3,
            AlertSeverity.CRITICAL: 4,
        }
        return levels[self]

    @property
    def should_notify(self) -> bool:
        """Check if this severity requires notification."""
        return self in {AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL}

    @property
    def requires_action(self) -> bool:
        """Check if this severity requires immediate action."""
        return self in {AlertSeverity.ERROR, AlertSeverity.CRITICAL}

    def __lt__(self, other: str) -> bool:
        """Compare severity levels."""
        if isinstance(other, AlertSeverity):
            return self.level < other.level
        return self.value < other

    def __le__(self, other: str) -> bool:
        """Compare severity levels."""
        if isinstance(other, AlertSeverity):
            return self.level <= other.level
        return self.value <= other

    def __gt__(self, other: str) -> bool:
        """Compare severity levels."""
        if isinstance(other, AlertSeverity):
            return self.level > other.level
        return self.value > other

    def __ge__(self, other: str) -> bool:
        """Compare severity levels."""
        if isinstance(other, AlertSeverity):
            return self.level >= other.level
        return self.value >= other
