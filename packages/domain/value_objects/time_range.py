"""TimeRange value object - Represents a time interval."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass(frozen=True)
class TimeRange:
    """Represents a time interval with start and end.

    Used for:
    - Querying historical data
    - Backtest periods
    - Trading sessions
    """

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        """Validate time range."""
        if self.end <= self.start:
            raise ValueError(f"End ({self.end}) must be after start ({self.start})")

    def __str__(self) -> str:
        """String representation."""
        return f"{self.start.isoformat()} to {self.end.isoformat()}"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"TimeRange({self.start!r}, {self.end!r})"

    def __eq__(self, other: object) -> bool:
        """Compare by start and end."""
        if not isinstance(other, TimeRange):
            return NotImplemented
        return self.start == other.start and self.end == other.end

    def __hash__(self) -> int:
        """Hash based on start and end."""
        return hash((self.start, self.end))

    def __contains__(self, item: datetime | TimeRange) -> bool:
        """Check if datetime or TimeRange is within this range."""
        if isinstance(item, datetime):
            return self.start <= item <= self.end
        return self.start <= item.start and item.end <= self.end

    @property
    def duration(self) -> timedelta:
        """Get the duration of the time range."""
        return self.end - self.start

    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        return self.duration.total_seconds()

    @property
    def duration_hours(self) -> float:
        """Get duration in hours."""
        return self.duration.total_seconds() / 3600

    @property
    def duration_days(self) -> float:
        """Get duration in days."""
        return self.duration.total_seconds() / 86400

    def overlaps(self, other: TimeRange) -> bool:
        """Check if this range overlaps with another."""
        return self.start < other.end and other.start < self.end

    def intersection(self, other: TimeRange) -> TimeRange | None:
        """Get intersection with another range."""
        if not self.overlaps(other):
            return None
        return TimeRange(max(self.start, other.start), min(self.end, other.end))

    def is_adjacent_to(self, other: TimeRange) -> bool:
        """Check if ranges are adjacent (share boundary or gap is negligible)."""
        return self.end == other.start or other.end == self.start

    def split(self, size: timedelta) -> list[TimeRange]:
        """Split into smaller ranges of given size."""
        ranges = []
        current = self.start
        while current < self.end:
            next_time = min(current + size, self.end)
            ranges.append(TimeRange(current, next_time))
            current = next_time
        return ranges

    @classmethod
    def from_days(cls, days: int, end: datetime | None = None) -> TimeRange:
        """Create range for last N days."""
        end_dt = end or datetime.utcnow()
        start_dt = end_dt - timedelta(days=days)
        return cls(start_dt, end_dt)

    @classmethod
    def from_hours(cls, hours: int, end: datetime | None = None) -> TimeRange:
        """Create range for last N hours."""
        end_dt = end or datetime.utcnow()
        start_dt = end_dt - timedelta(hours=hours)
        return cls(start_dt, end_dt)

    @classmethod
    def current_day(cls, tz: str | None = None) -> TimeRange:
        """Get current trading day range."""
        now = datetime.utcnow()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return cls(start, now)
