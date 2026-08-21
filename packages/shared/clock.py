"""Clock utilities - Time operations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class SystemClock:
    """System clock implementation using actual system time.

    This is the default clock used in production.
    For testing, replace with a mock clock.
    """

    @staticmethod
    def now() -> datetime:
        """Get current UTC datetime."""
        return datetime.now(UTC)

    @staticmethod
    def utcnow() -> datetime:
        """Get current naive UTC datetime."""
        return datetime.utcnow()

    @staticmethod
    def timestamp() -> float:
        """Get current Unix timestamp."""
        return datetime.now(UTC).timestamp()

    @staticmethod
    def from_timestamp(timestamp: float) -> datetime:
        """Create datetime from Unix timestamp."""
        return datetime.fromtimestamp(timestamp, tz=UTC)

    @staticmethod
    def sleep(seconds: float) -> None:
        """Sleep for specified seconds."""
        import time

        time.sleep(seconds)

    @staticmethod
    async def async_sleep(seconds: float) -> None:
        """Async sleep for specified seconds."""
        import asyncio

        await asyncio.sleep(seconds)


class Clock:
    """Clock interface for time operations.

    Allows domain code to be decoupled from system time,
    making testing and backtesting easier.
    """

    def __init__(self, clock: type[SystemClock] = SystemClock) -> None:
        """Initialize with clock implementation."""
        self._clock = clock

    def now(self) -> datetime:
        """Get current datetime."""
        return self._clock.now()

    def utcnow(self) -> datetime:
        """Get current UTC datetime."""
        return self._clock.utcnow()

    def timestamp(self) -> float:
        """Get current Unix timestamp."""
        return self._clock.timestamp()

    def from_timestamp(self, timestamp: float) -> datetime:
        """Create datetime from timestamp."""
        return self._clock.from_timestamp(timestamp)

    def sleep(self, seconds: float) -> None:
        """Sleep for specified seconds."""
        self._clock.sleep(seconds)

    async def async_sleep(self, seconds: float) -> None:
        """Async sleep for specified seconds."""
        await self._clock.async_sleep(seconds)
