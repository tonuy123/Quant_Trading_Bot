"""Clock interface - Provides time-related functionality."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class Clock(ABC):
    """Abstract clock for time operations.

    This allows domain code to be decoupled from system time,
    making testing and backtesting easier.
    """

    @abstractmethod
    def now(self) -> datetime:
        """Get current datetime.

        Returns:
            Current datetime in UTC.
        """
        ...

    @abstractmethod
    def utcnow(self) -> datetime:
        """Get current UTC datetime.

        Returns:
            Current UTC datetime.
        """
        ...

    @abstractmethod
    def timestamp(self) -> float:
        """Get current Unix timestamp.

        Returns:
            Current Unix timestamp in seconds.
        """
        ...

    @abstractmethod
    def sleep(self, seconds: float) -> None:
        """Sleep for specified seconds.

        Args:
            seconds: Number of seconds to sleep.
        """
        ...


class SystemClock(Clock):
    """System clock implementation using actual system time."""

    def now(self) -> datetime:
        """Get current datetime in UTC."""
        return datetime.now(UTC)

    def utcnow(self) -> datetime:
        """Get current UTC datetime."""
        return datetime.utcnow()

    def timestamp(self) -> float:
        """Get current Unix timestamp."""
        return datetime.now(UTC).timestamp()

    def sleep(self, seconds: float) -> None:
        """Sleep using system time."""
        import time

        time.sleep(seconds)


class TimeProvider(ABC):
    """Abstract time provider for scheduling and time-based operations.

    Unlike Clock which provides "current" time, TimeProvider is used
    for scheduling future events and time-based calculations.
    """

    @abstractmethod
    def get_next_candle_time(self, timeframe: str, from_time: datetime) -> datetime:
        """Get the next candle close time for a timeframe.

        Args:
            timeframe: Timeframe string (e.g., "1m", "1h", "1d")
            from_time: Current or reference time

        Returns:
            Next candle close time.
        """
        ...

    @abstractmethod
    def get_previous_candle_time(self, timeframe: str, from_time: datetime) -> datetime:
        """Get the previous candle close time for a timeframe.

        Args:
            timeframe: Timeframe string
            from_time: Current or reference time

        Returns:
            Previous candle close time.
        """
        ...

    @abstractmethod
    def timeframe_seconds(self, timeframe: str) -> int:
        """Get number of seconds in a timeframe.

        Args:
            timeframe: Timeframe string

        Returns:
            Number of seconds.
        """
        ...

    @abstractmethod
    def is_market_open(self, exchange: str) -> bool:
        """Check if market is currently open for trading.

        Args:
            exchange: Exchange name

        Returns:
            True if market is open.
        """
        ...

    @abstractmethod
    def get_market_open_time(self, exchange: str, date: datetime) -> datetime:
        """Get market open time for a specific date.

        Args:
            exchange: Exchange name
            date: Date to check

        Returns:
            Market open time.
        """
        ...

    @abstractmethod
    def get_market_close_time(self, exchange: str, date: datetime) -> datetime:
        """Get market close time for a specific date.

        Args:
            exchange: Exchange name
            date: Date to check

        Returns:
            Market close time.
        """
        ...
