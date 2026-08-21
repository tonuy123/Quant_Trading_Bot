"""Timeframe enumeration for candlestick data.

Interval semantics (DATA-TIME-001):

* ``"1m"`` is one minute.  ``"1M"`` is one calendar month.  They are never
  case-folded into the same value: ``Timeframe.from_string("1M")`` returns
  ``MO1`` and ``Timeframe.from_string("1m")`` returns ``M1``.
* A calendar month has no fixed duration.  ``Timeframe.seconds`` raises
  ``ValueError`` for ``MO1``; callers that need a boundary must use
  :func:`interval_boundary_after`, which returns the exclusive end of the
  interval in epoch milliseconds (UTC).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum

MONTHLY_INTERVAL = "1M"

_FIXED_DURATION_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}

SUPPORTED_INTERVALS: frozenset[str] = frozenset(_FIXED_DURATION_MS) | {MONTHLY_INTERVAL}

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def interval_boundary_after(open_ms: int, interval: str) -> int:
    """Return the exclusive UTC epoch-ms end of the interval starting at ``open_ms``.

    Fixed intervals add their fixed duration.  For ``"1M"`` the boundary is the
    next UTC calendar-month start, and ``open_ms`` must be aligned to the first
    day of a month at ``00:00:00`` UTC.  ``open_ms`` must be a real non-negative
    integer; malformed input raises ``ValueError`` with a sanitized message.
    """
    if isinstance(open_ms, bool) or not isinstance(open_ms, int) or open_ms < 0:
        raise ValueError("open_ms must be a non-negative integer epoch millisecond value")
    if not isinstance(interval, str) or interval not in SUPPORTED_INTERVALS:
        raise ValueError(f"unsupported interval {interval!r}")
    if interval in _FIXED_DURATION_MS:
        return open_ms + _FIXED_DURATION_MS[interval]
    try:
        open_dt = _EPOCH + timedelta(milliseconds=open_ms)
    except OverflowError:
        raise ValueError("open_ms out of supported datetime range") from None
    if not (
        open_dt.day == 1
        and open_dt.hour == 0
        and open_dt.minute == 0
        and open_dt.second == 0
        and open_dt.microsecond == 0
    ):
        raise ValueError("monthly open_ms must align to the first day of a UTC month at 00:00:00")
    try:
        if open_dt.month == 12:
            next_open = open_dt.replace(year=open_dt.year + 1, month=1, day=1)
        else:
            next_open = open_dt.replace(month=open_dt.month + 1, day=1)
    except (OverflowError, ValueError):
        raise ValueError("open_ms out of supported datetime range") from None
    delta = next_open - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


class Timeframe(StrEnum):
    """Standard timeframes for candlestick data."""

    # Minutes
    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"

    # Hours
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H8 = "8h"
    H12 = "12h"

    # Days
    D1 = "1d"
    D3 = "3d"

    # Weeks
    W1 = "1w"

    # Months
    MO1 = "1M"

    def __str__(self) -> str:
        """String representation."""
        return self.value

    @property
    def seconds(self) -> int:
        """Get seconds in this timeframe.

        ``MO1`` (a calendar month) has no fixed duration and raises
        ``ValueError``; use :func:`interval_boundary_after` instead.
        """
        if self is Timeframe.MO1:
            raise ValueError(
                "calendar month (1M) has no fixed duration; use interval_boundary_after"
            )
        return _FIXED_DURATION_MS[self.value] // 1000

    @property
    def minutes(self) -> int:
        """Get minutes in this timeframe."""
        return self.seconds // 60

    @property
    def hours(self) -> int:
        """Get hours in this timeframe."""
        return self.seconds // 3600

    @property
    def is_intraday(self) -> bool:
        """Check if this is an intraday timeframe."""
        return self in {
            Timeframe.M1,
            Timeframe.M3,
            Timeframe.M5,
            Timeframe.M15,
            Timeframe.M30,
            Timeframe.H1,
            Timeframe.H2,
            Timeframe.H4,
            Timeframe.H6,
            Timeframe.H8,
            Timeframe.H12,
        }

    @property
    def is_daily(self) -> bool:
        """Check if this is a daily timeframe."""
        return self in {Timeframe.D1, Timeframe.D3}

    @classmethod
    def from_string(cls, value: str) -> Timeframe:
        """Parse from string without case-folding monthly and minute together.

        ``"1M"`` resolves to ``MO1`` before any lowercasing; only values that
        are not an exact match are retried case-insensitively, so ``"1H"``
        still resolves to ``H1`` while ``"1M"`` and ``"1m"`` stay distinct.
        """
        if not isinstance(value, str):
            raise ValueError(f"Unknown timeframe: {value!r}")
        mapping = {
            "1m": cls.M1,
            "3m": cls.M3,
            "5m": cls.M5,
            "15m": cls.M15,
            "30m": cls.M30,
            "1h": cls.H1,
            "2h": cls.H2,
            "4h": cls.H4,
            "6h": cls.H6,
            "8h": cls.H8,
            "12h": cls.H12,
            "1d": cls.D1,
            "3d": cls.D3,
            "1w": cls.W1,
            "1M": cls.MO1,
        }
        if result := mapping.get(value):
            return result
        if result := mapping.get(value.lower()):
            return result
        raise ValueError(f"Unknown timeframe: {value}")
