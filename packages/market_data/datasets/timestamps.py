"""DATA-004: strict timestamp unit and UTC normalization primitive.

Design decisions (documented, do not silently change):

Supported units
---------------
Only explicit units: ``"s"`` (seconds), ``"ms"`` (milliseconds), ``"us"``
(microseconds), ``"ns"`` (nanoseconds).  Units are exact strings -- no
whitespace stripping, no case folding, and never inferred from the number
magnitude.  ``1690000000000`` without a unit is rejected; callers must pass
``unit="ms"``.  A magnitude-based heuristic like ``value > 10**12`` (used by
the legacy ``data_normalizer`` service) is deliberately NOT reproduced here.

Canonical output
----------------
* UTC-aware ``datetime`` for human/domain usage -- ``tzinfo`` is
  ``datetime.UTC``, ``utcoffset()`` is zero, never naive.
* integer UTC epoch milliseconds for dataset serialization, produced by
  :func:`utc_to_epoch_ms` without float arithmetic.

Precision policy
----------------
Epoch values must be real ``int``.  ``bool`` (an ``int`` subclass), ``float``,
``Decimal``, ``str``, ``None``, and NaN/Infinity are rejected -- float epoch
arithmetic silently loses precision and is forbidden.  Nanoseconds are
converted to microseconds only when the value divides exactly; a
sub-microsecond residue raises ``TimestampNormalizationError`` instead of
silently truncating.

Timezone policy
---------------
Aware datetimes are normalized with ``astimezone(UTC)`` -- the instant is
preserved, not the wall-clock text.  Naive datetimes are rejected: there is no
safe default timezone.

Negative and overflow policy
----------------------------
Negative epoch values are rejected (pre-1970 data is unsupported; the
DATA-002 downloader already enforces the same rule).  Values that would
exceed the Python ``datetime`` range raise ``TimestampNormalizationError``
instead of a raw overflow.

Range semantics
---------------
:class:`NormalizedTimestampRange` is a half-open ``[start, end)`` range:
``start`` is inclusive, ``end`` is exclusive.  ``end < start`` is rejected;
``start == end`` is a valid empty range.  DATA-001 metadata coverage is a
separate contract and still requires ``coverage_end > coverage_start``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

TimestampUnit = Literal["s", "ms", "us", "ns"]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_UNIT_NS: dict[str, int] = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


class TimestampNormalizationError(ValueError):
    """Raised for any input that cannot be normalized without guessing."""


@dataclass(frozen=True, kw_only=True)
class NormalizedTimestampRange:
    """Immutable half-open ``[start, end)`` range of UTC-aware datetimes.

    Direct construction validates like :func:`normalize_datetime_to_utc`:
    both endpoints must be real aware datetimes (naive and non-datetime values
    are rejected) and offset-aware values are canonicalized to UTC, preserving
    the instant.  ``end < start`` raises :class:`TimestampNormalizationError`;
    ``start == end`` is a valid empty range.  Immutability is preserved after
    construction.
    """

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        start = normalize_datetime_to_utc(self.start)
        end = normalize_datetime_to_utc(self.end)
        if end < start:
            raise TimestampNormalizationError("range end must not precede range start")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)


def _require_unit(unit: object) -> TimestampUnit:
    if not isinstance(unit, str) or unit not in _UNIT_NS:
        raise TimestampNormalizationError(
            f"unit must be one of {sorted(_UNIT_NS)} (got {type(unit).__name__})"
        )
    return unit  # type: ignore[return-value]


def _require_epoch_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TimestampNormalizationError(
            f"epoch value must be an integer (got {type(value).__name__})"
        )
    if value < 0:
        raise TimestampNormalizationError("epoch value must be non-negative")
    return value


def normalize_epoch_to_utc(value: int, unit: TimestampUnit) -> datetime:
    """Convert an integer epoch value in an explicit unit to UTC-aware datetime."""
    unit = _require_unit(unit)
    epoch = _require_epoch_int(value)
    total_ns = epoch * _UNIT_NS[unit]
    if total_ns % 1000 != 0:
        raise TimestampNormalizationError(
            "nanosecond value must be an exact multiple of 1000 (microsecond resolution)"
        )
    try:
        return _EPOCH + timedelta(microseconds=total_ns // 1000)
    except (OverflowError, ValueError) as error:
        raise TimestampNormalizationError("epoch value out of supported datetime range") from error


def normalize_datetime_to_utc(value: datetime) -> datetime:
    """Normalize an aware datetime to UTC, preserving the instant."""
    if not isinstance(value, datetime):
        raise TimestampNormalizationError(f"expected a datetime (got {type(value).__name__})")
    if value.tzinfo is None or value.utcoffset() is None:
        raise TimestampNormalizationError("naive datetime is ambiguous; provide an aware datetime")
    return value.astimezone(UTC)


def normalize_timestamp_to_utc(
    value: int | datetime, unit: TimestampUnit | None = None
) -> datetime:
    """Normalize an integer epoch (with an explicit unit) or an aware datetime.

    The unit is mandatory for integer input -- it is never inferred from the
    magnitude.  Supplying a unit together with a datetime is a caller mistake
    and is rejected rather than silently ignored.
    """
    if isinstance(value, datetime):
        if unit is not None:
            raise TimestampNormalizationError("unit must be None when value is a datetime")
        return normalize_datetime_to_utc(value)
    if unit is None:
        raise TimestampNormalizationError(
            "unit is required for integer epoch values; it is never inferred from magnitude"
        )
    return normalize_epoch_to_utc(value, unit)


def utc_to_epoch_ms(value: datetime) -> int:
    """Return the deterministic integer UTC epoch milliseconds of a datetime.

    The datetime must be aware; the instant is normalized to UTC first.  The
    conversion is delta-based integer arithmetic (no ``timestamp() * 1000``
    float rounding) and sub-millisecond residue is truncated toward zero,
    never rounded.  Pre-1970 instants are rejected.
    """
    utc = normalize_datetime_to_utc(value)
    delta = utc - _EPOCH
    if delta < timedelta(0):
        raise TimestampNormalizationError("epoch milliseconds must be non-negative")
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def normalize_range_to_utc(
    start: int | datetime,
    end: int | datetime,
    unit: TimestampUnit | None = None,
) -> NormalizedTimestampRange:
    """Normalize a half-open ``[start, end)`` range to UTC.

    ``end < start`` is rejected; ``start == end`` is a valid empty range.
    """
    start_utc = normalize_timestamp_to_utc(start, unit)
    end_utc = normalize_timestamp_to_utc(end, unit)
    if end_utc < start_utc:
        raise TimestampNormalizationError("range end must not precede range start")
    return NormalizedTimestampRange(start=start_utc, end=end_utc)
