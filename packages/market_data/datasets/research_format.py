"""DATA-005: strict, deterministic research-format candle schema.

The format is a public research-data boundary, not a permissive parser.  It
accepts only canonical runtime types, preserves financial values as ``str``
until explicit :class:`~decimal.Decimal` validation, and exposes an exclusive
``[open_time, close_time)`` candle boundary.  Input failures intentionally use
sanitized, typed errors: no raw payloads or numeric values are retained in an
exception or included in an error message.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import ClassVar

from packages.domain.enums.timeframe import SUPPORTED_INTERVALS, interval_boundary_after
from packages.market_data.adapters.value_types import MarketSymbol
from packages.market_data.datasets.timestamps import normalize_epoch_to_utc

RESEARCH_SCHEMA_VERSION: int = 1
RESEARCH_SOURCE: str = "binance_public_rest"

_SUPPORTED_INTERVALS: frozenset[str] = SUPPORTED_INTERVALS
_DECIMAL_PATTERN = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
_MAX_CANONICAL_DECIMAL_CHARS: int = 4_096
_RECORD_FIELDS: frozenset[str] = frozenset(
    {
        "close",
        "close_time",
        "close_time_ms",
        "high",
        "interval",
        "low",
        "open",
        "open_time",
        "open_time_ms",
        "quote_volume",
        "schema_version",
        "source",
        "symbol",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "trade_count",
        "volume",
    }
)


class ResearchCandleValidationError(ValueError):
    """A sanitized failure of the public ResearchCandle contract."""

    def __init__(self, *, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


class DecimalInvalidError(ValueError):
    """A sanitized decimal input failure with no raw input retention."""

    def __init__(
        self,
        *,
        field: str = "decimal",
        reason: str = "invalid decimal input",
    ) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


def _fixed_point_output_length(value: Decimal) -> int:
    decimal_tuple = value.as_tuple()
    exponent = decimal_tuple.exponent
    if not isinstance(exponent, int):
        raise DecimalInvalidError()

    sign_chars = decimal_tuple.sign
    digit_count = len(decimal_tuple.digits)
    is_zero = all(digit == 0 for digit in decimal_tuple.digits)
    if is_zero and exponent >= 0:
        return sign_chars + 1
    if exponent >= 0:
        return sign_chars + digit_count + exponent
    if digit_count + exponent > 0:
        return sign_chars + digit_count + 1
    return sign_chars + 2 - exponent


def canonical_decimal(value: str) -> str:
    """Return a fixed-point, trailing-zero-free representation of ``value``.

    Scientific notation is accepted on input but never emitted.  Runtime
    coercion is forbidden: non-string values, empty strings, whitespace, NaN,
    Infinity, and non-decimal representations raise :class:`DecimalInvalidError`.
    """
    if not isinstance(value, str) or not _DECIMAL_PATTERN.fullmatch(value):
        raise DecimalInvalidError()

    decimal_value: Decimal | None = None
    decimal_parse_failed = False
    try:
        decimal_value = Decimal(value)
    except (InvalidOperation, ValueError):
        decimal_parse_failed = True
    if decimal_parse_failed or decimal_value is None:
        raise DecimalInvalidError()

    if not decimal_value.is_finite():
        raise DecimalInvalidError()
    if _fixed_point_output_length(decimal_value) > _MAX_CANONICAL_DECIMAL_CHARS:
        raise DecimalInvalidError()

    normalized = format(decimal_value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return "0" if normalized == "-0" else normalized


def _validation_error(field: str, reason: str) -> ResearchCandleValidationError:
    """Create only static, sanitized candle-validation errors."""
    return ResearchCandleValidationError(field=field, reason=reason)


def _require_string(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise _validation_error(field, "must be a string")
    return value


def _require_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _validation_error(field, "must be an integer")
    return value


def _validate_symbol(value: object) -> str:
    symbol = _require_string(value, field="symbol")
    parts = symbol.split("/")
    if len(parts) != 2:
        raise _validation_error("symbol", "must be a canonical BASE/QUOTE symbol")
    canonical: str | None = None
    symbol_validation_failed = False
    try:
        canonical = MarketSymbol(base=parts[0], quote=parts[1]).canonical
    except ValueError:
        symbol_validation_failed = True
    if symbol_validation_failed or canonical is None:
        raise _validation_error("symbol", "must be a canonical BASE/QUOTE symbol")
    if symbol != canonical:
        raise _validation_error("symbol", "must be a canonical BASE/QUOTE symbol")
    return symbol


def _validate_interval(value: object) -> str:
    interval = _require_string(value, field="interval")
    if interval not in _SUPPORTED_INTERVALS:
        raise _validation_error("interval", "unsupported interval")
    return interval


def _validate_source(value: object) -> str:
    source = _require_string(value, field="source")
    if source != RESEARCH_SOURCE:
        raise _validation_error("source", "unsupported source")
    return source


def _validate_schema_version(value: object) -> int:
    schema_version = _require_integer(value, field="schema_version")
    if schema_version != RESEARCH_SCHEMA_VERSION:
        raise _validation_error("schema_version", "unsupported schema version")
    return schema_version


def _epoch_ms_to_iso(value: int, *, field: str) -> str:
    """Convert a validated epoch-ms value to canonical UTC ISO text.

    The timestamp primitive has a broader error surface intended for its own
    public API.  At this research-format boundary it is always translated into
    a sanitized ``ResearchCandleValidationError``.
    """
    normalized: str | None = None
    timestamp_conversion_failed = False
    try:
        normalized = normalize_epoch_to_utc(value, "ms").isoformat()
    except (OverflowError, TypeError, ValueError):
        timestamp_conversion_failed = True
    if timestamp_conversion_failed or normalized is None:
        raise _validation_error(field, "invalid epoch timestamp")
    return normalized


def _canonical_decimal_field(value: object, *, field: str) -> str:
    decimal_text = _require_string(value, field=field)
    canonical: str | None = None
    decimal_validation_failed = False
    try:
        canonical = canonical_decimal(decimal_text)
    except DecimalInvalidError:
        decimal_validation_failed = True
    if decimal_validation_failed or canonical is None:
        raise _validation_error(field, "invalid decimal representation")
    return canonical


@dataclass(frozen=True, kw_only=True)
class ResearchCandle:
    """Canonical public candle for deterministic research datasets.

    All values are exact strings or exact integers.  ``close_time_ms`` is the
    exclusive end of the interval: for fixed intervals it equals
    ``open_time_ms + fixed duration``; for ``"1M"`` it equals the next UTC
    calendar-month boundary and ``open_time_ms`` must be aligned to the first
    day of a UTC month at ``00:00:00``.
    """

    symbol: str
    interval: str
    open_time: str
    open_time_ms: int
    close_time: str
    close_time_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    quote_volume: str
    trade_count: int
    taker_buy_base_volume: str
    taker_buy_quote_volume: str
    source: str
    schema_version: int

    _SUPPORTED_INTERVALS: ClassVar[frozenset[str]] = _SUPPORTED_INTERVALS

    def __post_init__(self) -> None:
        """Validate and canonicalize with no asserts or runtime coercion."""
        _validate_symbol(self.symbol)
        interval = _validate_interval(self.interval)
        _validate_source(self.source)
        _validate_schema_version(self.schema_version)

        open_time = _require_string(self.open_time, field="open_time")
        close_time = _require_string(self.close_time, field="close_time")
        open_time_ms = _require_integer(self.open_time_ms, field="open_time_ms")
        close_time_ms = _require_integer(self.close_time_ms, field="close_time_ms")
        trade_count = _require_integer(self.trade_count, field="trade_count")

        expected_open_iso = _epoch_ms_to_iso(open_time_ms, field="open_time_ms")
        expected_close_iso = _epoch_ms_to_iso(close_time_ms, field="close_time_ms")
        if open_time != expected_open_iso:
            raise _validation_error(
                "open_time/open_time_ms",
                "ISO string and epoch ms must represent the same instant",
            )
        if close_time != expected_close_iso:
            raise _validation_error(
                "close_time/close_time_ms",
                "ISO string and epoch ms must represent the same instant",
            )
        if close_time_ms <= open_time_ms:
            raise _validation_error("close_time_ms", "must be greater than open_time_ms")
        expected_close_ms: int | None = None
        interval_boundary_failed = False
        try:
            expected_close_ms = interval_boundary_after(open_time_ms, interval)
        except ValueError:
            interval_boundary_failed = True
        if interval_boundary_failed or expected_close_ms is None:
            raise _validation_error(
                "open_time_ms", "must align to the first day of a UTC month at 00:00:00"
            )
        if close_time_ms != expected_close_ms:
            raise _validation_error(
                "close_time_ms",
                "must equal the exclusive end of the interval",
            )
        if trade_count < 0:
            raise _validation_error("trade_count", "must be a non-negative integer")

        canon_open = _canonical_decimal_field(self.open, field="open")
        canon_high = _canonical_decimal_field(self.high, field="high")
        canon_low = _canonical_decimal_field(self.low, field="low")
        canon_close = _canonical_decimal_field(self.close, field="close")
        canon_volume = _canonical_decimal_field(self.volume, field="volume")
        canon_quote_volume = _canonical_decimal_field(self.quote_volume, field="quote_volume")
        canon_taker_buy_base = _canonical_decimal_field(
            self.taker_buy_base_volume,
            field="taker_buy_base_volume",
        )
        canon_taker_buy_quote = _canonical_decimal_field(
            self.taker_buy_quote_volume,
            field="taker_buy_quote_volume",
        )

        decimal_open = Decimal(canon_open)
        decimal_high = Decimal(canon_high)
        decimal_low = Decimal(canon_low)
        decimal_close = Decimal(canon_close)
        if decimal_high < decimal_low:
            raise _validation_error("OHLC", "high must be greater than or equal to low")
        if decimal_high < decimal_open:
            raise _validation_error("OHLC", "high must be greater than or equal to open")
        if decimal_high < decimal_close:
            raise _validation_error("OHLC", "high must be greater than or equal to close")
        if decimal_low > decimal_open:
            raise _validation_error("OHLC", "low must be less than or equal to open")
        if decimal_low > decimal_close:
            raise _validation_error("OHLC", "low must be less than or equal to close")
        if Decimal(canon_volume) < 0:
            raise _validation_error("volume", "must be non-negative")
        if Decimal(canon_quote_volume) < 0:
            raise _validation_error("quote_volume", "must be non-negative")
        if Decimal(canon_taker_buy_base) < 0:
            raise _validation_error("taker_buy_base_volume", "must be non-negative")
        if Decimal(canon_taker_buy_quote) < 0:
            raise _validation_error("taker_buy_quote_volume", "must be non-negative")

        object.__setattr__(self, "open", canon_open)
        object.__setattr__(self, "high", canon_high)
        object.__setattr__(self, "low", canon_low)
        object.__setattr__(self, "close", canon_close)
        object.__setattr__(self, "volume", canon_volume)
        object.__setattr__(self, "quote_volume", canon_quote_volume)
        object.__setattr__(self, "taker_buy_base_volume", canon_taker_buy_base)
        object.__setattr__(self, "taker_buy_quote_volume", canon_taker_buy_quote)

    def to_record(self) -> dict[str, object]:
        """Return an alphabetically ordered canonical record with no floats."""
        values: dict[str, object] = {
            "symbol": self.symbol,
            "interval": self.interval,
            "open_time": self.open_time,
            "open_time_ms": self.open_time_ms,
            "close_time": self.close_time,
            "close_time_ms": self.close_time_ms,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "quote_volume": self.quote_volume,
            "trade_count": self.trade_count,
            "taker_buy_base_volume": self.taker_buy_base_volume,
            "taker_buy_quote_volume": self.taker_buy_quote_volume,
            "source": self.source,
            "schema_version": self.schema_version,
        }
        return {key: values[key] for key in sorted(values)}

    def to_json(self) -> str:
        """Serialize compactly and deterministically without float conversion."""
        return json.dumps(self.to_record(), separators=(",", ":"), sort_keys=True)

    def to_ndjson_line(self) -> str:
        """Serialize as exactly one compact NDJSON line."""
        return f"{self.to_json()}\n"

    @classmethod
    def from_record(cls, record: object) -> ResearchCandle:
        """Restore a candle only from the exact canonical record shape."""
        if not isinstance(record, dict):
            raise _validation_error("record", "must be an object")

        record_fields = set(record)
        if _RECORD_FIELDS - record_fields:
            raise _validation_error("record", "missing required fields")
        if record_fields - _RECORD_FIELDS:
            raise _validation_error("record", "unknown fields are not allowed")

        return cls(
            symbol=_require_string(record["symbol"], field="symbol"),
            interval=_require_string(record["interval"], field="interval"),
            open_time=_require_string(record["open_time"], field="open_time"),
            open_time_ms=_require_integer(record["open_time_ms"], field="open_time_ms"),
            close_time=_require_string(record["close_time"], field="close_time"),
            close_time_ms=_require_integer(record["close_time_ms"], field="close_time_ms"),
            open=_require_string(record["open"], field="open"),
            high=_require_string(record["high"], field="high"),
            low=_require_string(record["low"], field="low"),
            close=_require_string(record["close"], field="close"),
            volume=_require_string(record["volume"], field="volume"),
            quote_volume=_require_string(record["quote_volume"], field="quote_volume"),
            trade_count=_require_integer(record["trade_count"], field="trade_count"),
            taker_buy_base_volume=_require_string(
                record["taker_buy_base_volume"],
                field="taker_buy_base_volume",
            ),
            taker_buy_quote_volume=_require_string(
                record["taker_buy_quote_volume"],
                field="taker_buy_quote_volume",
            ),
            source=_require_string(record["source"], field="source"),
            schema_version=_require_integer(record["schema_version"], field="schema_version"),
        )

    @classmethod
    def from_json(cls, text: object) -> ResearchCandle:
        """Restore a candle from JSON without exposing parser implementation errors."""
        if not isinstance(text, str):
            raise _validation_error("json", "must be a string")
        record: object = None
        json_parse_failed = False
        try:
            record = json.loads(text)
        except (TypeError, ValueError):
            json_parse_failed = True
        if json_parse_failed:
            raise _validation_error("json", "invalid JSON object")
        return cls.from_record(record)


def research_candle_from_binance_kline(
    raw_payload: list[object],
    symbol: str,
    interval: str,
) -> ResearchCandle:
    """Build a canonical candle from exactly one public Binance 12-field kline.

    Binance's raw ``close_time`` (field 6) is inclusive.  It is validated as a
    real integer and converted to the canonical exclusive boundary as
    ``raw_close_time_ms + 1``, which is then cross-checked against the expected
    interval boundary (fixed duration, or the next UTC calendar month for
    ``"1M"``).  Malformed boundaries are rejected, never silently corrected.
    """
    if not isinstance(raw_payload, list) or len(raw_payload) != 12:
        raise _validation_error("raw_payload", "must be a 12-field Binance kline list")

    canonical_symbol = _validate_symbol(symbol)
    canonical_interval = _validate_interval(interval)
    open_time_ms = _require_integer(raw_payload[0], field="raw_payload")
    raw_close_time_ms = _require_integer(raw_payload[6], field="raw_payload")
    trade_count = _require_integer(raw_payload[8], field="raw_payload")
    _require_string(raw_payload[11], field="raw_payload")

    open_time = _epoch_ms_to_iso(open_time_ms, field="raw_payload")
    _epoch_ms_to_iso(raw_close_time_ms, field="raw_payload")
    expected_close_ms: int | None = None
    interval_boundary_failed = False
    try:
        expected_close_ms = interval_boundary_after(open_time_ms, canonical_interval)
    except ValueError:
        interval_boundary_failed = True
    if interval_boundary_failed or expected_close_ms is None:
        raise _validation_error(
            "raw_payload", "open time must align to the first day of a UTC month at 00:00:00"
        )
    close_time_ms = raw_close_time_ms + 1
    if close_time_ms != expected_close_ms:
        raise _validation_error(
            "raw_payload", "raw inclusive close time does not match the interval boundary"
        )
    close_time = _epoch_ms_to_iso(close_time_ms, field="raw_payload")

    return ResearchCandle(
        symbol=canonical_symbol,
        interval=canonical_interval,
        open_time=open_time,
        open_time_ms=open_time_ms,
        close_time=close_time,
        close_time_ms=close_time_ms,
        open=_require_string(raw_payload[1], field="raw_payload"),
        high=_require_string(raw_payload[2], field="raw_payload"),
        low=_require_string(raw_payload[3], field="raw_payload"),
        close=_require_string(raw_payload[4], field="raw_payload"),
        volume=_require_string(raw_payload[5], field="raw_payload"),
        quote_volume=_require_string(raw_payload[7], field="raw_payload"),
        trade_count=trade_count,
        taker_buy_base_volume=_require_string(raw_payload[9], field="raw_payload"),
        taker_buy_quote_volume=_require_string(raw_payload[10], field="raw_payload"),
        source=RESEARCH_SOURCE,
        schema_version=RESEARCH_SCHEMA_VERSION,
    )
