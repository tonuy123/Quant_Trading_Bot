"""DATA-005B-2A: pure conversion of one downloader archive line.

This module deliberately has no filesystem, environment, logging, or network
behavior.  It converts one exact ``bytes`` line into either one validated
research candle or one typed, sanitized failure.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from types import MappingProxyType
from typing import Literal, Never, cast
from unicodedata import category as unicode_category

from packages.domain.enums.timeframe import SUPPORTED_INTERVALS
from packages.market_data.adapters.value_types import MarketSymbol
from packages.market_data.datasets.research_format import (
    RESEARCH_SOURCE,
    ResearchCandle,
    ResearchCandleValidationError,
    research_candle_from_binance_kline,
)
from packages.market_data.datasets.timestamps import normalize_datetime_to_utc, utc_to_epoch_ms

ConversionFailureType = Literal[
    "line_too_large",
    "invalid_utf8",
    "malformed_json",
    "invalid_envelope",
    "invalid_candle",
    "out_of_range",
    "ordering_violation",
]

_FAILURE_REASONS: Mapping[ConversionFailureType, str] = MappingProxyType(
    {
        "line_too_large": "raw archive line exceeds the configured byte limit",
        "invalid_utf8": "raw archive line is not valid UTF-8",
        "malformed_json": "raw archive line is not valid JSON",
        "invalid_envelope": "raw archive record violates the downloader envelope contract",
        "invalid_candle": "raw payload violates the research candle contract",
        "out_of_range": "candle falls outside the requested half-open range",
        "ordering_violation": "candle open time is earlier than the previous accepted record",
    }
)
_FAILURE_TYPES: frozenset[str] = frozenset(_FAILURE_REASONS)
_ENVELOPE_KEYS: frozenset[str] = frozenset(
    {"symbol", "interval", "open_time", "close_time", "source", "payload"}
)
_SECRET_PATTERNS: tuple[str, ...] = (
    "apikey",
    "api_key",
    "api-key",
    "secret",
    "signature",
    "listenkey",
    "authorization",
    "bearer",
    "private_key",
    "private-key",
)
_UNSAFE_PREVIEW_CATEGORIES: frozenset[str] = frozenset({"Cc", "Cf", "Cs"})
_JSON_UNICODE_ESCAPE = re.compile(r"\\u[0-9A-Fa-f]{4}")
_LOWERCASE_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _safe_basename(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or any(forbidden in value for forbidden in ("/", "\\", "\0", "\r", "\n"))
    ):
        raise ValueError(f"{field} must be a non-empty safe basename")
    return value


def _canonical_symbol(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("symbol must be a canonical BASE/QUOTE string")
    parts = value.split("/")
    if len(parts) != 2:
        raise ValueError("symbol must be a canonical BASE/QUOTE string")
    try:
        canonical = MarketSymbol(base=parts[0], quote=parts[1]).canonical
    except ValueError:
        raise ValueError("symbol must be a canonical BASE/QUOTE string") from None
    if value != canonical:
        raise ValueError("symbol must be a canonical BASE/QUOTE string")
    return value


def _supported_interval(value: object) -> str:
    if not isinstance(value, str) or value not in SUPPORTED_INTERVALS:
        raise ValueError("interval must be an exact supported interval")
    return value


def _normalized_range_endpoint(value: object, *, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field} must be an aware datetime")
    try:
        return normalize_datetime_to_utc(value)
    except ValueError:
        raise ValueError(f"{field} must be an aware datetime") from None


def _positive_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _non_negative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _sanitize_preview(content: object, *, truncate: bool) -> str | None:
    if content is None:
        return None
    if not isinstance(content, str):
        raise ValueError("line_preview must be None or at most 256 characters")

    casefolded = content.casefold()
    if (
        _JSON_UNICODE_ESCAPE.search(content) is not None
        or any(pattern in casefolded for pattern in _SECRET_PATTERNS)
        or any(unicode_category(character) in _UNSAFE_PREVIEW_CATEGORIES for character in content)
    ):
        return None
    if len(content) > 256 and not truncate:
        raise ValueError("line_preview must be None or at most 256 characters")
    return content[:256] if truncate else content


@dataclass(frozen=True, kw_only=True)
class RawConversionContext:
    """Validated immutable identity and half-open range for one raw file."""

    file_name: str
    symbol: str
    interval: str
    range_start: datetime
    range_end: datetime
    max_line_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        file_name = _safe_basename(self.file_name, field="file_name")
        symbol = _canonical_symbol(self.symbol)
        interval = _supported_interval(self.interval)
        range_start = _normalized_range_endpoint(self.range_start, field="range_start")
        range_end = _normalized_range_endpoint(self.range_end, field="range_end")
        max_line_bytes = _positive_integer(self.max_line_bytes, field="max_line_bytes")
        if range_end <= range_start:
            raise ValueError("range_end must be later than range_start")

        object.__setattr__(self, "file_name", file_name)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "interval", interval)
        object.__setattr__(self, "range_start", range_start)
        object.__setattr__(self, "range_end", range_end)
        object.__setattr__(self, "max_line_bytes", max_line_bytes)


@dataclass(frozen=True, kw_only=True)
class ConversionFailure:
    """Typed failure metadata with a fixed public reason vocabulary."""

    file: str
    line_number: int
    failure_type: ConversionFailureType
    line_preview: str | None
    line_sha256: str

    def __post_init__(self) -> None:
        _safe_basename(self.file, field="file")
        _positive_integer(self.line_number, field="line_number")
        if not isinstance(self.failure_type, str) or self.failure_type not in _FAILURE_TYPES:
            raise ValueError("failure_type must be a supported conversion failure type")
        line_preview = _sanitize_preview(self.line_preview, truncate=False)
        object.__setattr__(self, "line_preview", line_preview)
        if (
            not isinstance(self.line_sha256, str)
            or _LOWERCASE_SHA256.fullmatch(self.line_sha256) is None
        ):
            raise ValueError("line_sha256 must be a lowercase SHA-256 hexadecimal digest")

    @property
    def reason(self) -> str:
        """Return the fixed sanitized reason for this failure type."""
        return _FAILURE_REASONS[self.failure_type]


@dataclass(frozen=True, kw_only=True)
class LineConversionResult:
    """Exactly one successful candle or one data-line failure."""

    candle: ResearchCandle | None = None
    failure: ConversionFailure | None = None

    def __post_init__(self) -> None:
        if (self.candle is None) == (self.failure is None):
            raise ValueError("exactly one of candle or failure must be provided")
        if self.candle is not None and not isinstance(self.candle, ResearchCandle):
            raise ValueError("candle must be a ResearchCandle")
        if self.failure is not None and not isinstance(self.failure, ConversionFailure):
            raise ValueError("failure must be a ConversionFailure")


class _DuplicateJsonObjectKey(ValueError):
    """Internal marker for duplicate JSON object keys."""


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    record: dict[str, object] = {}
    for key, value in pairs:
        if key in record:
            raise _DuplicateJsonObjectKey
        record[key] = value
    return record


def _reject_non_json_constant(_constant: str) -> Never:
    raise ValueError


def _without_one_line_ending(decoded: str) -> str:
    if not decoded.endswith("\n"):
        return decoded
    content = decoded[:-1]
    return content[:-1] if content.endswith("\r") else content


def _failure_result(
    *,
    context: RawConversionContext,
    line_number: int,
    failure_type: ConversionFailureType,
    line_preview: str | None,
    line_sha256: str,
) -> LineConversionResult:
    return LineConversionResult(
        failure=ConversionFailure(
            file=context.file_name,
            line_number=line_number,
            failure_type=failure_type,
            line_preview=line_preview,
            line_sha256=line_sha256,
        )
    )


def _context_epoch_range(context: RawConversionContext) -> tuple[int, int]:
    try:
        return utc_to_epoch_ms(context.range_start), utc_to_epoch_ms(context.range_end)
    except ValueError:
        raise ValueError("context range must use supported non-negative UTC timestamps") from None


def convert_raw_archive_line(
    raw_line: bytes,
    *,
    line_number: int,
    context: RawConversionContext,
    previous_open_time_ms: int | None = None,
) -> LineConversionResult:
    """Convert one exact downloader archive line without external side effects."""
    if type(raw_line) is not bytes:
        raise ValueError("raw_line must be exact bytes")
    line_number = _positive_integer(line_number, field="line_number")
    if not isinstance(context, RawConversionContext):
        raise ValueError("context must be a RawConversionContext")
    if previous_open_time_ms is not None:
        previous_open_time_ms = _non_negative_integer(
            previous_open_time_ms,
            field="previous_open_time_ms",
        )

    range_start_ms, range_end_ms = _context_epoch_range(context)
    line_sha256 = sha256(raw_line).hexdigest()

    if len(raw_line) > context.max_line_bytes:
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="line_too_large",
            line_preview=None,
            line_sha256=line_sha256,
        )

    try:
        decoded = raw_line.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="invalid_utf8",
            line_preview=None,
            line_sha256=line_sha256,
        )

    content = _without_one_line_ending(decoded)
    line_preview = _sanitize_preview(content, truncate=True)
    try:
        parsed: object = json.loads(
            content,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_json_constant,
        )
    except _DuplicateJsonObjectKey:
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="invalid_envelope",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )
    except (RecursionError, ValueError):
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="malformed_json",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )

    if not isinstance(parsed, dict) or set(parsed) != _ENVELOPE_KEYS:
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="invalid_envelope",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )

    envelope = cast(dict[str, object], parsed)
    symbol = envelope["symbol"]
    interval = envelope["interval"]
    open_time = envelope["open_time"]
    close_time = envelope["close_time"]
    source = envelope["source"]
    payload = envelope["payload"]
    if not (
        isinstance(symbol, str)
        and isinstance(interval, str)
        and isinstance(open_time, str)
        and isinstance(close_time, str)
        and isinstance(source, str)
        and isinstance(payload, list)
    ):
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="invalid_envelope",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )
    if symbol != context.symbol or interval != context.interval or source != RESEARCH_SOURCE:
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="invalid_envelope",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )

    try:
        candle = research_candle_from_binance_kline(
            raw_payload=cast(list[object], payload),
            symbol=context.symbol,
            interval=context.interval,
        )
    except ResearchCandleValidationError:
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="invalid_candle",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )

    if not (
        symbol == candle.symbol
        and interval == candle.interval
        and source == candle.source
        and open_time == candle.open_time
        and close_time == candle.close_time
    ):
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="invalid_envelope",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )

    if not (
        candle.open_time_ms >= range_start_ms
        and candle.open_time_ms < range_end_ms
        and candle.close_time_ms <= range_end_ms
    ):
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="out_of_range",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )

    if previous_open_time_ms is not None and candle.open_time_ms < previous_open_time_ms:
        return _failure_result(
            context=context,
            line_number=line_number,
            failure_type="ordering_violation",
            line_preview=line_preview,
            line_sha256=line_sha256,
        )

    return LineConversionResult(candle=candle)
