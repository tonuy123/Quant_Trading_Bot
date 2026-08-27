"""DATA-005 B-2B slice 2A: pure research publication-manifest contracts.

This module defines immutable metadata only.  It deliberately performs no
filesystem orchestration, hashing of file contents, staging, resume, or
publication.  ``research_checksum`` covers exact research output bytes
concatenated by sorted ``research_name``; ``failure_checksum`` does the same
for failure sidecars sorted by ``failure_name``.  An incomplete manifest covers
only its completed artifacts, and a zero-byte concatenation uses SHA-256 of
``b""``.  The filesystem publisher owns computing those digests.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Literal, cast

from packages.domain.enums.timeframe import SUPPORTED_INTERVALS
from packages.market_data.adapters.value_types import MarketSymbol
from packages.market_data.datasets.conversion_stream import (
    ConversionStatus,
    StreamConversionReport,
)
from packages.market_data.datasets.downloader import (
    DATASET_DOWNLOAD_VERSION,
    DOWNLOADER_VERSION,
    SOURCE,
    DownloadManifest,
    OutputFileInfo,
)
from packages.market_data.datasets.metadata import DATASET_SCHEMA_VERSION
from packages.market_data.datasets.research_format import (
    RESEARCH_SCHEMA_VERSION,
    RESEARCH_SOURCE,
)

RESEARCH_DATASET_VERSION: str = "1.0.1"
RESEARCH_CONVERTER_VERSION: str = "1.0.0"
RESEARCH_MANIFEST_FILE: str = "research_manifest.json"

PublicationStatus = Literal["incomplete", "complete"]

_LOWERCASE_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_EMPTY_SHA256 = sha256(b"").hexdigest()
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MAX_JSON_INTEGER_DIGITS = 4_300
_DUPLICATE_JSON_OBJECT = object()
_CONVERSION_STATUSES: frozenset[str] = frozenset({"success", "partial"})
_PUBLICATION_STATUSES: frozenset[str] = frozenset({"incomplete", "complete"})
_ARTIFACT_COUNTER_FIELDS: tuple[str, ...] = (
    "raw_bytes",
    "lines_seen",
    "records_written",
    "records_quarantined",
    "research_bytes",
    "failure_bytes",
)
_MANIFEST_COUNTER_FIELDS: tuple[str, ...] = (
    "lines_seen",
    "records_written",
    "records_quarantined",
    "research_bytes",
    "failure_bytes",
)
_ARTIFACT_RECORD_FIELDS: frozenset[str] = frozenset(
    {
        "coverage_end_ms",
        "coverage_start_ms",
        "failure_bytes",
        "failure_name",
        "failure_sha256",
        "interval",
        "lines_seen",
        "raw_bytes",
        "raw_name",
        "raw_sha256",
        "records_quarantined",
        "records_written",
        "research_bytes",
        "research_name",
        "research_sha256",
        "status",
        "symbol",
    }
)
_MANIFEST_RECORD_FIELDS: frozenset[str] = frozenset(
    {
        "completion_status",
        "conversion_status",
        "converter_version",
        "coverage_end",
        "coverage_start",
        "dataset_id",
        "dataset_version",
        "downloader_version",
        "exchange",
        "expected_raw_files",
        "failure_bytes",
        "failure_checksum",
        "files",
        "intervals",
        "lines_seen",
        "market_type",
        "max_line_bytes",
        "raw_dataset_version",
        "raw_manifest_sha256",
        "records_quarantined",
        "records_written",
        "requested_end",
        "requested_start",
        "research_bytes",
        "research_checksum",
        "schema_version",
        "source",
        "symbols",
    }
)


class ResearchManifestValidationError(ValueError):
    """A sanitized research-manifest contract failure."""

    def __init__(self, *, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


def _validation_error(field: str, reason: str) -> ResearchManifestValidationError:
    return ResearchManifestValidationError(field=field, reason=reason)


def _require_exact_bool(value: object, *, field: str) -> bool:
    """Reject int/str/object subclasses for bool fields."""
    if type(value) is not bool:
        raise _validation_error(field, "must be an exact boolean")
    return value


def _require_exact_string(value: object, *, field: str) -> str:
    """Reject str subclasses for direct DownloadManifest string fields."""
    if type(value) is not str:
        raise _validation_error(field, "must be an exact string")
    return value


def _require_exact_non_negative_integer(value: object, *, field: str) -> int:
    """Reject bool, int subclasses, and negative values."""
    if type(value) is not int or isinstance(value, bool):
        raise _validation_error(field, "must be an exact non-negative integer")
    if value < 0:
        raise _validation_error(field, "must be a non-negative integer")
    return value


def _require_string(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise _validation_error(field, "must be a string")
    return value


def _require_non_empty_string(value: object, *, field: str) -> str:
    text = _require_string(value, field=field)
    if not text:
        raise _validation_error(field, "must be non-empty")
    return text


def _require_non_negative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _validation_error(field, "must be a non-negative integer")
    return value


def _require_positive_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _validation_error(field, "must be a positive integer")
    return value


def _require_lowercase_sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _LOWERCASE_SHA256.fullmatch(value) is None:
        raise _validation_error(field, "must be a lowercase SHA-256 hexadecimal digest")
    return value


def _safe_basename(value: object, *, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or any(forbidden in value for forbidden in ("/", "\\", "\0", "\r", "\n"))
    ):
        raise _validation_error(field, "must be a non-empty safe basename")
    return value


def _parse_json_integer(value: str) -> int:
    digit_text = value[1:] if value.startswith("-") else value
    if len(digit_text) > _MAX_JSON_INTEGER_DIGITS:
        raise ValueError("JSON integer exceeds the supported digit limit")
    return int(value)


def _contains_duplicate_json_object(value: object) -> bool:
    if value is _DUPLICATE_JSON_OBJECT:
        return True
    if type(value) is list:
        return any(_contains_duplicate_json_object(item) for item in cast(list[object], value))
    if type(value) is dict:
        return any(
            _contains_duplicate_json_object(item)
            for item in cast(dict[str, object], value).values()
        )
    return False


def _strict_json_object(pairs: list[tuple[str, object]]) -> object:
    result: dict[str, object] = {}
    duplicate_found = False
    for key, value in pairs:
        if key in result or _contains_duplicate_json_object(value):
            duplicate_found = True
        elif not duplicate_found:
            result[key] = value
    return _DUPLICATE_JSON_OBJECT if duplicate_found else result


def _validate_byte_digest_presence(
    *,
    byte_count: int,
    digest: str,
    field: str,
) -> None:
    if byte_count == 0 and digest != _EMPTY_SHA256:
        raise _validation_error(
            field,
            "must be the empty-byte SHA-256 digest when byte count is zero",
        )
    if byte_count > 0 and digest == _EMPTY_SHA256:
        raise _validation_error(
            field,
            "must not be the empty-byte SHA-256 digest when byte count is nonzero",
        )


def _canonical_symbol(value: object) -> str:
    symbol = _require_string(value, field="symbol")
    parts = symbol.split("/")
    if len(parts) != 2:
        raise _validation_error("symbol", "must be a canonical BASE/QUOTE symbol")

    canonical: str | None = None
    dependency_failed = False
    try:
        canonical = MarketSymbol(base=parts[0], quote=parts[1]).canonical
    except ValueError:
        dependency_failed = True
    if dependency_failed or canonical is None or canonical != symbol:
        raise _validation_error("symbol", "must be a canonical BASE/QUOTE symbol")
    return symbol


def _supported_interval(value: object) -> str:
    interval = _require_string(value, field="interval")
    if interval not in SUPPORTED_INTERVALS:
        raise _validation_error("interval", "must be an exact supported interval")
    return interval


def _normalize_exact_aware_datetime(value: object, *, field: str) -> datetime:
    """Normalize a required exact datetime to UTC. Rejects subclasses.

    BLOCKER 2 fix: utcoffset() and astimezone() are in the same protected try so that
    a stateful or malformed tzinfo whose utcoffset() succeeds but whose astimezone() raises
    ValueError is caught by the same handler and sanitized to ResearchManifestValidationError
    with detached __cause__/__context__.
    """
    if type(value) is not datetime:
        raise _validation_error(field, "must be an exact datetime")

    # Both utcoffset() and astimezone() must be protected — a stateful tzinfo can
    # return timedelta(0) on the first call but raise ValueError on the second.
    normalized: datetime | None = None
    dependency_failed = False
    try:
        offset = value.utcoffset()
        if offset is None:
            dependency_failed = True
        else:
            normalized = value.astimezone(UTC)
    except (OverflowError, TypeError, ValueError):
        dependency_failed = True

    if dependency_failed or normalized is None:
        raise _validation_error(field, "must be an aware datetime")
    return normalized


def _normalize_optional_aware_datetime(value: object, *, field: str) -> datetime | None:
    """Normalize an optional datetime: None stays None, exact aware datetime normalizes to UTC."""
    if value is None:
        return None
    return _normalize_exact_aware_datetime(value, field=field)


def _normalize_aware_datetime(value: object, *, field: str) -> datetime:
    """Normalize any aware datetime (used by ResearchDatasetManifest and _parse_datetime)."""
    return _normalize_exact_aware_datetime(value, field=field)


def _validate_optional_aware_datetime(value: object, *, field: str) -> None:
    """Validate optional datetime fields: only None or exact aware datetime."""
    if value is None:
        return
    # Delegate to normalizer: rejects subclasses, catches OverflowError/TypeError/ValueError,
    # normalizes to UTC, raises with detached __cause__/__context__.
    _normalize_optional_aware_datetime(value, field=field)


def _parse_datetime(value: object, *, field: str) -> datetime:
    text = _require_string(value, field=field)
    if not text:
        raise _validation_error(field, "must be an ISO-8601 aware datetime")

    parsed: datetime | None = None
    parser_failed = False
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parser_failed = True
    if parser_failed or parsed is None:
        raise _validation_error(field, "must be an ISO-8601 aware datetime")
    return _normalize_aware_datetime(parsed, field=field)


def _parse_optional_datetime(value: object, *, field: str) -> datetime | None:
    if value is None:
        return None
    return _parse_datetime(value, field=field)


def _datetime_from_epoch_ms(value: int, *, field: str) -> datetime:
    converted: datetime | None = None
    conversion_failed = False
    try:
        converted = _EPOCH + timedelta(milliseconds=value)
    except OverflowError:
        conversion_failed = True
    if conversion_failed or converted is None:
        raise _validation_error(field, "must be a representable epoch-millisecond timestamp")
    return converted


def _require_exact_record(
    value: object,
    *,
    fields: frozenset[str],
    field: str,
) -> dict[str, object]:
    if type(value) is not dict:
        raise _validation_error(field, "must be an object with exactly the required fields")
    record = cast(dict[object, object], value)
    if frozenset(record) != fields:
        raise _validation_error(field, "must be an object with exactly the required fields")
    return cast(dict[str, object], value)


def _require_json_array(value: object, *, field: str) -> list[object]:
    if type(value) is not list:
        raise _validation_error(field, "must be a JSON array")
    return cast(list[object], value)


def _require_conversion_status(value: object) -> ConversionStatus:
    if not isinstance(value, str) or value not in _CONVERSION_STATUSES:
        raise _validation_error("conversion_status", "must be a supported conversion status")
    return cast(ConversionStatus, value)


def _require_artifact_status(value: object) -> ConversionStatus:
    if not isinstance(value, str) or value not in _CONVERSION_STATUSES:
        raise _validation_error("status", "must be a supported conversion status")
    return cast(ConversionStatus, value)


def _require_publication_status(value: object) -> PublicationStatus:
    if not isinstance(value, str) or value not in _PUBLICATION_STATUSES:
        raise _validation_error("completion_status", "must be a supported publication status")
    return cast(PublicationStatus, value)


def _validate_symbols(value: object) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise _validation_error("symbols", "must be a tuple")
    symbols = cast(tuple[object, ...], value)
    if not symbols:
        raise _validation_error("symbols", "must be non-empty")
    for symbol in symbols:
        if type(symbol) is not str:
            raise _validation_error("symbols", "must be a tuple of exact strings")
        _canonical_symbol(symbol)
    if tuple(sorted(cast(tuple[str, ...], symbols))) != symbols or len(set(symbols)) != len(
        symbols
    ):
        raise _validation_error("symbols", "must be sorted and deduplicated")
    return cast(tuple[str, ...], value)


def _validate_intervals(value: object) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise _validation_error("intervals", "must be a tuple")
    intervals = cast(tuple[object, ...], value)
    if not intervals:
        raise _validation_error("intervals", "must be non-empty")
    for interval in intervals:
        if type(interval) is not str:
            raise _validation_error("intervals", "must be a tuple of exact strings")
        _supported_interval(interval)
    if tuple(sorted(cast(tuple[str, ...], intervals))) != intervals or len(set(intervals)) != len(
        intervals
    ):
        raise _validation_error("intervals", "must be sorted and deduplicated")
    return cast(tuple[str, ...], value)


def _validate_expected_raw_files(value: object) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise _validation_error("expected_raw_files", "must be a tuple")
    names = cast(tuple[object, ...], value)
    for name in names:
        _safe_basename(name, field="expected_raw_files")
    if tuple(sorted(cast(tuple[str, ...], names))) != names or len(set(names)) != len(names):
        raise _validation_error("expected_raw_files", "must be sorted and deduplicated")
    return cast(tuple[str, ...], value)


def _raw_output_name(symbol: str, interval: str) -> str:
    return f"{symbol.replace('/', '-')}-{interval}.jsonl"


def _failure_output_name(symbol: str, interval: str) -> str:
    return f"{symbol.replace('/', '-')}-{interval}.failures.jsonl"


@dataclass(frozen=True, kw_only=True)
class ResearchFilePlan:
    """Immutable pre-conversion identity contract for a single research file.

    Validates and derives the canonical naming triplet (raw, research, failure)
    from a raw identity without any filesystem I/O.
    """

    raw_name: str
    research_name: str
    failure_name: str
    symbol: str
    interval: str

    def __post_init__(self) -> None:
        # Validate names using the centralized naming authority.
        # Errors are sanitized: no raw value appears in the error.
        raw_validated = _safe_basename(self.raw_name, field="raw_name")
        research_validated = _safe_basename(self.research_name, field="research_name")
        failure_validated = _safe_basename(self.failure_name, field="failure_name")

        # Validate identity fields.
        symbol_validated = _canonical_symbol(self.symbol)
        interval_validated = _supported_interval(self.interval)

        # Cross-check raw_name against the canonical naming formula first.
        # This fires when raw_name contains the wrong symbol/interval.
        expected_raw = _raw_output_name(symbol_validated, interval_validated)
        if raw_validated != expected_raw:
            raise _validation_error("raw_name", "must match the symbol and interval")

        # Enforce naming authority: raw_name and research_name must be identical.
        if research_validated != raw_validated:
            raise _validation_error("research_name", "must match the symbol and interval")

        # Derive and cross-check the canonical failure name.
        expected_failure = _failure_output_name(symbol_validated, interval_validated)
        if failure_validated != expected_failure:
            raise _validation_error("failure_name", "must match the symbol and interval")

    @classmethod
    def from_raw_identity(
        cls,
        *,
        raw_name: str,
        symbol: str,
        interval: str,
    ) -> ResearchFilePlan:
        """Derive the full plan from a raw identity (name + symbol + interval)."""
        # Validate inputs first to fail fast with sanitized errors.
        validated_raw_name = _safe_basename(raw_name, field="raw_name")
        validated_symbol = _canonical_symbol(symbol)
        validated_interval = _supported_interval(interval)

        # Derive canonical names using the centralized naming authority only.
        derived_raw = _raw_output_name(validated_symbol, validated_interval)
        derived_research = derived_raw
        derived_failure = _failure_output_name(validated_symbol, validated_interval)

        # Cross-check: the caller's raw_name must match what we derive.
        if validated_raw_name != derived_raw:
            raise _validation_error("raw_name", "must match the symbol and interval")

        return cls(
            raw_name=derived_raw,
            research_name=derived_research,
            failure_name=derived_failure,
            symbol=symbol,
            interval=interval,
        )


@dataclass(frozen=True, kw_only=True)
class ResearchFileArtifact:
    """Immutable exact-byte contract for one completed raw-file conversion."""

    raw_name: str
    research_name: str
    failure_name: str
    symbol: str
    interval: str
    raw_sha256: str
    raw_bytes: int
    lines_seen: int
    records_written: int
    records_quarantined: int
    coverage_start_ms: int | None
    coverage_end_ms: int | None
    research_sha256: str
    failure_sha256: str
    research_bytes: int
    failure_bytes: int
    status: ConversionStatus

    def __post_init__(self) -> None:
        # Reuse ResearchFilePlan validation as the single naming authority.
        plan = ResearchFilePlan(
            raw_name=self.raw_name,
            research_name=self.research_name,
            failure_name=self.failure_name,
            symbol=self.symbol,
            interval=self.interval,
        )

        _require_lowercase_sha256(self.raw_sha256, field="raw_sha256")
        _require_lowercase_sha256(self.research_sha256, field="research_sha256")
        _require_lowercase_sha256(self.failure_sha256, field="failure_sha256")
        for field in _ARTIFACT_COUNTER_FIELDS:
            _require_non_negative_integer(getattr(self, field), field=field)
        if self.coverage_start_ms is not None:
            _require_non_negative_integer(self.coverage_start_ms, field="coverage_start_ms")
        if self.coverage_end_ms is not None:
            _require_non_negative_integer(self.coverage_end_ms, field="coverage_end_ms")
        _require_artifact_status(self.status)

        if (self.lines_seen == 0) != (self.raw_bytes == 0):
            raise _validation_error(
                "raw_bytes",
                "must be zero exactly when lines_seen is zero",
            )
        if (self.records_written == 0) != (self.research_bytes == 0):
            raise _validation_error(
                "research_bytes",
                "must be zero exactly when records_written is zero",
            )
        if (self.records_quarantined == 0) != (self.failure_bytes == 0):
            raise _validation_error(
                "failure_bytes",
                "must be zero exactly when records_quarantined is zero",
            )
        _validate_byte_digest_presence(
            byte_count=self.raw_bytes,
            digest=self.raw_sha256,
            field="raw_sha256",
        )
        _validate_byte_digest_presence(
            byte_count=self.research_bytes,
            digest=self.research_sha256,
            field="research_sha256",
        )
        _validate_byte_digest_presence(
            byte_count=self.failure_bytes,
            digest=self.failure_sha256,
            field="failure_sha256",
        )

        stream_contract_failed = False
        try:
            StreamConversionReport(
                file=plan.raw_name,
                lines_seen=self.lines_seen,
                records_written=self.records_written,
                records_quarantined=self.records_quarantined,
                coverage_start_ms=self.coverage_start_ms,
                coverage_end_ms=self.coverage_end_ms,
                research_sha256=self.research_sha256,
                failure_sha256=self.failure_sha256,
                research_bytes=self.research_bytes,
                failure_bytes=self.failure_bytes,
                status=self.status,
            )
        except ValueError:
            stream_contract_failed = True
        if stream_contract_failed:
            raise _validation_error("report", "fields violate stream conversion invariants")

    @classmethod
    def from_stream_report(
        cls,
        *,
        raw_name: str,
        research_name: str,
        failure_name: str,
        symbol: str,
        interval: str,
        raw_sha256: str,
        raw_bytes: int,
        report: StreamConversionReport,
    ) -> ResearchFileArtifact:
        """Copy one exact stream report into a publication artifact."""
        validated_raw_name = _safe_basename(raw_name, field="raw_name")
        if type(report) is not StreamConversionReport:
            raise _validation_error("report", "must be a StreamConversionReport")
        if report.file != validated_raw_name:
            raise _validation_error("report", "file must match raw_name")
        return cls(
            raw_name=validated_raw_name,
            research_name=research_name,
            failure_name=failure_name,
            symbol=symbol,
            interval=interval,
            raw_sha256=raw_sha256,
            raw_bytes=raw_bytes,
            lines_seen=report.lines_seen,
            records_written=report.records_written,
            records_quarantined=report.records_quarantined,
            coverage_start_ms=report.coverage_start_ms,
            coverage_end_ms=report.coverage_end_ms,
            research_sha256=report.research_sha256,
            failure_sha256=report.failure_sha256,
            research_bytes=report.research_bytes,
            failure_bytes=report.failure_bytes,
            status=report.status,
        )


def _artifact_to_record(artifact: ResearchFileArtifact) -> dict[str, object]:
    return {
        "coverage_end_ms": artifact.coverage_end_ms,
        "coverage_start_ms": artifact.coverage_start_ms,
        "failure_bytes": artifact.failure_bytes,
        "failure_name": artifact.failure_name,
        "failure_sha256": artifact.failure_sha256,
        "interval": artifact.interval,
        "lines_seen": artifact.lines_seen,
        "raw_bytes": artifact.raw_bytes,
        "raw_name": artifact.raw_name,
        "raw_sha256": artifact.raw_sha256,
        "records_quarantined": artifact.records_quarantined,
        "records_written": artifact.records_written,
        "research_bytes": artifact.research_bytes,
        "research_name": artifact.research_name,
        "research_sha256": artifact.research_sha256,
        "status": artifact.status,
        "symbol": artifact.symbol,
    }


def _optional_non_negative_integer(value: object, *, field: str) -> int | None:
    if value is None:
        return None
    return _require_non_negative_integer(value, field=field)


def _artifact_from_record(value: object) -> ResearchFileArtifact:
    record = _require_exact_record(
        value,
        fields=_ARTIFACT_RECORD_FIELDS,
        field="files",
    )
    return ResearchFileArtifact(
        raw_name=_require_string(record["raw_name"], field="raw_name"),
        research_name=_require_string(record["research_name"], field="research_name"),
        failure_name=_require_string(record["failure_name"], field="failure_name"),
        symbol=_require_string(record["symbol"], field="symbol"),
        interval=_require_string(record["interval"], field="interval"),
        raw_sha256=_require_string(record["raw_sha256"], field="raw_sha256"),
        raw_bytes=_require_non_negative_integer(record["raw_bytes"], field="raw_bytes"),
        lines_seen=_require_non_negative_integer(record["lines_seen"], field="lines_seen"),
        records_written=_require_non_negative_integer(
            record["records_written"], field="records_written"
        ),
        records_quarantined=_require_non_negative_integer(
            record["records_quarantined"], field="records_quarantined"
        ),
        coverage_start_ms=_optional_non_negative_integer(
            record["coverage_start_ms"], field="coverage_start_ms"
        ),
        coverage_end_ms=_optional_non_negative_integer(
            record["coverage_end_ms"], field="coverage_end_ms"
        ),
        research_sha256=_require_string(record["research_sha256"], field="research_sha256"),
        failure_sha256=_require_string(record["failure_sha256"], field="failure_sha256"),
        research_bytes=_require_non_negative_integer(
            record["research_bytes"], field="research_bytes"
        ),
        failure_bytes=_require_non_negative_integer(record["failure_bytes"], field="failure_bytes"),
        status=_require_artifact_status(record["status"]),
    )


@dataclass(frozen=True, kw_only=True)
class ResearchDatasetManifest:
    """Immutable deterministic manifest for research publication state.

    ``completion_status`` tracks whether every expected raw file is represented.
    ``conversion_status`` independently reports whether any completed artifact
    quarantined records.
    """

    dataset_id: str
    raw_dataset_version: str
    dataset_version: str
    downloader_version: str
    converter_version: str
    schema_version: int
    source: str
    exchange: str
    market_type: str
    symbols: tuple[str, ...]
    intervals: tuple[str, ...]
    requested_start: datetime
    requested_end: datetime
    coverage_start: datetime | None
    coverage_end: datetime | None
    raw_manifest_sha256: str
    research_checksum: str
    failure_checksum: str
    expected_raw_files: tuple[str, ...]
    files: tuple[ResearchFileArtifact, ...]
    lines_seen: int
    records_written: int
    records_quarantined: int
    research_bytes: int
    failure_bytes: int
    conversion_status: ConversionStatus
    completion_status: PublicationStatus
    max_line_bytes: int

    def __post_init__(self) -> None:
        _require_non_empty_string(self.dataset_id, field="dataset_id")
        if self.raw_dataset_version != DATASET_DOWNLOAD_VERSION:
            raise _validation_error(
                "raw_dataset_version", "must match the current raw dataset version"
            )
        if self.dataset_version != RESEARCH_DATASET_VERSION:
            raise _validation_error("dataset_version", "must match the research dataset version")
        if self.downloader_version != DOWNLOADER_VERSION:
            raise _validation_error(
                "downloader_version", "must match the current downloader version"
            )
        if self.converter_version != RESEARCH_CONVERTER_VERSION:
            raise _validation_error(
                "converter_version", "must match the research converter version"
            )
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version != RESEARCH_SCHEMA_VERSION
        ):
            raise _validation_error("schema_version", "must match the research schema version")
        if self.source != RESEARCH_SOURCE or self.source != SOURCE:
            raise _validation_error("source", "must match the public research source")
        if self.exchange != "binance":
            raise _validation_error("exchange", "must be binance")
        if self.market_type != "spot":
            raise _validation_error("market_type", "must be spot")

        symbols = _validate_symbols(self.symbols)
        intervals = _validate_intervals(self.intervals)
        expected_raw_files = _validate_expected_raw_files(self.expected_raw_files)
        requested_start = _normalize_aware_datetime(self.requested_start, field="requested_start")
        requested_end = _normalize_aware_datetime(self.requested_end, field="requested_end")
        if requested_end <= requested_start:
            raise _validation_error("requested_end", "must be later than requested_start")

        _require_lowercase_sha256(self.raw_manifest_sha256, field="raw_manifest_sha256")
        _require_lowercase_sha256(self.research_checksum, field="research_checksum")
        _require_lowercase_sha256(self.failure_checksum, field="failure_checksum")
        for field in _MANIFEST_COUNTER_FIELDS:
            _require_non_negative_integer(getattr(self, field), field=field)
        _require_positive_integer(self.max_line_bytes, field="max_line_bytes")
        _require_conversion_status(self.conversion_status)
        completion_status = _require_publication_status(self.completion_status)

        if type(self.files) is not tuple:
            raise _validation_error("files", "must be a tuple")
        for artifact in self.files:
            if type(artifact) is not ResearchFileArtifact:
                raise _validation_error("files", "must contain ResearchFileArtifact values")
        files = self.files
        if tuple(sorted(files, key=lambda artifact: artifact.raw_name)) != files:
            raise _validation_error("files", "must be sorted by raw_name")

        raw_names = tuple(artifact.raw_name for artifact in files)
        research_names = tuple(artifact.research_name for artifact in files)
        failure_names = tuple(artifact.failure_name for artifact in files)
        pairs = tuple((artifact.symbol, artifact.interval) for artifact in files)
        if len(set(raw_names)) != len(raw_names):
            raise _validation_error("files", "raw_name values must be unique")
        if len(set(research_names)) != len(research_names):
            raise _validation_error("files", "research_name values must be unique")
        if len(set(failure_names)) != len(failure_names):
            raise _validation_error("files", "failure_name values must be unique")
        if len(set(pairs)) != len(pairs):
            raise _validation_error("files", "symbol and interval pairs must be unique")
        for artifact in files:
            if artifact.symbol not in symbols or artifact.interval not in intervals:
                raise _validation_error("files", "artifact identity must belong to the manifest")
            if artifact.raw_name not in expected_raw_files:
                raise _validation_error("files", "artifact raw_name must be expected")

        completed_names = frozenset(raw_names)
        expected_names = frozenset(expected_raw_files)
        if completion_status == "complete":
            if completed_names != expected_names:
                raise _validation_error(
                    "completion_status", "complete requires every expected file"
                )
        elif not completed_names < expected_names:
            raise _validation_error("completion_status", "incomplete requires a proper subset")

        aggregate_lines_seen = sum(artifact.lines_seen for artifact in files)
        aggregate_records_written = sum(artifact.records_written for artifact in files)
        aggregate_records_quarantined = sum(artifact.records_quarantined for artifact in files)
        aggregate_research_bytes = sum(artifact.research_bytes for artifact in files)
        aggregate_failure_bytes = sum(artifact.failure_bytes for artifact in files)
        if self.lines_seen != aggregate_lines_seen:
            raise _validation_error("lines_seen", "must equal the artifact total")
        if self.records_written != aggregate_records_written:
            raise _validation_error("records_written", "must equal the artifact total")
        if self.records_quarantined != aggregate_records_quarantined:
            raise _validation_error("records_quarantined", "must equal the artifact total")
        if self.research_bytes != aggregate_research_bytes:
            raise _validation_error("research_bytes", "must equal the artifact total")
        if self.failure_bytes != aggregate_failure_bytes:
            raise _validation_error("failure_bytes", "must equal the artifact total")

        expected_conversion_status: ConversionStatus = (
            "partial" if aggregate_records_quarantined > 0 else "success"
        )
        if self.conversion_status != expected_conversion_status:
            raise _validation_error("conversion_status", "must match the quarantined record total")

        coverage_start: datetime | None = None
        coverage_end: datetime | None = None
        if aggregate_records_written == 0:
            if self.coverage_start is not None or self.coverage_end is not None:
                raise _validation_error("coverage_start", "coverage must be absent without records")
        else:
            if self.coverage_start is None or self.coverage_end is None:
                raise _validation_error("coverage_start", "coverage must be present with records")
            coverage_start = _normalize_aware_datetime(self.coverage_start, field="coverage_start")
            coverage_end = _normalize_aware_datetime(self.coverage_end, field="coverage_end")
            if coverage_end <= coverage_start:
                raise _validation_error("coverage_end", "must be later than coverage_start")
            if coverage_start < requested_start:
                raise _validation_error(
                    "coverage_start",
                    "must not be earlier than requested_start",
                )
            if coverage_end > requested_end:
                raise _validation_error(
                    "coverage_end",
                    "must not be later than requested_end",
                )

            accepted_starts_ms: list[int] = []
            accepted_ends_ms: list[int] = []
            for artifact in files:
                if artifact.records_written == 0:
                    continue
                if artifact.coverage_start_ms is None or artifact.coverage_end_ms is None:
                    raise _validation_error("files", "accepted artifacts must have coverage")
                accepted_starts_ms.append(artifact.coverage_start_ms)
                accepted_ends_ms.append(artifact.coverage_end_ms)
            expected_coverage_start = _datetime_from_epoch_ms(
                min(accepted_starts_ms), field="coverage_start"
            )
            expected_coverage_end = _datetime_from_epoch_ms(
                max(accepted_ends_ms), field="coverage_end"
            )
            if coverage_start != expected_coverage_start:
                raise _validation_error("coverage_start", "must equal the artifact minimum")
            if coverage_end != expected_coverage_end:
                raise _validation_error("coverage_end", "must equal the artifact maximum")

        if aggregate_research_bytes == 0 and self.research_checksum != _EMPTY_SHA256:
            raise _validation_error("research_checksum", "must be the empty-byte SHA-256 digest")
        if aggregate_failure_bytes == 0 and self.failure_checksum != _EMPTY_SHA256:
            raise _validation_error("failure_checksum", "must be the empty-byte SHA-256 digest")

        object.__setattr__(self, "requested_start", requested_start)
        object.__setattr__(self, "requested_end", requested_end)
        object.__setattr__(self, "coverage_start", coverage_start)
        object.__setattr__(self, "coverage_end", coverage_end)

    def to_record(self) -> dict[str, object]:
        """Return the canonical JSON-safe document with sorted keys."""
        return {
            "completion_status": self.completion_status,
            "conversion_status": self.conversion_status,
            "converter_version": self.converter_version,
            "coverage_end": self.coverage_end.isoformat() if self.coverage_end else None,
            "coverage_start": self.coverage_start.isoformat() if self.coverage_start else None,
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "downloader_version": self.downloader_version,
            "exchange": self.exchange,
            "expected_raw_files": list(self.expected_raw_files),
            "failure_bytes": self.failure_bytes,
            "failure_checksum": self.failure_checksum,
            "files": [_artifact_to_record(artifact) for artifact in self.files],
            "intervals": list(self.intervals),
            "lines_seen": self.lines_seen,
            "market_type": self.market_type,
            "max_line_bytes": self.max_line_bytes,
            "raw_dataset_version": self.raw_dataset_version,
            "raw_manifest_sha256": self.raw_manifest_sha256,
            "records_quarantined": self.records_quarantined,
            "records_written": self.records_written,
            "requested_end": self.requested_end.isoformat(),
            "requested_start": self.requested_start.isoformat(),
            "research_bytes": self.research_bytes,
            "research_checksum": self.research_checksum,
            "schema_version": self.schema_version,
            "source": self.source,
            "symbols": list(self.symbols),
        }

    def to_json(self) -> str:
        """Serialize compactly and deterministically without a trailing newline."""
        return json.dumps(self.to_record(), separators=(",", ":"), sort_keys=True)

    @classmethod
    def from_record(cls, record: object) -> ResearchDatasetManifest:
        """Restore a manifest from one exact-key JSON-safe object."""
        values = _require_exact_record(
            record,
            fields=_MANIFEST_RECORD_FIELDS,
            field="record",
        )
        symbol_values = _require_json_array(values["symbols"], field="symbols")
        interval_values = _require_json_array(values["intervals"], field="intervals")
        expected_values = _require_json_array(
            values["expected_raw_files"], field="expected_raw_files"
        )
        file_values = _require_json_array(values["files"], field="files")
        return cls(
            dataset_id=_require_string(values["dataset_id"], field="dataset_id"),
            raw_dataset_version=_require_string(
                values["raw_dataset_version"], field="raw_dataset_version"
            ),
            dataset_version=_require_string(values["dataset_version"], field="dataset_version"),
            downloader_version=_require_string(
                values["downloader_version"], field="downloader_version"
            ),
            converter_version=_require_string(
                values["converter_version"], field="converter_version"
            ),
            schema_version=_require_non_negative_integer(
                values["schema_version"], field="schema_version"
            ),
            source=_require_string(values["source"], field="source"),
            exchange=_require_string(values["exchange"], field="exchange"),
            market_type=_require_string(values["market_type"], field="market_type"),
            symbols=tuple(_require_string(value, field="symbols") for value in symbol_values),
            intervals=tuple(_require_string(value, field="intervals") for value in interval_values),
            requested_start=_parse_datetime(values["requested_start"], field="requested_start"),
            requested_end=_parse_datetime(values["requested_end"], field="requested_end"),
            coverage_start=_parse_optional_datetime(
                values["coverage_start"], field="coverage_start"
            ),
            coverage_end=_parse_optional_datetime(values["coverage_end"], field="coverage_end"),
            raw_manifest_sha256=_require_string(
                values["raw_manifest_sha256"], field="raw_manifest_sha256"
            ),
            research_checksum=_require_string(
                values["research_checksum"], field="research_checksum"
            ),
            failure_checksum=_require_string(values["failure_checksum"], field="failure_checksum"),
            expected_raw_files=tuple(
                _require_string(value, field="expected_raw_files") for value in expected_values
            ),
            files=tuple(_artifact_from_record(value) for value in file_values),
            lines_seen=_require_non_negative_integer(values["lines_seen"], field="lines_seen"),
            records_written=_require_non_negative_integer(
                values["records_written"], field="records_written"
            ),
            records_quarantined=_require_non_negative_integer(
                values["records_quarantined"], field="records_quarantined"
            ),
            research_bytes=_require_non_negative_integer(
                values["research_bytes"], field="research_bytes"
            ),
            failure_bytes=_require_non_negative_integer(
                values["failure_bytes"], field="failure_bytes"
            ),
            conversion_status=_require_conversion_status(values["conversion_status"]),
            completion_status=_require_publication_status(values["completion_status"]),
            max_line_bytes=_require_positive_integer(
                values["max_line_bytes"], field="max_line_bytes"
            ),
        )

    @classmethod
    def from_json(cls, text: object) -> ResearchDatasetManifest:
        """Restore a manifest from deterministic JSON with detached errors."""
        if not isinstance(text, str):
            raise _validation_error("text", "must be a JSON string")

        record: object = None
        parser_failed = False
        try:
            record = json.loads(
                text,
                object_pairs_hook=_strict_json_object,
                parse_int=_parse_json_integer,
            )
        except json.JSONDecodeError:
            parser_failed = True
        except RecursionError:
            parser_failed = True
        except ValueError:
            parser_failed = True
        if parser_failed:
            raise _validation_error("text", "must contain valid JSON")
        if _contains_duplicate_json_object(record):
            raise _validation_error("text", "must not contain duplicate object keys")
        if type(record) is not dict:
            raise _validation_error("record", "must be a JSON object")
        return cls.from_record(record)


def _validate_raw_manifest(
    raw_manifest: DownloadManifest,
) -> tuple[datetime, datetime, tuple[OutputFileInfo, ...]]:
    # Strict outer contract: reject non-DownloadManifest objects and subclasses.
    if type(raw_manifest) is not DownloadManifest:
        raise _validation_error("raw_manifest", "must be a DownloadManifest")

    # ============================================================
    # STEP 1 — Exact type preflight: ALL scalar/nested fields
    # before ANY semantic operation (comparison, truthiness, etc.)
    # ============================================================

    # Exact scalars first.
    _require_exact_non_negative_integer(raw_manifest.schema_version, field="raw_manifest")
    _require_exact_string(raw_manifest.dataset_id, field="raw_manifest")
    _require_exact_string(raw_manifest.dataset_version, field="raw_manifest")
    _require_exact_string(raw_manifest.downloader_version, field="raw_manifest")
    _require_exact_string(raw_manifest.source, field="raw_manifest")
    _require_exact_string(raw_manifest.exchange, field="raw_manifest")
    _require_exact_string(raw_manifest.market_type, field="raw_manifest")
    _require_exact_string(raw_manifest.completion_status, field="raw_manifest")
    _require_exact_bool(raw_manifest.resume, field="raw_manifest")
    _require_exact_non_negative_integer(raw_manifest.page_limit, field="raw_manifest")
    _require_exact_non_negative_integer(raw_manifest.record_count, field="raw_manifest")

    # Exact tuple preflight for symbols/intervals BEFORE canonical validators dispatch.
    if type(raw_manifest.symbols) is not tuple:
        raise _validation_error("symbols", "must be a tuple")
    _symbols_tuple = cast(tuple[object, ...], raw_manifest.symbols)
    for _sym in _symbols_tuple:
        if type(_sym) is not str:
            raise _validation_error("symbols", "must be a tuple of exact strings")

    if type(raw_manifest.intervals) is not tuple:
        raise _validation_error("intervals", "must be a tuple")
    _intervals_tuple = cast(tuple[object, ...], raw_manifest.intervals)
    for _iv in _intervals_tuple:
        if type(_iv) is not str:
            raise _validation_error("intervals", "must be a tuple of exact strings")

    # Canonical validation: non-empty, exact str members, canonical form, sorted/deduplicated.
    # BLOCKER 1 fix: call canonical validators so C4A maps invalid symbols/intervals
    # to operation=validate_manifest.
    symbols: tuple[str, ...] = _validate_symbols(raw_manifest.symbols)
    intervals: tuple[str, ...] = _validate_intervals(raw_manifest.intervals)

    # Exact required datetimes (Finding 3: reject datetime subclasses).
    requested_start = _normalize_exact_aware_datetime(
        raw_manifest.requested_start, field="raw_manifest"
    )
    requested_end = _normalize_exact_aware_datetime(
        raw_manifest.requested_end, field="raw_manifest"
    )

    # Exact optional datetimes: normalize once, reuse cached values for all semantic checks.
    # This is the only normalization pass — no re-observation of the same untrusted datetime.
    actual_start: datetime | None = _normalize_optional_aware_datetime(
        raw_manifest.actual_start, field="raw_manifest"
    )
    actual_end: datetime | None = _normalize_optional_aware_datetime(
        raw_manifest.actual_end, field="raw_manifest"
    )
    _normalize_optional_aware_datetime(raw_manifest.server_time, field="raw_manifest")

    # Exact tuple for files.
    if type(raw_manifest.files) is not tuple:
        raise _validation_error("raw_manifest", "files must be a tuple")
    files = cast(tuple[object, ...], raw_manifest.files)

    # Build allowed filenames from canonical symbol/interval members.
    allowed_file_names = frozenset(_raw_output_name(sym, iv) for sym in symbols for iv in intervals)

    # Nested OutputFileInfo with exact nested scalars.
    validated_files: list[OutputFileInfo] = []
    # BLOCKER 2 fix: normalize range datetimes once, reuse for all semantic checks.
    # Avoids repeated utcoffset()/astimezone() on the same untrusted datetime.
    normalized_file_ranges: list[tuple[datetime, datetime]] = []
    for file_info in files:
        if type(file_info) is not OutputFileInfo:
            raise _validation_error("raw_manifest", "files must contain OutputFileInfo values")
        typed_file = file_info
        # Exact str name BEFORE safe_basename dispatches.
        if type(typed_file.name) is not str:
            raise _validation_error("raw_manifest", "file name must be an exact string")
        _safe_basename(typed_file.name, field="raw_manifest")
        # Exact int records BEFORE numeric comparison.
        if type(typed_file.records) is not int or isinstance(typed_file.records, bool):
            raise _validation_error("raw_manifest", "records must be an exact integer")
        # Exact required datetimes for OutputFileInfo (Finding 3).
        file_rs = _normalize_exact_aware_datetime(typed_file.range_start, field="raw_manifest")
        file_re = _normalize_exact_aware_datetime(typed_file.range_end, field="raw_manifest")
        validated_files.append(typed_file)
        normalized_file_ranges.append((file_rs, file_re))

    # ============================================================
    # STEP 2 — Semantic operations: safe to use exact values now.
    # ============================================================

    _require_non_empty_string(raw_manifest.dataset_id, field="raw_manifest")
    if raw_manifest.completion_status != "complete":
        raise _validation_error("raw_manifest", "must be complete")
    if raw_manifest.failure is not None:
        raise _validation_error("raw_manifest", "must not contain a failure")
    if raw_manifest.dataset_version != DATASET_DOWNLOAD_VERSION:
        raise _validation_error("raw_manifest", "must use the current raw dataset version")
    if raw_manifest.downloader_version != DOWNLOADER_VERSION:
        raise _validation_error("raw_manifest", "must use the current downloader version")
    if raw_manifest.schema_version != DATASET_SCHEMA_VERSION:
        raise _validation_error("raw_manifest", "must use the current raw schema version")
    if raw_manifest.source != SOURCE or raw_manifest.source != RESEARCH_SOURCE:
        raise _validation_error("raw_manifest", "must use the public research source")
    if raw_manifest.exchange != "binance":
        raise _validation_error("raw_manifest", "must use binance")
    if raw_manifest.market_type != "spot":
        raise _validation_error("raw_manifest", "must use spot market data")

    if not 1 <= raw_manifest.page_limit <= 1000:
        raise _validation_error("raw_manifest", "must use a page_limit between 1 and 1000")
    if raw_manifest.record_count < 0:
        raise _validation_error("raw_manifest", "record_count must be a non-negative integer")
    if requested_end <= requested_start:
        raise _validation_error("raw_manifest", "requested range must be increasing")

    for typed_file, (file_rs, file_re) in zip(validated_files, normalized_file_ranges, strict=True):
        typed_name = typed_file.name
        _safe_basename(typed_name, field="raw_manifest")
        if typed_name not in allowed_file_names:
            raise _validation_error(
                "raw_manifest",
                "file names must match a requested symbol and interval",
            )
        if typed_file.records < 0:
            raise _validation_error("raw_manifest", "records must be a non-negative integer")
        if file_re <= file_rs:
            raise _validation_error("raw_manifest", "file ranges must be increasing")

    names = tuple(file_info.name for file_info in validated_files)
    if names != tuple(sorted(names)) or len(set(names)) != len(names):
        raise _validation_error("raw_manifest", "file names must be sorted and unique")

    record_count = raw_manifest.record_count
    if record_count != sum(file_info.records for file_info in validated_files):
        raise _validation_error("raw_manifest", "record_count must equal the file total")

    # Cross-field invariants: file ranges within requested half-open range.
    # Uses already-normalized ranges from step 1 — no repeated utcoffset()/astimezone() calls.
    for _file_info, (file_range_start, file_range_end) in zip(
        validated_files, normalized_file_ranges, strict=True
    ):
        if file_range_start < requested_start:
            raise _validation_error(
                "raw_manifest",
                "file range_start must not be earlier than requested_start",
            )
        if file_range_end > requested_end:
            raise _validation_error(
                "raw_manifest",
                "file range_end must not be later than requested_end",
            )

    # Cross-field invariants: actual coverage must match file boundaries.
    # Uses cached normalized values — single normalization per original datetime field.
    if not validated_files:
        # Empty dataset: actual_start and actual_end must be None.
        if actual_start is not None:
            raise _validation_error("raw_manifest", "actual_start must be None for empty files")
        if actual_end is not None:
            raise _validation_error("raw_manifest", "actual_end must be None for empty files")
    else:
        # Non-empty dataset: actual_start and actual_end must be non-None.
        if actual_start is None:
            raise _validation_error(
                "raw_manifest", "actual_start must be non-None for non-empty files"
            )
        if actual_end is None:
            raise _validation_error(
                "raw_manifest", "actual_end must be non-None for non-empty files"
            )
        # actual_start must equal the cached normalized first file range_start.
        first_range_start = normalized_file_ranges[0][0]
        if actual_start != first_range_start:
            raise _validation_error(
                "raw_manifest", "actual_start must equal the first file range_start"
            )
        # actual_end must equal the cached normalized last file range_end.
        last_range_end = normalized_file_ranges[-1][1]
        if actual_end != last_range_end:
            raise _validation_error("raw_manifest", "actual_end must equal the last file range_end")

    return requested_start, requested_end, tuple(validated_files)


def _copy_artifact_sequence(
    files: Sequence[ResearchFileArtifact],
) -> tuple[ResearchFileArtifact, ...]:
    if isinstance(files, (str, bytes, bytearray)) or not isinstance(files, Sequence):
        raise _validation_error("files", "must be a sequence of ResearchFileArtifact values")
    copied = tuple(files)
    for artifact in copied:
        if type(artifact) is not ResearchFileArtifact:
            raise _validation_error("files", "must contain ResearchFileArtifact values")
    return copied


def build_research_manifest(
    raw_manifest: DownloadManifest,
    *,
    raw_manifest_sha256: str,
    files: Sequence[ResearchFileArtifact],
    research_checksum: str,
    failure_checksum: str,
    max_line_bytes: int,
    completion_status: PublicationStatus,
) -> ResearchDatasetManifest:
    """Build one research manifest without touching files or deriving identity."""
    if type(raw_manifest) is not DownloadManifest:
        raise _validation_error("raw_manifest", "must be a DownloadManifest")
    requested_start, requested_end, raw_files = _validate_raw_manifest(raw_manifest)
    _require_lowercase_sha256(raw_manifest_sha256, field="raw_manifest_sha256")
    _require_lowercase_sha256(research_checksum, field="research_checksum")
    _require_lowercase_sha256(failure_checksum, field="failure_checksum")
    _require_positive_integer(max_line_bytes, field="max_line_bytes")
    publication_status = _require_publication_status(completion_status)
    completed_files = _copy_artifact_sequence(files)

    raw_by_name = {file_info.name: file_info for file_info in raw_files}
    for artifact in completed_files:
        raw_file = raw_by_name.get(artifact.raw_name)
        if raw_file is None:
            raise _validation_error("files", "artifact raw_name must exist in the raw manifest")
        if artifact.lines_seen != raw_file.records:
            raise _validation_error("files", "artifact lines_seen must match raw records")

    lines_seen = sum(artifact.lines_seen for artifact in completed_files)
    records_written = sum(artifact.records_written for artifact in completed_files)
    records_quarantined = sum(artifact.records_quarantined for artifact in completed_files)
    research_bytes = sum(artifact.research_bytes for artifact in completed_files)
    failure_bytes = sum(artifact.failure_bytes for artifact in completed_files)
    conversion_status: ConversionStatus = "partial" if records_quarantined > 0 else "success"

    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    accepted = tuple(artifact for artifact in completed_files if artifact.records_written > 0)
    if accepted:
        accepted_starts = [cast(int, artifact.coverage_start_ms) for artifact in accepted]
        accepted_ends = [cast(int, artifact.coverage_end_ms) for artifact in accepted]
        coverage_start = _datetime_from_epoch_ms(min(accepted_starts), field="coverage_start")
        coverage_end = _datetime_from_epoch_ms(max(accepted_ends), field="coverage_end")

    return ResearchDatasetManifest(
        dataset_id=raw_manifest.dataset_id,
        raw_dataset_version=raw_manifest.dataset_version,
        dataset_version=RESEARCH_DATASET_VERSION,
        downloader_version=raw_manifest.downloader_version,
        converter_version=RESEARCH_CONVERTER_VERSION,
        schema_version=RESEARCH_SCHEMA_VERSION,
        source=raw_manifest.source,
        exchange=raw_manifest.exchange,
        market_type=raw_manifest.market_type,
        symbols=raw_manifest.symbols,
        intervals=raw_manifest.intervals,
        requested_start=requested_start,
        requested_end=requested_end,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        raw_manifest_sha256=raw_manifest_sha256,
        research_checksum=research_checksum,
        failure_checksum=failure_checksum,
        expected_raw_files=tuple(file_info.name for file_info in raw_files),
        files=completed_files,
        lines_seen=lines_seen,
        records_written=records_written,
        records_quarantined=records_quarantined,
        research_bytes=research_bytes,
        failure_bytes=failure_bytes,
        conversion_status=conversion_status,
        completion_status=publication_status,
        max_line_bytes=max_line_bytes,
    )
