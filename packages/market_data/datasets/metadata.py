"""DATA-001: immutable dataset metadata and deterministic version format.

Design decisions (documented, do not silently change):

Version format
--------------
``dataset_version`` is a three-part ``MAJOR.MINOR.PATCH`` string validated by
``^\\d+\\.\\d+\\.\\d+$``.  Bump semantics:

* MAJOR: the dataset layout or schema breaks compatibility.
* MINOR: additive, backward-compatible changes (new fields, new quality
  statuses).
* PATCH: corrections that do not change the dataset shape (re-downloaded
  slice, repaired records).

Versions are deterministic: they are chosen by the producer and never embed
timestamps, random values, or machine identity.  Two datasets with the same
identity and version must be byte-identical.

Dataset identity
----------------
``dataset_id`` is a deterministic UUID5 over the identity fields (source,
exchange, market_type, symbols, intervals, UTC coverage, schema version).  It
is stable across re-downloads of the same content and does not include mutable
statistics (checksum, record count, quality status), so a corrected re-export
of the same slice keeps its identity while bumping ``dataset_version``.

Checksums
---------
``checksum`` is the SHA-256 hex digest of the dataset record payloads,
computed by the producer with :func:`compute_dataset_checksum`; it never
contains secrets.  ``metadata_checksum()`` returns the SHA-256 digest of the
canonical JSON metadata document itself, so metadata tampering is observable.

Money and time
--------------
No field may be a float: prices, quantities, and financial metadata are either
absent from metadata or carried as Decimal in canonical events.  All
timestamps are UTC-aware datetimes; naive or offset-less values are rejected
and aware values are normalized to UTC.

Compatibility with replay and backtest
--------------------------------------
``symbols`` are canonical ``BASE/QUOTE`` strings (``MarketSymbol.canonical``)
and ``intervals`` match the kline interval strings of the ingestion layer.
Coverage is a half-open UTC range aligned to candle open times, which is the
same convention used by ``list_candles`` and the replay runner, so a metadata
document can select the exact stored records a backtest consumes.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal, cast

from packages.market_data.adapters.value_types import ExchangeId, MarketSymbol, MarketType

DATASET_SCHEMA_VERSION = 1
SUPPORTED_SCHEMA_VERSIONS: frozenset[int] = frozenset({DATASET_SCHEMA_VERSION})

DatasetQualityStatus = Literal["complete", "partial", "suspected_gaps", "failed"]
_VALID_QUALITY_STATUSES: frozenset[str] = frozenset(
    {"complete", "partial", "suspected_gaps", "failed"}
)

_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
_HEX64_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_DATASET_ID_NAMESPACE = uuid.NAMESPACE_URL
_COVERAGE_FIELDS = ("coverage_start", "coverage_end")


def derive_dataset_id(
    *,
    source: str,
    exchange: ExchangeId,
    market_type: MarketType,
    symbols: Sequence[str],
    intervals: Sequence[str],
    coverage_start: datetime,
    coverage_end: datetime,
    schema_version: int = DATASET_SCHEMA_VERSION,
) -> str:
    """Return the deterministic UUID5 identity of a dataset slice.

    Identity inputs are canonicalized before hashing, so direct callers get
    the same ID for equivalent content: symbols are normalized to canonical
    ``BASE/QUOTE`` strings (``("btc/usdt", "BTC/USDT")`` and
    ``("BTC/USDT",)`` are identical), intervals are stripped and sorted,
    aware timestamps are normalized to UTC, and naive timestamps are rejected.
    Only identity fields participate; checksum, record count, and quality
    status do not, so a corrected re-export keeps its identity.
    """
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise ValueError("schema_version must be an integer")
    exchange = _require_exchange(exchange)
    market_type = _require_market_type(market_type)
    canonical_symbols = _normalize_symbols(symbols)
    canonical_intervals = _normalize_intervals(intervals)
    start = _require_utc("coverage_start", coverage_start)
    end = _require_utc("coverage_end", coverage_end)
    identity = "|".join(
        (
            source,
            exchange,
            market_type,
            "|".join(canonical_symbols),
            "|".join(canonical_intervals),
            start.isoformat(),
            end.isoformat(),
            str(schema_version),
        )
    )
    return str(uuid.uuid5(_DATASET_ID_NAMESPACE, identity))


def compute_dataset_checksum(record_payloads: bytes) -> str:
    """Return the deterministic SHA-256 digest of dataset record payloads."""
    return hashlib.sha256(record_payloads).hexdigest()


@dataclass(frozen=True, kw_only=True)
class DatasetMetadata:
    """Immutable, JSON-serializable metadata describing one dataset slice.

    The document is self-describing: coverage uses half-open UTC boundaries,
    symbols use canonical ``BASE/QUOTE`` strings, and every value round-trips
    exactly through :meth:`to_json`/:meth:`from_json`.  It contains public
    market-data facts only: never API keys, account data, or order data.
    """

    dataset_id: str
    dataset_version: str
    source: str
    exchange: ExchangeId
    market_type: MarketType
    symbols: tuple[str, ...] = field(default_factory=tuple)
    intervals: tuple[str, ...] = field(default_factory=tuple)
    coverage_start: datetime
    coverage_end: datetime
    schema_version: int = DATASET_SCHEMA_VERSION
    checksum: str
    record_count: int
    quality_status: DatasetQualityStatus

    _COVERAGE_FIELDS: ClassVar[tuple[str, str]] = _COVERAGE_FIELDS

    def __post_init__(self) -> None:
        """Validate all invariants and normalize symbols, intervals, and UTC."""
        if not self.dataset_id:
            raise ValueError("dataset_id must be non-empty")
        object.__setattr__(self, "exchange", _require_exchange(self.exchange))
        object.__setattr__(self, "market_type", _require_market_type(self.market_type))
        if not _VERSION_PATTERN.match(self.dataset_version):
            raise ValueError(
                f"dataset_version must match MAJOR.MINOR.PATCH (got {self.dataset_version!r})"
            )
        if not self.source.strip():
            raise ValueError("source must be non-empty")
        if self.schema_version not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(
                f"unsupported schema version {self.schema_version}; "
                f"supported: {sorted(SUPPORTED_SCHEMA_VERSIONS)}"
            )
        if not _HEX64_PATTERN.match(self.checksum):
            raise ValueError("checksum must be a 64-character lowercase SHA-256 hex digest")
        if self.record_count < 0:
            raise ValueError("record_count must be non-negative")
        if self.quality_status not in _VALID_QUALITY_STATUSES:
            raise ValueError(
                f"invalid quality status {self.quality_status!r}; "
                f"valid: {sorted(_VALID_QUALITY_STATUSES)}"
            )
        object.__setattr__(self, "symbols", _normalize_symbols(self.symbols))
        object.__setattr__(self, "intervals", _normalize_intervals(self.intervals))
        start = _require_utc("coverage_start", self.coverage_start)
        end = _require_utc("coverage_end", self.coverage_end)
        if end <= start:
            raise ValueError("coverage_end must follow coverage_start")
        object.__setattr__(self, "coverage_start", start)
        object.__setattr__(self, "coverage_end", end)

    @classmethod
    def create(
        cls,
        *,
        dataset_version: str,
        source: str,
        exchange: ExchangeId,
        market_type: MarketType,
        symbols: Sequence[str],
        intervals: Sequence[str],
        coverage_start: datetime,
        coverage_end: datetime,
        checksum: str,
        record_count: int,
        quality_status: DatasetQualityStatus,
        schema_version: int = DATASET_SCHEMA_VERSION,
    ) -> DatasetMetadata:
        """Build metadata with a deterministic identity from content fields.

        The identity is derived from the canonical (normalized) symbols and
        intervals, so ``create(symbols=("btc/usdt", ...))`` and a direct
        construction with ``symbols=("BTC/USDT", ...)`` produce the same
        ``dataset_id``.
        """
        exchange = _require_exchange(exchange)
        market_type = _require_market_type(market_type)
        canonical_symbols = _normalize_symbols(symbols)
        canonical_intervals = _normalize_intervals(intervals)
        return cls(
            dataset_id=derive_dataset_id(
                source=source,
                exchange=exchange,
                market_type=market_type,
                symbols=canonical_symbols,
                intervals=canonical_intervals,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                schema_version=schema_version,
            ),
            dataset_version=dataset_version,
            source=source,
            exchange=exchange,
            market_type=market_type,
            symbols=canonical_symbols,
            intervals=canonical_intervals,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            schema_version=schema_version,
            checksum=checksum,
            record_count=record_count,
            quality_status=quality_status,
        )

    def to_record(self) -> dict[str, Any]:
        """Return the canonical JSON-safe document (no floats, all UTC)."""
        return {
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "source": self.source,
            "exchange": self.exchange,
            "market_type": self.market_type,
            "symbols": list(self.symbols),
            "intervals": list(self.intervals),
            "coverage_start": self.coverage_start.isoformat(),
            "coverage_end": self.coverage_end.isoformat(),
            "schema_version": self.schema_version,
            "checksum": self.checksum,
            "record_count": self.record_count,
            "quality_status": self.quality_status,
        }

    def to_json(self) -> str:
        """Serialize deterministically (sorted keys, compact separators)."""
        return json.dumps(self.to_record(), sort_keys=True, separators=(",", ":"))

    def metadata_checksum(self) -> str:
        """Return the SHA-256 digest of the canonical JSON metadata document."""
        return hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> DatasetMetadata:
        """Restore metadata from a canonical JSON-safe document.

        Runtime types are validated strictly: numeric fields must be real
        integers (bool and float are rejected, strings are never coerced),
        and string fields must be real strings.
        """
        try:
            return cls(
                dataset_id=_require_str("dataset_id", record["dataset_id"]),
                dataset_version=_require_str("dataset_version", record["dataset_version"]),
                source=_require_str("source", record["source"]),
                exchange=_require_exchange(record["exchange"]),
                market_type=_require_market_type(record["market_type"]),
                symbols=tuple(record["symbols"]),
                intervals=tuple(record["intervals"]),
                coverage_start=_parse_iso_utc("coverage_start", record["coverage_start"]),
                coverage_end=_parse_iso_utc("coverage_end", record["coverage_end"]),
                schema_version=_require_int("schema_version", record["schema_version"]),
                checksum=_require_str("checksum", record["checksum"]),
                record_count=_require_int("record_count", record["record_count"]),
                quality_status=cast(
                    DatasetQualityStatus, _require_str("quality_status", record["quality_status"])
                ),
            )
        except KeyError as error:
            raise ValueError(f"dataset metadata record is missing field {error.args[0]}") from error
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid dataset metadata record: {error}") from error

    @classmethod
    def from_json(cls, text: str) -> DatasetMetadata:
        """Restore metadata from its deterministic JSON serialization."""
        try:
            record = json.loads(text)
        except json.JSONDecodeError as error:
            raise ValueError(f"dataset metadata JSON is invalid: {error}") from error
        if not isinstance(record, dict):
            raise ValueError("dataset metadata JSON must be an object")
        return cls.from_record(record)


def _normalize_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    if not symbols:
        raise ValueError("symbols must be non-empty")
    canonical: list[str] = []
    for raw in symbols:
        if not raw or not isinstance(raw, str):
            raise ValueError("symbols must be canonical BASE/QUOTE strings")
        try:
            base, quote = raw.split("/", 1)
            canonical.append(MarketSymbol(base, quote).canonical)
        except (ValueError, AttributeError) as error:
            raise ValueError(f"invalid symbol {raw!r}: must be canonical BASE/QUOTE") from error
    deduplicated = tuple(sorted(set(canonical)))
    if not deduplicated:
        raise ValueError("symbols must be non-empty")
    return deduplicated


def _normalize_intervals(intervals: Sequence[str]) -> tuple[str, ...]:
    if not intervals:
        raise ValueError("intervals must be non-empty")
    cleaned: list[str] = []
    for raw in intervals:
        if not isinstance(raw, str):
            raise ValueError("intervals must be strings")
        stripped = raw.strip()
        if stripped:
            cleaned.append(stripped)
    deduplicated = tuple(sorted(set(cleaned)))
    if not deduplicated:
        raise ValueError("intervals must be non-empty")
    return deduplicated


def _require_str(name: str, value: object) -> str:
    """Require a real string, rejecting silent coercion of other types."""
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _require_int(name: str, value: object) -> int:
    """Require a real integer, rejecting bool, float, and string coercion."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _require_exchange(value: object) -> ExchangeId:
    """Require the exact public exchange identifier."""
    text = _require_str("exchange", value)
    if text != "binance":
        raise ValueError("exchange must be 'binance'")
    return cast(ExchangeId, text)


def _require_market_type(value: object) -> MarketType:
    """Require the exact public market type identifier."""
    text = _require_str("market_type", value)
    if text != "spot":
        raise ValueError("market_type must be 'spot'")
    return cast(MarketType, text)


def _require_utc(name: str, value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _parse_iso_utc(name: str, value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be an ISO-8601 UTC string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO-8601 UTC string") from error
    return _require_utc(name, parsed)
