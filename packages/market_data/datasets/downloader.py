"""DATA-002: deterministic historical public Binance Spot kline downloader.

Scope and boundaries (documented, do not silently change):

* Public Binance Spot market data only. No API key, no private endpoint, no
  account data, no order/execution/strategy/risk/portfolio logic, no live
  trading.
* Downloads only fully closed klines in a half-open UTC range ``[start, end)``.
  An in-progress/current candle is never included: a kline is kept only when
  its exclusive interval end (fixed duration, or the next UTC calendar month
  for ``1M``) is ``<= end`` and ``<= server time``.
* Prices, quantities, and timestamps never become floats. Binance payload
  values are preserved exactly as returned (strings for numerics, integer
  epoch milliseconds for times), and epoch conversions use integer arithmetic.
* Records are stored as deterministic NDJSON lines, one JSON object per
  closed kline, ordered by open time. Output files are written to a temporary
  file and atomically renamed; resume re-reads existing records so re-running
  never duplicates or silently overwrites a completed dataset.
* The manifest carries dataset identity (via ``derive_dataset_id``), the
  requested and actual UTC range, record counts, output files, downloader and
  schema versions, completion status, and sanitized failure information. It
  deliberately does not claim a checksum: DATA-003 owns checksum verification.

This module reuses the public transport protocol, the rate-limit coordinator,
the symbol mapper, and the dataset identity derivation already owned by the
Market Data layer. It does not create a competing symbol, interval,
timestamp, or checksum schema. Interval semantics (including the calendar
month ``1M``) come from the shared
``packages.domain.enums.timeframe`` helper.
"""

from __future__ import annotations

import asyncio
import json
import os
import urllib.parse
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from packages.domain.enums.timeframe import SUPPORTED_INTERVALS, interval_boundary_after
from packages.market_data.adapters.binance_rest import (
    BINANCE_PUBLIC_REST_URL,
    HttpResponse,
    PublicHttpTransport,
    UrllibPublicHttpTransport,
)
from packages.market_data.adapters.binance_ws import BinanceSymbolMapper
from packages.market_data.adapters.value_types import MarketSymbol
from packages.market_data.datasets.metadata import (
    DATASET_SCHEMA_VERSION,
    derive_dataset_id,
)
from packages.market_data.services.rate_limit import (
    BinanceRateLimitCoordinator,
    RateLimitBlockedError,
)

# PATCH correction: strict raw close-boundary validation and calendar-month
# semantics. A producer using the former fixed-30-day behavior must not be
# treated as byte/semantic compatible with this output.
DOWNLOADER_VERSION = "1.0.1"
DATASET_DOWNLOAD_VERSION = "1.0.1"
SOURCE = "binance_public_rest"
KLINES_ENDPOINT = "/api/v3/klines"
SERVER_TIME_ENDPOINT = "/api/v3/time"
KLINES_WEIGHT = 2
SERVER_TIME_WEIGHT = 1
MANIFEST_FILE = "manifest.json"
MAX_REQUEST_ATTEMPTS = 3
_UNKNOWN_SYMBOL = "*"
_UNKNOWN_INTERVAL = "*"

DownloadErrorType = Literal[
    "rate_limited",
    "ip_banned",
    "forbidden",
    "request_error",
    "server_error",
    "network_timeout",
    "malformed_response",
    "pagination_failure",
    "write_failure",
    "server_time_failure",
]
_VALID_ERROR_TYPES: frozenset[str] = frozenset(
    {
        "rate_limited",
        "ip_banned",
        "forbidden",
        "request_error",
        "server_error",
        "network_timeout",
        "malformed_response",
        "pagination_failure",
        "write_failure",
        "server_time_failure",
    }
)
CompletionStatus = Literal["complete", "incomplete"]

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


# =============================================================================
# Value types
# =============================================================================


@dataclass(frozen=True, kw_only=True)
class HistoricalDownloadRequest:
    """One deterministic historical public-market-data download request."""

    symbols: tuple[str, ...]
    intervals: tuple[str, ...]
    start: datetime
    end: datetime
    output_dir: str
    page_limit: int = 1000
    resume: bool = False
    base_url: str = BINANCE_PUBLIC_REST_URL

    def __post_init__(self) -> None:
        symbols = _normalize_symbols(self.symbols)
        intervals = _normalize_intervals(self.intervals)
        start = _require_utc("start", self.start)
        end = _require_utc("end", self.end)
        if end <= start:
            raise ValueError("end must follow start (half-open [start, end) range)")
        if isinstance(self.page_limit, bool) or not isinstance(self.page_limit, int):
            raise ValueError("page_limit must be an integer")
        if not 1 <= self.page_limit <= 1000:
            raise ValueError("page_limit must be between 1 and 1000")
        _validate_public_base_url(self.base_url)
        output_path = Path(self.output_dir)
        if output_path.exists() and not output_path.is_dir():
            raise ValueError(f"output path {self.output_dir!r} exists and is not a directory")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "intervals", intervals)
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))


@dataclass(frozen=True, kw_only=True)
class DownloadFailure(RuntimeError):
    """Sanitized failure report for one symbol/interval request range."""

    symbol: str
    interval: str
    range_start: datetime
    range_end: datetime
    endpoint: str
    error_type: DownloadErrorType
    message: str
    attempts: int

    def __str__(self) -> str:
        return (
            f"{self.error_type} {self.symbol}/{self.interval} "
            f"[{self.range_start.isoformat()}..{self.range_end.isoformat()}] "
            f"{self.endpoint}: {self.message}"
        )


@dataclass(frozen=True, kw_only=True)
class OutputFileInfo:
    """One completed output file inside a dataset directory."""

    name: str
    records: int
    range_start: datetime
    range_end: datetime


@dataclass(frozen=True, kw_only=True)
class DownloadManifest:
    """Deterministic manifest for one downloaded dataset slice."""

    dataset_id: str
    dataset_version: str
    downloader_version: str
    schema_version: int
    source: str
    exchange: str
    market_type: str
    symbols: tuple[str, ...]
    intervals: tuple[str, ...]
    requested_start: datetime
    requested_end: datetime
    actual_start: datetime | None
    actual_end: datetime | None
    record_count: int
    files: tuple[OutputFileInfo, ...]
    completion_status: CompletionStatus
    failure: DownloadFailure | None
    page_limit: int
    resume: bool
    server_time: datetime | None

    def to_record(self) -> dict[str, Any]:
        """Return the canonical JSON-safe manifest document (no floats)."""
        return {
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "downloader_version": self.downloader_version,
            "schema_version": self.schema_version,
            "source": self.source,
            "exchange": self.exchange,
            "market_type": self.market_type,
            "symbols": list(self.symbols),
            "intervals": list(self.intervals),
            "requested_start": self.requested_start.isoformat(),
            "requested_end": self.requested_end.isoformat(),
            "actual_start": self.actual_start.isoformat() if self.actual_start else None,
            "actual_end": self.actual_end.isoformat() if self.actual_end else None,
            "record_count": self.record_count,
            "files": [
                {
                    "name": file_info.name,
                    "records": file_info.records,
                    "range_start": file_info.range_start.isoformat(),
                    "range_end": file_info.range_end.isoformat(),
                }
                for file_info in self.files
            ],
            "completion_status": self.completion_status,
            "failure": _failure_to_record(self.failure),
            "page_limit": self.page_limit,
            "resume": self.resume,
            "server_time": self.server_time.isoformat() if self.server_time else None,
        }

    def to_json(self) -> str:
        """Serialize deterministically (sorted keys, compact separators)."""
        return json.dumps(self.to_record(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> DownloadManifest:
        """Restore a manifest from its canonical JSON-safe document."""
        try:
            files: list[OutputFileInfo] = []
            for item in record["files"]:
                if not isinstance(item, dict):
                    raise ValueError("manifest file entry must be an object")
                files.append(
                    OutputFileInfo(
                        name=_require_str("file name", item["name"]),
                        records=_require_int("file records", item["records"]),
                        range_start=_parse_iso_utc("file range_start", item["range_start"]),
                        range_end=_parse_iso_utc("file range_end", item["range_end"]),
                    )
                )
            failure = _failure_from_record(record["failure"])
            return cls(
                dataset_id=_require_str("dataset_id", record["dataset_id"]),
                dataset_version=_require_str("dataset_version", record["dataset_version"]),
                downloader_version=_require_str("downloader_version", record["downloader_version"]),
                schema_version=_require_int("schema_version", record["schema_version"]),
                source=_require_str("source", record["source"]),
                exchange=_require_str("exchange", record["exchange"]),
                market_type=_require_str("market_type", record["market_type"]),
                symbols=tuple(record["symbols"]),
                intervals=tuple(record["intervals"]),
                requested_start=_parse_iso_utc("requested_start", record["requested_start"]),
                requested_end=_parse_iso_utc("requested_end", record["requested_end"]),
                actual_start=_parse_iso_utc_optional("actual_start", record["actual_start"]),
                actual_end=_parse_iso_utc_optional("actual_end", record["actual_end"]),
                record_count=_require_int("record_count", record["record_count"]),
                files=tuple(files),
                completion_status=_require_completion_status(record["completion_status"]),
                failure=failure,
                page_limit=_require_int("page_limit", record["page_limit"]),
                resume=_require_bool("resume", record["resume"]),
                server_time=_parse_iso_utc_optional("server_time", record["server_time"]),
            )
        except KeyError as error:
            raise ValueError(f"download manifest is missing field {error.args[0]}") from error
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid download manifest: {error}") from error

    @classmethod
    def from_json(cls, text: str) -> DownloadManifest:
        """Restore a manifest from its deterministic JSON serialization."""
        try:
            record = json.loads(text)
        except json.JSONDecodeError as error:
            raise ValueError(f"download manifest JSON is invalid: {error}") from error
        if not isinstance(record, dict):
            raise ValueError("download manifest JSON must be an object")
        return cls.from_record(record)


def _failure_to_record(failure: DownloadFailure | None) -> dict[str, Any] | None:
    if failure is None:
        return None
    return {
        "symbol": failure.symbol,
        "interval": failure.interval,
        "range_start": failure.range_start.isoformat(),
        "range_end": failure.range_end.isoformat(),
        "endpoint": failure.endpoint,
        "error_type": failure.error_type,
        "message": failure.message,
        "attempts": failure.attempts,
    }


def _failure_from_record(record: Any) -> DownloadFailure | None:
    if record is None:
        return None
    if not isinstance(record, dict):
        raise ValueError("manifest failure must be an object or null")
    error_type = _require_str("failure error_type", record["error_type"])
    if error_type not in _VALID_ERROR_TYPES:
        raise ValueError(f"unsupported failure error type: {error_type}")
    return DownloadFailure(
        symbol=_require_str("failure symbol", record["symbol"]),
        interval=_require_str("failure interval", record["interval"]),
        range_start=_parse_iso_utc("failure range_start", record["range_start"]),
        range_end=_parse_iso_utc("failure range_end", record["range_end"]),
        endpoint=_require_str("failure endpoint", record["endpoint"]),
        error_type=cast(DownloadErrorType, error_type),
        message=_require_str("failure message", record["message"]),
        attempts=_require_int("failure attempts", record["attempts"]),
    )


# =============================================================================
# Downloader
# =============================================================================


class HistoricalDownloader:
    """Deterministic public Binance Spot historical kline downloader.

    The transport, rate-limit coordinator, clock, and sleeper are all
    injectable so tests stay network-free and deterministic. The downloader
    never holds credentials and never sends a request outside the public
    base URL validated by :class:`HistoricalDownloadRequest`.
    """

    def __init__(
        self,
        *,
        transport: PublicHttpTransport | None = None,
        limiter: BinanceRateLimitCoordinator | None = None,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], Awaitable[None]] | None = None,
        max_attempts: int = MAX_REQUEST_ATTEMPTS,
    ) -> None:
        self._transport = transport or UrllibPublicHttpTransport()
        self._limiter = limiter or BinanceRateLimitCoordinator()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper or asyncio.sleep
        self._max_attempts = max_attempts
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")

    async def download(self, request: HistoricalDownloadRequest) -> DownloadManifest:
        """Download the requested public dataset slice and return its manifest.

        A previously completed manifest with the same identity is returned
        unchanged (deterministic rerun, never a silent overwrite). A failed
        run always leaves an explicit ``incomplete`` manifest with sanitized
        failure information.
        """
        output_dir = Path(request.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = output_dir / MANIFEST_FILE
        existing_manifest = self._read_existing_manifest(manifest_path)
        if existing_manifest is not None and existing_manifest.completion_status == "complete":
            if existing_manifest.dataset_id != self._derive_id(request):
                raise ValueError(
                    "output directory contains a different completed dataset; "
                    "choose another output directory"
                )
            _require_current_manifest_versions(existing_manifest)
            return existing_manifest
        if existing_manifest is not None and request.resume:
            if existing_manifest.dataset_id != self._derive_id(request):
                raise ValueError(
                    "output directory contains an incomplete dataset with a different "
                    "identity; resume cannot merge it"
                )
            _require_current_manifest_versions(existing_manifest)

        try:
            server_time = await self._fetch_server_time(request)
        except DownloadFailure as failure:
            manifest = self._build_manifest(
                request=request,
                files=(),
                completion_status="incomplete",
                failure=failure,
                server_time=None,
            )
            self._write_manifest_atomic(manifest_path, manifest)
            return manifest

        end_ms = _to_epoch_ms(request.end)
        server_ms = _to_epoch_ms(server_time)
        collected: dict[tuple[str, str], dict[int, list[Any]]] = {}
        for symbol in request.symbols:
            for interval in request.intervals:
                file_path = output_dir / _output_file_name(symbol, interval)
                existing_rows = _read_kline_file(file_path, interval) if request.resume else {}
                start_ms = _max_ms(
                    _to_epoch_ms(request.start),
                    _watermark_ms(existing_rows, interval),
                )
                try:
                    rows = await self._download_one(
                        request=request,
                        symbol=symbol,
                        interval=interval,
                        start_ms=start_ms,
                        end_ms=end_ms,
                        server_ms=server_ms,
                        existing_rows=existing_rows,
                    )
                except DownloadFailure as failure:
                    manifest = self._build_manifest(
                        request=request,
                        files=_output_files(collected),
                        completion_status="incomplete",
                        failure=failure,
                        server_time=server_time,
                    )
                    self._write_manifest_atomic(manifest_path, manifest)
                    return manifest
                if rows:
                    try:
                        self._write_kline_file_atomic(file_path, symbol, interval, rows)
                    except DownloadFailure as failure:
                        manifest = self._build_manifest(
                            request=request,
                            files=_output_files(collected),
                            completion_status="incomplete",
                            failure=failure,
                            server_time=server_time,
                        )
                        self._write_manifest_atomic(manifest_path, manifest)
                        return manifest
                    collected[(symbol, interval)] = rows
                manifest = self._build_manifest(
                    request=request,
                    files=_output_files(collected),
                    completion_status="incomplete",
                    failure=None,
                    server_time=server_time,
                )
                self._write_manifest_atomic(manifest_path, manifest)

        files = _output_files(collected)
        manifest = self._build_manifest(
            request=request,
            files=files,
            completion_status="complete",
            failure=None,
            server_time=server_time,
        )
        self._write_manifest_atomic(manifest_path, manifest)
        return manifest

    async def _fetch_server_time(self, request: HistoricalDownloadRequest) -> datetime:
        try:
            response = await self._request_once(
                base_url=request.base_url,
                endpoint=SERVER_TIME_ENDPOINT,
                params={},
                declared_weight=SERVER_TIME_WEIGHT,
                attempts_left=self._max_attempts,
            )
        except DownloadFailure as error:
            raise DownloadFailure(
                symbol=_UNKNOWN_SYMBOL,
                interval=_UNKNOWN_INTERVAL,
                range_start=request.start,
                range_end=request.end,
                endpoint=SERVER_TIME_ENDPOINT,
                error_type="server_time_failure",
                message="public server time unavailable",
                attempts=error.attempts,
            ) from error
        try:
            payload = json.loads(response.body.decode("utf-8"))
            value = payload.get("serverTime")
        except (UnicodeDecodeError, json.JSONDecodeError, AttributeError) as error:
            raise DownloadFailure(
                symbol=_UNKNOWN_SYMBOL,
                interval=_UNKNOWN_INTERVAL,
                range_start=request.start,
                range_end=request.end,
                endpoint=SERVER_TIME_ENDPOINT,
                error_type="server_time_failure",
                message="public server time response is malformed",
                attempts=1,
            ) from error
        if not isinstance(payload, dict) or isinstance(value, bool) or not isinstance(value, int):
            raise DownloadFailure(
                symbol=_UNKNOWN_SYMBOL,
                interval=_UNKNOWN_INTERVAL,
                range_start=request.start,
                range_end=request.end,
                endpoint=SERVER_TIME_ENDPOINT,
                error_type="server_time_failure",
                message="public server time response is malformed",
                attempts=1,
            )
        return _from_epoch_ms(value)

    async def _request_once(
        self,
        *,
        base_url: str,
        endpoint: str,
        params: Mapping[str, str],
        declared_weight: int,
        attempts_left: int,
        symbol: str = _UNKNOWN_SYMBOL,
        interval: str = _UNKNOWN_INTERVAL,
        range_start: datetime | None = None,
        range_end: datetime | None = None,
    ) -> HttpResponse:
        """Send one request with bounded retry for 429/418/timeout only."""
        failure_range_start = range_start or _utc_now(self._clock)
        failure_range_end = range_end or _utc_now(self._clock)
        attempts = 0
        while True:
            attempts += 1
            try:
                await self._limiter.acquire(endpoint, declared_weight)
            except RateLimitBlockedError as error:
                await self._wait_until(error.blocked_until)
                continue
            try:
                response = await self._transport.get(f"{base_url}{endpoint}", params)
            except (ConnectionError, TimeoutError) as error:
                if attempts >= attempts_left:
                    raise DownloadFailure(
                        symbol=symbol,
                        interval=interval,
                        range_start=failure_range_start,
                        range_end=failure_range_end,
                        endpoint=endpoint,
                        error_type="network_timeout",
                        message="network failure while fetching public market data",
                        attempts=attempts,
                    ) from error
                await self._sleeper(0.25 * attempts)
                continue
            self._limiter.observe_response(
                status_code=response.status_code, headers=response.headers
            )
            if response.status_code == 200:
                return response
            if response.status_code in {429, 418}:
                if attempts >= attempts_left:
                    error_type: DownloadErrorType = (
                        "rate_limited" if response.status_code == 429 else "ip_banned"
                    )
                    label = "rate limit" if response.status_code == 429 else "IP ban"
                    raise DownloadFailure(
                        symbol=symbol,
                        interval=interval,
                        range_start=failure_range_start,
                        range_end=failure_range_end,
                        endpoint=endpoint,
                        error_type=error_type,
                        message=f"{label} exhausted after bounded retries",
                        attempts=attempts,
                    )
                await self._wait_until(self._limiter.snapshot().blocked_until)
                continue
            if response.status_code == 403:
                raise DownloadFailure(
                    symbol=symbol,
                    interval=interval,
                    range_start=failure_range_start,
                    range_end=failure_range_end,
                    endpoint=endpoint,
                    error_type="forbidden",
                    message="public endpoint returned HTTP 403 (no blind retry)",
                    attempts=attempts,
                )
            if 400 <= response.status_code < 500:
                raise DownloadFailure(
                    symbol=symbol,
                    interval=interval,
                    range_start=failure_range_start,
                    range_end=failure_range_end,
                    endpoint=endpoint,
                    error_type="request_error",
                    message=f"public endpoint returned HTTP {response.status_code}",
                    attempts=attempts,
                )
            raise DownloadFailure(
                symbol=symbol,
                interval=interval,
                range_start=failure_range_start,
                range_end=failure_range_end,
                endpoint=endpoint,
                error_type="server_error",
                message=f"public endpoint returned HTTP {response.status_code} (no blind retry)",
                attempts=attempts,
            )

    async def _download_one(
        self,
        *,
        request: HistoricalDownloadRequest,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        server_ms: int,
        existing_rows: Mapping[int, list[Any]],
    ) -> dict[int, list[Any]]:
        """Fetch one symbol/interval slice and merge it with existing rows."""
        rows = dict(existing_rows)
        market_symbol = MarketSymbol(*symbol.split("/", 1))
        binance_symbol = BinanceSymbolMapper.to_binance_symbol(market_symbol, "spot")
        cursor_ms = start_ms
        while cursor_ms < end_ms:
            response = await self._request_once(
                base_url=request.base_url,
                endpoint=KLINES_ENDPOINT,
                params={
                    "symbol": binance_symbol,
                    "interval": interval,
                    "startTime": str(cursor_ms),
                    "endTime": str(end_ms - 1),
                    "limit": str(request.page_limit),
                },
                declared_weight=KLINES_WEIGHT,
                attempts_left=self._max_attempts,
                symbol=symbol,
                interval=interval,
                range_start=_from_epoch_ms(cursor_ms),
                range_end=_from_epoch_ms(end_ms),
            )
            payload = self._decode_klines(response, symbol, interval, end_ms, server_ms)
            if not payload:
                break
            max_open_ms: int | None = None
            for row in payload:
                open_ms = cast(int, row[0])
                if open_ms < cursor_ms:
                    continue
                max_open_ms = open_ms if max_open_ms is None else max(open_ms, max_open_ms)
                rows.setdefault(open_ms, row)
            if max_open_ms is None:
                raise DownloadFailure(
                    symbol=symbol,
                    interval=interval,
                    range_start=_from_epoch_ms(start_ms),
                    range_end=_from_epoch_ms(end_ms),
                    endpoint=KLINES_ENDPOINT,
                    error_type="pagination_failure",
                    message="page contained no usable klines and no progress",
                    attempts=1,
                )
            next_cursor = interval_boundary_after(max_open_ms, interval)
            if next_cursor <= cursor_ms:
                raise DownloadFailure(
                    symbol=symbol,
                    interval=interval,
                    range_start=_from_epoch_ms(start_ms),
                    range_end=_from_epoch_ms(end_ms),
                    endpoint=KLINES_ENDPOINT,
                    error_type="pagination_failure",
                    message="pagination made no forward progress",
                    attempts=1,
                )
            cursor_ms = next_cursor
            if len(payload) < request.page_limit:
                break
        return rows

    def _decode_klines(
        self,
        response: HttpResponse,
        symbol: str,
        interval: str,
        end_ms: int,
        server_ms: int,
    ) -> list[list[Any]]:
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise self._malformed(symbol, interval, end_ms) from error
        if not isinstance(payload, list):
            raise self._malformed(symbol, interval, end_ms)
        closed: list[list[Any]] = []
        for row in payload:
            try:
                _open_ms, close_ms = _validated_kline_boundary(row, interval)
            except ValueError:
                raise self._malformed(symbol, interval, end_ms) from None
            if close_ms > end_ms:
                continue
            if close_ms > server_ms:
                continue
            closed.append(row)
        return closed

    def _malformed(self, symbol: str, interval: str, end_ms: int) -> DownloadFailure:
        return DownloadFailure(
            symbol=symbol,
            interval=interval,
            range_start=_from_epoch_ms(end_ms - 1),
            range_end=_from_epoch_ms(end_ms),
            endpoint=KLINES_ENDPOINT,
            error_type="malformed_response",
            message="klines response is malformed",
            attempts=1,
        )

    def _build_manifest(
        self,
        *,
        request: HistoricalDownloadRequest,
        files: tuple[OutputFileInfo, ...],
        completion_status: CompletionStatus,
        failure: DownloadFailure | None,
        server_time: datetime | None,
    ) -> DownloadManifest:
        actual_start = files[0].range_start if files else None
        actual_end = files[-1].range_end if files else None
        return DownloadManifest(
            dataset_id=self._derive_id(request),
            dataset_version=DATASET_DOWNLOAD_VERSION,
            downloader_version=DOWNLOADER_VERSION,
            schema_version=DATASET_SCHEMA_VERSION,
            source=SOURCE,
            exchange="binance",
            market_type="spot",
            symbols=request.symbols,
            intervals=request.intervals,
            requested_start=request.start,
            requested_end=request.end,
            actual_start=actual_start,
            actual_end=actual_end,
            record_count=sum(file_info.records for file_info in files),
            files=files,
            completion_status=completion_status,
            failure=failure,
            page_limit=request.page_limit,
            resume=request.resume,
            server_time=server_time,
        )

    @staticmethod
    def _derive_id(request: HistoricalDownloadRequest) -> str:
        return derive_dataset_id(
            source=SOURCE,
            exchange="binance",
            market_type="spot",
            symbols=request.symbols,
            intervals=request.intervals,
            coverage_start=request.start,
            coverage_end=request.end,
            schema_version=DATASET_SCHEMA_VERSION,
        )

    async def _wait_until(self, blocked_until: datetime | None) -> None:
        if blocked_until is None:
            return
        remaining = (blocked_until - _utc_now(self._clock)).total_seconds()
        if remaining > 0:
            await self._sleeper(remaining)

    def _write_kline_file_atomic(
        self,
        path: Path,
        symbol: str,
        interval: str,
        rows: Mapping[int, list[Any]],
    ) -> None:
        temp_path = path.with_name(f"{path.name}.tmp")
        try:
            with open(temp_path, "w", encoding="utf-8", newline="\n") as handle:
                for open_ms in sorted(rows):
                    handle.write(_record_line(symbol, interval, open_ms, rows[open_ms]))
                    handle.write("\n")
            os.replace(temp_path, path)
        except OSError as error:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise DownloadFailure(
                symbol=symbol,
                interval=interval,
                range_start=_from_epoch_ms(min(rows) if rows else 0),
                range_end=_from_epoch_ms(max(rows) if rows else 0),
                endpoint="local_output",
                error_type="write_failure",
                message="failed to write output file atomically",
                attempts=1,
            ) from error

    def _write_manifest_atomic(self, path: Path, manifest: DownloadManifest) -> None:
        temp_path = path.with_name(f"{path.name}.tmp")
        try:
            with open(temp_path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(manifest.to_json())
                handle.write("\n")
            os.replace(temp_path, path)
        except OSError:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def _read_existing_manifest(self, path: Path) -> DownloadManifest | None:
        if not path.exists():
            return None
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as error:
            raise ValueError(f"cannot read existing manifest: {error}") from error
        return DownloadManifest.from_json(text)


# =============================================================================
# Helpers
# =============================================================================


def _validated_kline_boundary(row: object, interval: str) -> tuple[int, int]:
    """Return a validated open/exclusive-close pair for one raw Binance kline."""
    if not isinstance(row, list) or len(row) != 12:
        raise ValueError("raw kline must contain exactly 12 fields")
    open_ms = row[0]
    raw_close_ms = row[6]
    if isinstance(open_ms, bool) or not isinstance(open_ms, int):
        raise ValueError("raw kline open time must be an integer")
    if isinstance(raw_close_ms, bool) or not isinstance(raw_close_ms, int):
        raise ValueError("raw kline close time must be an integer")
    close_ms = interval_boundary_after(open_ms, interval)
    if raw_close_ms + 1 != close_ms:
        raise ValueError("raw inclusive close time does not match interval boundary")
    return open_ms, close_ms


def _record_line(symbol: str, interval: str, open_ms: int, row: list[Any]) -> str:
    row_open_ms, close_ms = _validated_kline_boundary(row, interval)
    if row_open_ms != open_ms:
        raise ValueError("raw kline open time does not match its storage key")
    record = {
        "symbol": symbol,
        "interval": interval,
        "open_time": _from_epoch_ms(open_ms).isoformat(),
        "close_time": _from_epoch_ms(close_ms).isoformat(),
        "source": SOURCE,
        "payload": row,
    }
    return json.dumps(record, separators=(",", ":"))


def _read_kline_file(path: Path, interval: str) -> dict[int, list[Any]]:
    if not path.exists():
        return {}
    rows: dict[int, list[Any]] = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(
                        f"existing output file {path.name} is malformed at line {line_number}"
                    ) from error
                if not isinstance(record, dict):
                    raise ValueError(
                        f"existing output file {path.name} is malformed at line {line_number}"
                    )
                payload = record.get("payload")
                try:
                    open_ms, _close_ms = _validated_kline_boundary(payload, interval)
                except ValueError as error:
                    raise ValueError(
                        f"existing output file {path.name} is malformed at line {line_number}"
                    ) from error
                rows[open_ms] = cast(list[Any], payload)
    except OSError as error:
        raise ValueError(f"cannot read existing output file {path.name}: {error}") from error
    return rows


def _output_file_name(symbol: str, interval: str) -> str:
    return f"{symbol.replace('/', '-')}-{interval}.jsonl"


def _output_files(
    collected: Mapping[tuple[str, str], Mapping[int, list[Any]]],
) -> tuple[OutputFileInfo, ...]:
    return tuple(
        OutputFileInfo(
            name=_output_file_name(symbol, interval),
            records=len(rows),
            range_start=_from_epoch_ms(min(rows)),
            range_end=_from_epoch_ms(interval_boundary_after(max(rows), interval)),
        )
        for (symbol, interval), rows in sorted(collected.items())
    )


def _watermark_ms(rows: Mapping[int, list[Any]], interval: str) -> int:
    if not rows:
        return 0
    return interval_boundary_after(max(rows), interval)


def _require_current_manifest_versions(manifest: DownloadManifest) -> None:
    """Reject silent reuse/resume of output produced under older semantics."""
    if (
        manifest.dataset_version != DATASET_DOWNLOAD_VERSION
        or manifest.downloader_version != DOWNLOADER_VERSION
        or manifest.schema_version != DATASET_SCHEMA_VERSION
    ):
        raise ValueError(
            "output directory contains a dataset produced by an incompatible version; "
            "choose another output directory"
        )


def _max_ms(first: int, second: int) -> int:
    return first if first >= second else second


def _normalize_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    if not symbols:
        raise ValueError("symbols must be non-empty")
    canonical: list[str] = []
    for raw in symbols:
        if not raw or not isinstance(raw, str):
            raise ValueError("symbols must be canonical BASE/QUOTE strings")
        if raw.count("/") != 1:
            raise ValueError(f"invalid symbol {raw!r}: must be canonical BASE/QUOTE")
        base, quote = raw.split("/", 1)
        canonical.append(MarketSymbol(base, quote).canonical)
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
        if not stripped:
            raise ValueError("intervals must be non-empty strings")
        if stripped not in SUPPORTED_INTERVALS:
            raise ValueError(f"unsupported interval {stripped!r}")
        cleaned.append(stripped)
    deduplicated = tuple(sorted(set(cleaned)))
    if not deduplicated:
        raise ValueError("intervals must be non-empty")
    return deduplicated


def _validate_public_base_url(base_url: str) -> None:
    if not isinstance(base_url, str) or not base_url:
        raise ValueError("base_url must be a non-empty string")
    parsed = urllib.parse.urlparse(base_url)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError("public base URL must use HTTPS")
    if any(token in base_url.lower() for token in ("apikey", "listenkey", "userdata", "auth")):
        raise ValueError("private endpoint configuration is not supported")


def _to_epoch_ms(value: datetime) -> int:
    delta = value.astimezone(UTC) - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def _from_epoch_ms(ms: int) -> datetime:
    if ms < 0:
        raise ValueError("epoch milliseconds must be non-negative")
    return _EPOCH + timedelta(milliseconds=ms)


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


def _parse_iso_utc_optional(name: str, value: object) -> datetime | None:
    if value is None:
        return None
    return _parse_iso_utc(name, value)


def _require_str(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


def _require_int(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _require_bool(name: str, value: object) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _require_completion_status(value: object) -> CompletionStatus:
    if value != "complete" and value != "incomplete":
        raise ValueError("completion_status must be 'complete' or 'incomplete'")
    return value


def _utc_now(clock: Callable[[], datetime]) -> datetime:
    now = clock()
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("clock must return UTC-aware datetime")
    return now.astimezone(UTC)
