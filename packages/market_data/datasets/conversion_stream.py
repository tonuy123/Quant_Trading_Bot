"""DATA-005 B-2B slice 1: bounded raw-to-research stream conversion.

The caller owns every stream and all filesystem publication concerns.  This
module reads one bounded logical raw line at a time and writes canonical
research or sanitized failure NDJSON without seeking, flushing, or closing.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import BinaryIO, Literal, cast

from packages.market_data.datasets.converter import (
    ConversionFailure,
    RawConversionContext,
    convert_raw_archive_line,
)

ConversionStatus = Literal["success", "partial"]
_StreamOperation = Literal["read_raw", "write_research", "write_failure"]
_WriteOperation = Literal["write_research", "write_failure"]

_STREAM_ERROR_MESSAGES: Mapping[_StreamOperation, str] = MappingProxyType(
    {
        "read_raw": "raw archive stream read failed",
        "write_research": "research stream write failed",
        "write_failure": "conversion failure stream write failed",
    }
)
_STREAM_OPERATIONS: frozenset[str] = frozenset(_STREAM_ERROR_MESSAGES)
_LOWERCASE_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_COUNTER_FIELDS: tuple[str, ...] = (
    "lines_seen",
    "records_written",
    "records_quarantined",
    "research_bytes",
    "failure_bytes",
)

_RAW_METHOD_ERROR = "raw_stream must provide callable readline(size)"
_RESEARCH_METHOD_ERROR = "research_stream must provide callable write(bytes)"
_FAILURE_METHOD_ERROR = "failure_stream must provide callable write(bytes)"
_RAW_RESULT_ERROR = "raw_stream.readline(size) must return bounded bytes"


class ConversionStreamError(RuntimeError):
    """Sanitized operational failure at one stream boundary."""

    operation: _StreamOperation

    def __init__(self, operation: _StreamOperation) -> None:
        if not isinstance(operation, str) or operation not in _STREAM_OPERATIONS:
            raise ValueError("operation must be a supported stream operation")
        self.operation = operation
        super().__init__(_STREAM_ERROR_MESSAGES[self.operation])


def _safe_basename(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or any(forbidden in value for forbidden in ("/", "\\", "\0", "\r", "\n"))
    ):
        raise ValueError("file must be a non-empty safe basename")
    return value


def _non_negative_integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _lowercase_sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _LOWERCASE_SHA256.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 hexadecimal digest")
    return value


@dataclass(frozen=True, kw_only=True)
class StreamConversionReport:
    """Immutable exact-byte summary of one completed stream conversion."""

    file: str
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
        _safe_basename(self.file)
        for field in _COUNTER_FIELDS:
            _non_negative_integer(getattr(self, field), field=field)
        _lowercase_sha256(self.research_sha256, field="research_sha256")
        _lowercase_sha256(self.failure_sha256, field="failure_sha256")

        if self.lines_seen != self.records_written + self.records_quarantined:
            raise ValueError("lines_seen must equal written plus quarantined records")
        if not isinstance(self.status, str) or self.status not in {"success", "partial"}:
            raise ValueError("status must be a supported conversion status")
        expected_status: ConversionStatus = (
            "success" if self.records_quarantined == 0 else "partial"
        )
        if self.status != expected_status:
            raise ValueError("status must match the quarantined record count")

        if self.records_written == 0:
            if self.coverage_start_ms is not None or self.coverage_end_ms is not None:
                raise ValueError("coverage must be absent when no records were written")
            return

        coverage_start_ms = _non_negative_integer(
            self.coverage_start_ms,
            field="coverage_start_ms",
        )
        coverage_end_ms = _non_negative_integer(
            self.coverage_end_ms,
            field="coverage_end_ms",
        )
        if coverage_end_ms <= coverage_start_ms:
            raise ValueError("coverage_end_ms must be later than coverage_start_ms")


def conversion_failure_to_ndjson_line(failure: ConversionFailure) -> str:
    """Serialize one sanitized conversion failure deterministically."""
    if not isinstance(failure, ConversionFailure):
        raise ValueError("failure must be a ConversionFailure")
    record = {
        "failure_type": failure.failure_type,
        "file": failure.file,
        "line_number": failure.line_number,
        "line_preview": failure.line_preview,
        "line_sha256": failure.line_sha256,
        "reason": failure.reason,
    }
    return f"{json.dumps(record, separators=(',', ':'), sort_keys=True)}\n"


def _required_method(
    stream: object,
    *,
    method_name: str,
    error_message: str,
    operation: _StreamOperation,
) -> Callable[..., object]:
    method: object = None
    operation_failed = False
    signature_failed = False
    try:
        method = getattr(stream, method_name, None)
    except OSError:
        operation_failed = True
    except TypeError:
        signature_failed = True
    if operation_failed:
        raise ConversionStreamError(operation)
    if signature_failed:
        raise ValueError(error_message)
    if not callable(method):
        raise ValueError(error_message)
    return cast(Callable[..., object], method)


def _read_chunk(readline: Callable[..., object], size: int) -> bytes:
    chunk: object = None
    read_failed = False
    signature_failed = False
    try:
        chunk = readline(size)
    except OSError:
        read_failed = True
    except TypeError:
        signature_failed = True
    if read_failed:
        raise ConversionStreamError("read_raw")
    if signature_failed:
        raise ValueError(_RAW_METHOD_ERROR)
    if type(chunk) is not bytes or len(chunk) > size:
        raise ValueError(_RAW_RESULT_ERROR)
    return chunk


def _write_all(
    write: Callable[..., object],
    payload: bytes,
    *,
    operation: _WriteOperation,
    method_error: str,
) -> None:
    offset = 0
    chunk_size = len(payload)
    while offset < len(payload):
        chunk = payload[offset : offset + chunk_size]
        written: object = None
        write_failed = False
        signature_failed = False
        try:
            written = write(chunk)
        except OSError:
            write_failed = True
        except TypeError:
            signature_failed = True
        if write_failed:
            raise ConversionStreamError(operation)
        if signature_failed:
            raise ValueError(method_error)
        if (
            isinstance(written, bool)
            or not isinstance(written, int)
            or written <= 0
            or written > len(chunk)
        ):
            raise ConversionStreamError(operation)
        offset += written
        if written < len(chunk):
            chunk_size = written


def _oversized_failure(
    first_chunk: bytes,
    *,
    readline: Callable[..., object],
    read_size: int,
    context: RawConversionContext,
    line_number: int,
) -> ConversionFailure:
    digest = sha256()
    chunk = first_chunk
    while True:
        digest.update(chunk)
        if chunk.endswith(b"\n"):
            break
        chunk = _read_chunk(readline, read_size)
        if chunk == b"":
            break
    return ConversionFailure(
        file=context.file_name,
        line_number=line_number,
        failure_type="line_too_large",
        line_preview=None,
        line_sha256=digest.hexdigest(),
    )


def _failure_for_stream_output(failure: ConversionFailure) -> ConversionFailure:
    if failure.failure_type != "invalid_candle" or failure.line_preview is None:
        return failure
    return ConversionFailure(
        file=failure.file,
        line_number=failure.line_number,
        failure_type=failure.failure_type,
        line_preview=None,
        line_sha256=failure.line_sha256,
    )


def convert_raw_archive_stream(
    raw_stream: BinaryIO,
    research_stream: BinaryIO,
    failure_stream: BinaryIO,
    *,
    context: RawConversionContext,
) -> StreamConversionReport:
    """Convert one raw archive stream with bounded memory and exact checksums."""
    if not isinstance(context, RawConversionContext):
        raise ValueError("context must be a RawConversionContext")
    readline = _required_method(
        raw_stream,
        method_name="readline",
        error_message=_RAW_METHOD_ERROR,
        operation="read_raw",
    )
    research_write = _required_method(
        research_stream,
        method_name="write",
        error_message=_RESEARCH_METHOD_ERROR,
        operation="write_research",
    )
    failure_write = _required_method(
        failure_stream,
        method_name="write",
        error_message=_FAILURE_METHOD_ERROR,
        operation="write_failure",
    )

    read_size = context.max_line_bytes + 1
    research_digest = sha256()
    failure_digest = sha256()
    lines_seen = 0
    records_written = 0
    records_quarantined = 0
    research_bytes = 0
    failure_bytes = 0
    coverage_start_ms: int | None = None
    coverage_end_ms: int | None = None
    previous_open_time_ms: int | None = None
    failure: ConversionFailure | None

    while True:
        raw_line = _read_chunk(readline, read_size)
        if raw_line == b"":
            break
        lines_seen += 1

        if len(raw_line) > context.max_line_bytes:
            failure = _oversized_failure(
                raw_line,
                readline=readline,
                read_size=read_size,
                context=context,
                line_number=lines_seen,
            )
            result_candle = None
        else:
            result = convert_raw_archive_line(
                raw_line,
                line_number=lines_seen,
                context=context,
                previous_open_time_ms=previous_open_time_ms,
            )
            result_candle = result.candle
            failure = result.failure

        if result_candle is not None:
            research_output = result_candle.to_ndjson_line().encode("utf-8")
            _write_all(
                research_write,
                research_output,
                operation="write_research",
                method_error=_RESEARCH_METHOD_ERROR,
            )
            research_digest.update(research_output)
            research_bytes += len(research_output)
            records_written += 1
            coverage_start_ms = (
                result_candle.open_time_ms
                if coverage_start_ms is None
                else min(coverage_start_ms, result_candle.open_time_ms)
            )
            coverage_end_ms = (
                result_candle.close_time_ms
                if coverage_end_ms is None
                else max(coverage_end_ms, result_candle.close_time_ms)
            )
            previous_open_time_ms = result_candle.open_time_ms
            continue

        if failure is None:
            raise RuntimeError("conversion kernel returned an invalid result")
        failure = _failure_for_stream_output(failure)
        failure_output = conversion_failure_to_ndjson_line(failure).encode("utf-8")
        _write_all(
            failure_write,
            failure_output,
            operation="write_failure",
            method_error=_FAILURE_METHOD_ERROR,
        )
        failure_digest.update(failure_output)
        failure_bytes += len(failure_output)
        records_quarantined += 1

    status: ConversionStatus = "success" if records_quarantined == 0 else "partial"
    return StreamConversionReport(
        file=context.file_name,
        lines_seen=lines_seen,
        records_written=records_written,
        records_quarantined=records_quarantined,
        coverage_start_ms=coverage_start_ms,
        coverage_end_ms=coverage_end_ms,
        research_sha256=research_digest.hexdigest(),
        failure_sha256=failure_digest.hexdigest(),
        research_bytes=research_bytes,
        failure_bytes=failure_bytes,
        status=status,
    )
