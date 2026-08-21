"""DATA-005 B-2B slice 1: bounded binary-stream conversion tests."""

from __future__ import annotations

import ast
import inspect
import io
import json
import subprocess
import sys
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

import packages.market_data.datasets.conversion_stream as stream_module
from packages.market_data.datasets.conversion_stream import (
    ConversionStreamError,
    StreamConversionReport,
    conversion_failure_to_ndjson_line,
    convert_raw_archive_stream,
)
from packages.market_data.datasets.converter import ConversionFailure, RawConversionContext

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_OPEN_MS = 1_704_067_200_000
_CLOSE_MS = 1_704_067_260_000
_FEBRUARY_OPEN_MS = 1_706_745_600_000
_MARCH_OPEN_MS = 1_709_251_200_000
_FEBRUARY_OPEN_ISO = "2024-02-01T00:00:00+00:00"
_MARCH_OPEN_ISO = "2024-03-01T00:00:00+00:00"
_EMPTY_SHA256 = sha256(b"").hexdigest()


def _iso_from_ms(epoch_ms: int) -> str:
    return (_EPOCH + timedelta(milliseconds=epoch_ms)).isoformat()


def make_payload(*, open_ms: int = _OPEN_MS, close_ms: int = _CLOSE_MS) -> list[object]:
    return [
        open_ms,
        "50000.00",
        "50001.00",
        "49999.00",
        "50000.50",
        "1.5",
        close_ms - 1,
        "75000.75",
        100,
        "0.75",
        "37500.00",
        "0",
    ]


def fixed_line(minute: int = 0, *, ending: bytes = b"\n") -> bytes:
    open_ms = _OPEN_MS + minute * 60_000
    close_ms = open_ms + 60_000
    record = {
        "symbol": "BTC/USDT",
        "interval": "1m",
        "open_time": _iso_from_ms(open_ms),
        "close_time": _iso_from_ms(close_ms),
        "source": "binance_public_rest",
        "payload": make_payload(open_ms=open_ms, close_ms=close_ms),
    }
    return json.dumps(record, separators=(",", ":")).encode("utf-8") + ending


def monthly_line(*, ending: bytes = b"\n") -> bytes:
    record = {
        "symbol": "BTC/USDT",
        "interval": "1M",
        "open_time": _FEBRUARY_OPEN_ISO,
        "close_time": _MARCH_OPEN_ISO,
        "source": "binance_public_rest",
        "payload": make_payload(
            open_ms=_FEBRUARY_OPEN_MS,
            close_ms=_MARCH_OPEN_MS,
        ),
    }
    return json.dumps(record, separators=(",", ":")).encode("utf-8") + ending


def overbound_decimal_line(value: str) -> bytes:
    payload = make_payload()
    for index in (1, 2, 3, 4):
        payload[index] = value
    record = {
        "symbol": "BTC/USDT",
        "interval": "1m",
        "open_time": _iso_from_ms(_OPEN_MS),
        "close_time": _iso_from_ms(_CLOSE_MS),
        "source": "binance_public_rest",
        "payload": payload,
    }
    return json.dumps(record, separators=(",", ":")).encode("utf-8") + b"\n"


def make_context(**overrides: object) -> RawConversionContext:
    values: dict[str, object] = {
        "file_name": "BTC-USDT-1m.jsonl",
        "symbol": "BTC/USDT",
        "interval": "1m",
        "range_start": datetime(2024, 1, 1, tzinfo=UTC),
        "range_end": datetime(2024, 1, 2, tzinfo=UTC),
        "max_line_bytes": 1_048_576,
    }
    values.update(overrides)
    return RawConversionContext(**values)  # type: ignore[arg-type]


def convert_bytes(
    raw_bytes: bytes,
    *,
    context: RawConversionContext | None = None,
) -> tuple[StreamConversionReport, bytes, bytes]:
    research_stream = io.BytesIO()
    failure_stream = io.BytesIO()
    report = convert_raw_archive_stream(
        io.BytesIO(raw_bytes),
        research_stream,
        failure_stream,
        context=make_context() if context is None else context,
    )
    return report, research_stream.getvalue(), failure_stream.getvalue()


def ndjson_records(payload: bytes) -> list[dict[str, object]]:
    return [json.loads(line) for line in payload.splitlines()]


def make_failure(**overrides: object) -> ConversionFailure:
    values: dict[str, object] = {
        "file": "safe.jsonl",
        "line_number": 3,
        "failure_type": "malformed_json",
        "line_preview": "ordinary-preview",
        "line_sha256": "0" * 64,
    }
    values.update(overrides)
    return ConversionFailure(**values)  # type: ignore[arg-type]


def make_report(**overrides: object) -> StreamConversionReport:
    values: dict[str, object] = {
        "file": "safe.jsonl",
        "lines_seen": 0,
        "records_written": 0,
        "records_quarantined": 0,
        "coverage_start_ms": None,
        "coverage_end_ms": None,
        "research_sha256": _EMPTY_SHA256,
        "failure_sha256": _EMPTY_SHA256,
        "research_bytes": 0,
        "failure_bytes": 0,
        "status": "success",
    }
    values.update(overrides)
    return StreamConversionReport(**values)  # type: ignore[arg-type]


def assert_no_float(value: object) -> None:
    assert not isinstance(value, float)
    if isinstance(value, dict):
        for nested in value.values():
            assert_no_float(nested)
    elif isinstance(value, list):
        for nested in value:
            assert_no_float(nested)


class TrackingReader:
    def __init__(self, payload: bytes) -> None:
        self._buffer = io.BytesIO(payload)
        self.readline_sizes: list[int] = []

    def readline(self, size: int) -> bytes:
        self.readline_sizes.append(size)
        return self._buffer.readline(size)

    def read(self, _size: int = -1) -> bytes:
        raise AssertionError("unbounded read must not be used")

    def readlines(self) -> list[bytes]:
        raise AssertionError("readlines must not be used")

    def close(self) -> None:
        raise AssertionError("reader must not be closed")

    def flush(self) -> None:
        raise AssertionError("reader must not be flushed")

    def seek(self, _offset: int) -> int:
        raise AssertionError("reader must not be sought")


class TrackingWriter:
    def __init__(self) -> None:
        self.buffer = bytearray()

    def write(self, payload: bytes | memoryview) -> int:
        chunk = bytes(payload)
        self.buffer.extend(chunk)
        return len(chunk)

    def getvalue(self) -> bytes:
        return bytes(self.buffer)

    def close(self) -> None:
        raise AssertionError("writer must not be closed")

    def flush(self) -> None:
        raise AssertionError("writer must not be flushed")

    def seek(self, _offset: int) -> int:
        raise AssertionError("writer must not be sought")

    def truncate(self, _size: int | None = None) -> int:
        raise AssertionError("writer must not be truncated")


class ShortWriter(TrackingWriter):
    def __init__(self, limit: int) -> None:
        super().__init__()
        self.limit = limit

    def write(self, payload: bytes | memoryview) -> int:
        chunk = bytes(payload[: self.limit])
        self.buffer.extend(chunk)
        return len(chunk)


class OSErrorReader:
    def readline(self, _size: int) -> bytes:
        raise OSError("C:\\synthetic-private\\raw-api_key.jsonl")


class OSErrorWriter:
    def write(self, _payload: bytes | memoryview) -> int:
        raise OSError("C:\\synthetic-private\\output-api_key.jsonl")


class OSErrorMethodDescriptor:
    @property
    def readline(self) -> object:
        raise OSError("C:\\synthetic-private\\descriptor-api_key.jsonl")

    @property
    def write(self) -> object:
        raise OSError("C:\\synthetic-private\\descriptor-api_key.jsonl")


class SensitiveTypeErrorReader:
    def readline(self, _size: int) -> bytes:
        raise TypeError("C:\\synthetic-private\\signature-api_key.jsonl")


class InvalidCountWriter:
    def __init__(self, result: object) -> None:
        self.result = result

    def write(self, payload: bytes | memoryview) -> object:
        return len(payload) + 1 if self.result == "impossible" else self.result


class StaticResultReader:
    def __init__(self, result: object) -> None:
        self.result = result

    def readline(self, _size: int) -> object:
        return self.result


class NoArgumentReader:
    def readline(self) -> bytes:
        return b""


class NoArgumentWriter:
    def write(self) -> int:
        return 1


class OverBoundReader:
    def readline(self, size: int) -> bytes:
        return b"x" * (size + 1)


class TestFailureSerializer:
    def test_failure_ndjson_has_exact_sorted_fields(self) -> None:
        failure = make_failure()
        line = conversion_failure_to_ndjson_line(failure)
        expected = (
            '{"failure_type":"malformed_json","file":"safe.jsonl","line_number":3,'
            '"line_preview":"ordinary-preview","line_sha256":"'
            + "0" * 64
            + '","reason":"raw archive line is not valid JSON"}\n'
        )

        assert line == expected
        assert list(json.loads(line)) == [
            "failure_type",
            "file",
            "line_number",
            "line_preview",
            "line_sha256",
            "reason",
        ]

    def test_failure_ndjson_is_deterministic_with_one_trailing_lf(self) -> None:
        failure = make_failure()
        first = conversion_failure_to_ndjson_line(failure)
        second = conversion_failure_to_ndjson_line(failure)

        assert first == second
        assert first.endswith("\n")
        assert not first.endswith("\n\n")
        assert "\n" not in first[:-1]

    def test_failure_serializer_rejects_wrong_type(self) -> None:
        with pytest.raises(ValueError, match="ConversionFailure"):
            conversion_failure_to_ndjson_line(object())  # type: ignore[arg-type]


class TestSuccessfulConversion:
    def test_valid_fixed_interval_stream_conversion(self) -> None:
        report, research, failures = convert_bytes(fixed_line())
        record = ndjson_records(research)[0]

        assert report.status == "success"
        assert (report.lines_seen, report.records_written, report.records_quarantined) == (1, 1, 0)
        assert (report.coverage_start_ms, report.coverage_end_ms) == (_OPEN_MS, _CLOSE_MS)
        assert record["open_time_ms"] == _OPEN_MS
        assert record["close_time_ms"] == _CLOSE_MS
        assert failures == b""

    def test_february_2024_calendar_month_uses_independent_boundaries(self) -> None:
        context = make_context(
            file_name="BTC-USDT-1M.jsonl",
            interval="1M",
            range_start=datetime(2024, 2, 1, tzinfo=UTC),
            range_end=datetime(2024, 3, 1, tzinfo=UTC),
        )
        report, research, failures = convert_bytes(monthly_line(), context=context)
        record = ndjson_records(research)[0]

        assert report.coverage_start_ms == 1_706_745_600_000
        assert report.coverage_end_ms == 1_709_251_200_000
        assert record["open_time"] == "2024-02-01T00:00:00+00:00"
        assert record["close_time"] == "2024-03-01T00:00:00+00:00"
        assert failures == b""

    def test_multiple_valid_lines_preserve_input_order(self) -> None:
        report, research, _ = convert_bytes(b"".join(fixed_line(minute) for minute in range(3)))

        assert report.records_written == 3
        assert [record["open_time_ms"] for record in ndjson_records(research)] == [
            _OPEN_MS,
            _OPEN_MS + 60_000,
            _OPEN_MS + 120_000,
        ]

    def test_empty_stream_is_success_with_empty_digests_and_no_coverage(self) -> None:
        report, research, failures = convert_bytes(b"")

        assert report == make_report(file="BTC-USDT-1m.jsonl")
        assert research == b""
        assert failures == b""

    def test_research_checksum_bytes_and_coverage_match_exact_output(self) -> None:
        raw = fixed_line(0) + fixed_line(2)
        report, research, failures = convert_bytes(raw)

        assert report.research_sha256 == sha256(research).hexdigest()
        assert report.failure_sha256 == sha256(failures).hexdigest()
        assert report.research_bytes == len(research)
        assert report.failure_bytes == len(failures) == 0
        assert report.coverage_start_ms == _OPEN_MS
        assert report.coverage_end_ms == _OPEN_MS + 180_000

    def test_duplicate_timestamp_is_accepted(self) -> None:
        report, research, failures = convert_bytes(fixed_line() + fixed_line())

        assert report.status == "success"
        assert report.records_written == 2
        assert len(ndjson_records(research)) == 2
        assert failures == b""

    def test_final_line_without_lf_is_processed(self) -> None:
        report, research, failures = convert_bytes(fixed_line(ending=b""))

        assert report.records_written == 1
        assert len(ndjson_records(research)) == 1
        assert failures == b""

    @pytest.mark.parametrize("ending", [b"\n", b"\r\n"])
    def test_lf_and_crlf_input_hash_matches_kernel_exact_bytes(self, ending: bytes) -> None:
        raw = b"ordinary-invalid" + ending
        report, _, failures = convert_bytes(raw)
        failure = ndjson_records(failures)[0]

        assert report.records_quarantined == 1
        assert failure["line_sha256"] == sha256(raw).hexdigest()

    def test_serialized_outputs_contain_no_float(self) -> None:
        _, research, failures = convert_bytes(fixed_line() + b"ordinary-invalid\n")

        for record in [*ndjson_records(research), *ndjson_records(failures)]:
            assert_no_float(record)


class TestQuarantineAndOrdering:
    def test_mixed_valid_and_invalid_stream_is_partial(self) -> None:
        report, research, failures = convert_bytes(
            fixed_line(0) + b"ordinary-invalid\n" + fixed_line(1)
        )

        assert report.status == "partial"
        assert (report.lines_seen, report.records_written, report.records_quarantined) == (3, 2, 1)
        assert len(ndjson_records(research)) == 2
        assert ndjson_records(failures)[0]["line_number"] == 2

    def test_all_invalid_stream_is_partial_without_coverage(self) -> None:
        report, research, failures = convert_bytes(b"bad-one\nbad-two\n")

        assert report.status == "partial"
        assert report.records_written == 0
        assert report.records_quarantined == 2
        assert report.coverage_start_ms is None
        assert report.coverage_end_ms is None
        assert research == b""
        assert len(ndjson_records(failures)) == 2

    def test_failure_checksum_and_bytes_match_exact_output(self) -> None:
        report, research, failures = convert_bytes(b"ordinary-invalid\n")

        assert research == b""
        assert report.failure_sha256 == sha256(failures).hexdigest()
        assert report.failure_bytes == len(failures)
        assert report.research_sha256 == _EMPTY_SHA256
        assert report.research_bytes == 0

    def test_decreasing_timestamp_is_quarantined_without_coverage_expansion(self) -> None:
        report, research, failures = convert_bytes(fixed_line(1) + fixed_line(0))
        failure = ndjson_records(failures)[0]

        assert report.status == "partial"
        assert (report.records_written, report.records_quarantined) == (1, 1)
        assert failure["failure_type"] == "ordering_violation"
        assert failure["line_number"] == 2
        assert report.coverage_start_ms == _OPEN_MS + 60_000
        assert report.coverage_end_ms == _OPEN_MS + 120_000
        assert len(ndjson_records(research)) == 1

    def test_previous_open_time_updates_only_after_accepted_candle(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        previous_values: list[int | None] = []
        original = stream_module.convert_raw_archive_line

        def recording_converter(
            raw_line: bytes,
            *,
            line_number: int,
            context: RawConversionContext,
            previous_open_time_ms: int | None = None,
        ) -> object:
            previous_values.append(previous_open_time_ms)
            return original(
                raw_line,
                line_number=line_number,
                context=context,
                previous_open_time_ms=previous_open_time_ms,
            )

        monkeypatch.setattr(stream_module, "convert_raw_archive_line", recording_converter)
        report, _, _ = convert_bytes(fixed_line(0) + b"ordinary-invalid\n" + fixed_line(1))

        assert report.records_written == 2
        assert previous_values == [None, _OPEN_MS, _OPEN_MS]

    def test_empty_raw_line_inside_file_is_quarantined(self) -> None:
        report, research, failures = convert_bytes(fixed_line(0) + b"\n" + fixed_line(1))

        assert (report.lines_seen, report.records_written, report.records_quarantined) == (3, 2, 1)
        assert len(ndjson_records(research)) == 2
        assert ndjson_records(failures)[0]["line_number"] == 2


class TestOversizedLines:
    def test_exact_max_line_bytes_is_accepted(self) -> None:
        raw = fixed_line(ending=b"")
        context = make_context(max_line_bytes=len(raw))
        report, research, failures = convert_bytes(raw, context=context)

        assert report.records_written == 1
        assert research
        assert failures == b""

    def test_max_line_bytes_plus_one_is_oversized(self) -> None:
        raw = fixed_line(ending=b"")
        context = make_context(max_line_bytes=len(raw) - 1)
        report, research, failures = convert_bytes(raw, context=context)

        assert report.records_written == 0
        assert report.records_quarantined == 1
        assert research == b""
        assert ndjson_records(failures)[0]["failure_type"] == "line_too_large"

    def test_oversized_line_is_consumed_as_one_logical_line(self) -> None:
        valid = fixed_line()
        max_line_bytes = len(valid) + 8
        oversized = b"x" * (max_line_bytes * 2 + 7) + b"\n"
        context = make_context(max_line_bytes=max_line_bytes)
        report, research, failures = convert_bytes(oversized + valid, context=context)

        assert (report.lines_seen, report.records_written, report.records_quarantined) == (2, 1, 1)
        assert ndjson_records(failures)[0]["line_number"] == 1
        assert len(ndjson_records(research)) == 1

    def test_oversized_line_hash_includes_all_continuations_and_newline(self) -> None:
        context = make_context(max_line_bytes=16)
        oversized = b"a" * 19 + b"b" * 23 + b"c" * 11 + b"\r\n"
        report, _, failures = convert_bytes(oversized, context=context)
        failure = ndjson_records(failures)[0]

        assert report.lines_seen == 1
        assert failure["line_sha256"] == sha256(oversized).hexdigest()

    def test_oversized_line_uses_only_bounded_readline_sizes(self) -> None:
        context = make_context(max_line_bytes=17)
        reader = TrackingReader(b"x" * 100 + b"\n")
        research = TrackingWriter()
        failures = TrackingWriter()

        report = convert_raw_archive_stream(
            reader,  # type: ignore[arg-type]
            research,  # type: ignore[arg-type]
            failures,  # type: ignore[arg-type]
            context=context,
        )

        assert report.records_quarantined == 1
        assert reader.readline_sizes
        assert set(reader.readline_sizes) == {context.max_line_bytes + 1}

    def test_oversized_secret_like_line_has_no_preview_or_raw_content(self) -> None:
        context = make_context(max_line_bytes=12)
        raw = b"api_key=synthetic-marker-" * 4 + b"\n"
        report, _, failures = convert_bytes(raw, context=context)
        record = ndjson_records(failures)[0]

        assert report.records_quarantined == 1
        assert record["line_preview"] is None
        assert b"api_key" not in failures
        assert b"synthetic-marker" not in failures


class TestStreamBoundaries:
    def test_over_bound_decimal_is_quarantined_without_output_amplification(self) -> None:
        raw_value = "1E-200000"
        raw = overbound_decimal_line(raw_value)
        context = make_context(max_line_bytes=1_024)

        report, research, failures = convert_bytes(raw, context=context)
        failure_records = ndjson_records(failures)

        assert len(raw) < context.max_line_bytes
        assert research == b""
        assert len(failure_records) == 1
        assert failure_records[0]["failure_type"] == "invalid_candle"
        assert failure_records[0]["line_preview"] is None
        assert raw_value.encode("utf-8") not in failures
        assert b"200000" not in failures
        assert report.status == "partial"
        assert (report.lines_seen, report.records_written, report.records_quarantined) == (1, 0, 1)
        assert report.research_bytes == 0
        assert report.failure_bytes == len(failures)

    def test_reader_oserror_is_sanitized(self) -> None:
        with pytest.raises(ConversionStreamError) as exc_info:
            convert_raw_archive_stream(
                OSErrorReader(),  # type: ignore[arg-type]
                io.BytesIO(),
                io.BytesIO(),
                context=make_context(),
            )

        assert exc_info.value.operation == "read_raw"
        assert str(exc_info.value) == "raw archive stream read failed"
        assert "synthetic-private" not in str(exc_info.value)
        assert "api_key" not in str(exc_info.value)
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
        assert "synthetic-private" not in repr(exc_info.value)
        assert "api_key" not in repr(vars(exc_info.value))

    @pytest.mark.parametrize("operation", ["write_research", "write_failure"])
    def test_writer_oserror_is_sanitized(self, operation: str) -> None:
        raw = fixed_line() if operation == "write_research" else b"ordinary-invalid\n"
        research: object = OSErrorWriter() if operation == "write_research" else io.BytesIO()
        failures: object = OSErrorWriter() if operation == "write_failure" else io.BytesIO()

        with pytest.raises(ConversionStreamError) as exc_info:
            convert_raw_archive_stream(
                io.BytesIO(raw),
                research,  # type: ignore[arg-type]
                failures,  # type: ignore[arg-type]
                context=make_context(),
            )

        expected_message = (
            "research stream write failed"
            if operation == "write_research"
            else "conversion failure stream write failed"
        )
        assert exc_info.value.operation == operation
        assert str(exc_info.value) == expected_message
        assert "synthetic-private" not in str(exc_info.value)
        assert "api_key" not in str(exc_info.value)
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None
        assert "synthetic-private" not in repr(exc_info.value)
        assert "api_key" not in repr(vars(exc_info.value))

    @pytest.mark.parametrize(
        "operation",
        ["read_raw", "write_research", "write_failure"],
    )
    def test_method_acquisition_oserror_is_sanitized_without_context(
        self,
        operation: str,
    ) -> None:
        descriptor = OSErrorMethodDescriptor()
        raw: object = descriptor if operation == "read_raw" else io.BytesIO(fixed_line())
        research: object = descriptor if operation == "write_research" else io.BytesIO()
        failures: object = descriptor if operation == "write_failure" else io.BytesIO()
        if operation == "write_failure":
            raw = io.BytesIO(b"ordinary-invalid\n")

        with pytest.raises(ConversionStreamError) as exc_info:
            convert_raw_archive_stream(
                raw,  # type: ignore[arg-type]
                research,  # type: ignore[arg-type]
                failures,  # type: ignore[arg-type]
                context=make_context(),
            )

        error = exc_info.value
        assert error.operation == operation
        assert error.__cause__ is None
        assert error.__context__ is None
        for public_surface in (str(error), repr(error), repr(vars(error))):
            assert "synthetic-private" not in public_surface
            assert "api_key" not in public_surface

    def test_type_error_is_sanitized_without_retained_context(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            convert_raw_archive_stream(
                SensitiveTypeErrorReader(),  # type: ignore[arg-type]
                io.BytesIO(),
                io.BytesIO(),
                context=make_context(),
            )

        error = exc_info.value
        assert str(error) == "raw_stream must provide callable readline(size)"
        assert error.__cause__ is None
        assert error.__context__ is None
        for public_surface in (str(error), repr(error), repr(vars(error))):
            assert "synthetic-private" not in public_surface
            assert "api_key" not in public_surface

    def test_short_writes_are_completed_for_both_outputs(self) -> None:
        raw = fixed_line() + b"ordinary-invalid\n"
        expected_report, expected_research, expected_failures = convert_bytes(raw)
        research = ShortWriter(limit=1)
        failures = ShortWriter(limit=2)

        report = convert_raw_archive_stream(
            io.BytesIO(raw),
            research,  # type: ignore[arg-type]
            failures,  # type: ignore[arg-type]
            context=make_context(),
        )

        assert report == expected_report
        assert research.getvalue() == expected_research
        assert failures.getvalue() == expected_failures

    @pytest.mark.parametrize("operation", ["write_research", "write_failure"])
    @pytest.mark.parametrize("write_result", [True, 0, -1, None, 1.5, "impossible"])
    def test_invalid_write_counts_fail_safely(
        self,
        operation: str,
        write_result: object,
    ) -> None:
        raw = fixed_line() if operation == "write_research" else b"ordinary-invalid\n"
        invalid_writer = InvalidCountWriter(write_result)
        research: object = invalid_writer if operation == "write_research" else io.BytesIO()
        failures: object = invalid_writer if operation == "write_failure" else io.BytesIO()

        with pytest.raises(ConversionStreamError) as exc_info:
            convert_raw_archive_stream(
                io.BytesIO(raw),
                research,  # type: ignore[arg-type]
                failures,  # type: ignore[arg-type]
                context=make_context(),
            )

        assert exc_info.value.operation == operation

    @pytest.mark.parametrize("result", ["text", bytearray(b"x"), memoryview(b"x"), None])
    def test_non_bytes_read_result_is_rejected(self, result: object) -> None:
        with pytest.raises(ValueError, match="bounded bytes"):
            convert_raw_archive_stream(
                StaticResultReader(result),  # type: ignore[arg-type]
                io.BytesIO(),
                io.BytesIO(),
                context=make_context(),
            )

    @pytest.mark.parametrize("stream_name", ["raw", "research", "failure"])
    def test_missing_stream_method_is_rejected(self, stream_name: str) -> None:
        raw: object = object() if stream_name == "raw" else io.BytesIO()
        research: object = object() if stream_name == "research" else io.BytesIO()
        failure: object = object() if stream_name == "failure" else io.BytesIO()

        with pytest.raises(ValueError, match=stream_name) as exc_info:
            convert_raw_archive_stream(
                raw,  # type: ignore[arg-type]
                research,  # type: ignore[arg-type]
                failure,  # type: ignore[arg-type]
                context=make_context(),
            )
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__context__ is None

    @pytest.mark.parametrize("stream_name", ["raw", "research", "failure"])
    def test_incompatible_stream_method_signature_is_rejected(self, stream_name: str) -> None:
        raw: object = NoArgumentReader() if stream_name == "raw" else io.BytesIO(fixed_line())
        research: object = NoArgumentWriter() if stream_name == "research" else io.BytesIO()
        failure: object = NoArgumentWriter() if stream_name == "failure" else io.BytesIO()
        if stream_name == "failure":
            raw = io.BytesIO(b"ordinary-invalid\n")

        with pytest.raises(ValueError, match=stream_name):
            convert_raw_archive_stream(
                raw,  # type: ignore[arg-type]
                research,  # type: ignore[arg-type]
                failure,  # type: ignore[arg-type]
                context=make_context(),
            )

    def test_reader_returning_more_than_requested_bound_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="bounded bytes"):
            convert_raw_archive_stream(
                OverBoundReader(),  # type: ignore[arg-type]
                io.BytesIO(),
                io.BytesIO(),
                context=make_context(max_line_bytes=8),
            )

    def test_engine_does_not_close_flush_seek_truncate_or_unbounded_read(self) -> None:
        reader = TrackingReader(fixed_line())
        research = TrackingWriter()
        failures = TrackingWriter()

        report = convert_raw_archive_stream(
            reader,  # type: ignore[arg-type]
            research,  # type: ignore[arg-type]
            failures,  # type: ignore[arg-type]
            context=make_context(),
        )

        assert report.records_written == 1
        assert research.getvalue()
        assert failures.getvalue() == b""


class TestReportAndPublicContracts:
    def test_valid_report_is_frozen(self) -> None:
        report = make_report()
        with pytest.raises(FrozenInstanceError):
            report.lines_seen = 1  # type: ignore[misc]

    @pytest.mark.parametrize(
        "overrides",
        [
            {"file": ""},
            {"file": "."},
            {"file": ".."},
            {"file": "dir/file.jsonl"},
            {"file": "dir\\file.jsonl"},
            {"file": "bad\0file"},
            {"file": "bad\rfile"},
            {"file": "bad\nfile"},
            {"lines_seen": True},
            {"records_written": 1.0},
            {"records_quarantined": "1"},
            {"research_bytes": -1},
            {"failure_bytes": None},
            {"lines_seen": 1},
            {"records_quarantined": 1, "lines_seen": 1, "status": "success"},
            {"records_quarantined": 0, "status": "partial"},
            {"status": "failed"},
            {"coverage_start_ms": 0},
            {"coverage_end_ms": 1},
            {
                "lines_seen": 1,
                "records_written": 1,
                "coverage_start_ms": None,
                "coverage_end_ms": 2,
            },
            {
                "lines_seen": 1,
                "records_written": 1,
                "coverage_start_ms": 2,
                "coverage_end_ms": 2,
            },
            {
                "lines_seen": 1,
                "records_written": 1,
                "coverage_start_ms": True,
                "coverage_end_ms": 2,
            },
            {"research_sha256": "A" * 64},
            {"failure_sha256": "0" * 63},
        ],
    )
    def test_report_rejects_invalid_direct_construction(
        self,
        overrides: dict[str, object],
    ) -> None:
        with pytest.raises(ValueError):
            make_report(**overrides)

    @pytest.mark.parametrize(
        ("operation", "message"),
        [
            ("read_raw", "raw archive stream read failed"),
            ("write_research", "research stream write failed"),
            ("write_failure", "conversion failure stream write failed"),
        ],
    )
    def test_stream_error_has_fixed_message_and_operation(
        self,
        operation: str,
        message: str,
    ) -> None:
        error = ConversionStreamError(operation)  # type: ignore[arg-type]
        assert error.operation == operation
        assert str(error) == message

    @pytest.mark.parametrize("operation", ["", "failed", None, True])
    def test_stream_error_rejects_unsupported_operation(self, operation: object) -> None:
        with pytest.raises(ValueError, match="supported stream operation"):
            ConversionStreamError(operation)  # type: ignore[arg-type]

    def test_context_and_stream_methods_are_validated_as_programmer_arguments(self) -> None:
        with pytest.raises(ValueError, match="RawConversionContext"):
            convert_raw_archive_stream(
                io.BytesIO(),
                io.BytesIO(),
                io.BytesIO(),
                context=object(),  # type: ignore[arg-type]
            )

    @pytest.mark.parametrize(
        "statement",
        [
            (
                "StreamConversionReport(file='safe.jsonl',lines_seen=1,records_written=0,"
                "records_quarantined=0,coverage_start_ms=None,coverage_end_ms=None,"
                f"research_sha256='{_EMPTY_SHA256}',failure_sha256='{_EMPTY_SHA256}',"
                "research_bytes=0,failure_bytes=0,status='success')"
            ),
            "ConversionStreamError('unsupported')",
        ],
    )
    def test_validation_remains_active_under_python_optimized_mode(self, statement: str) -> None:
        script = (
            "from packages.market_data.datasets.conversion_stream import "
            "ConversionStreamError,StreamConversionReport; "
            f"{statement}"
        )
        result = subprocess.run(
            [sys.executable, "-O", "-c", script],
            capture_output=True,
            check=False,
            text=True,
        )
        assert result.returncode != 0
        assert "ValueError" in result.stderr

    def test_five_lazy_exports_import_successfully(self) -> None:
        import packages.market_data.datasets as dataset_exports

        assert dataset_exports.ConversionStatus is not None
        assert dataset_exports.ConversionStreamError is ConversionStreamError
        assert dataset_exports.StreamConversionReport is StreamConversionReport
        assert (
            dataset_exports.conversion_failure_to_ndjson_line is conversion_failure_to_ndjson_line
        )
        assert dataset_exports.convert_raw_archive_stream is convert_raw_archive_stream

    def test_production_module_has_no_filesystem_network_or_forbidden_stream_calls(self) -> None:
        tree = ast.parse(inspect.getsource(stream_module))
        forbidden_imports = {
            "aiohttp",
            "httpx",
            "os",
            "pathlib",
            "requests",
            "shutil",
            "socket",
            "tempfile",
            "urllib",
        }
        forbidden_calls = {
            "close",
            "eval",
            "exec",
            "flush",
            "open",
            "read",
            "readlines",
            "seek",
            "truncate",
        }

        imported_roots: set[str] = set()
        called_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported_roots.update(alias.name.split(".", maxsplit=1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                imported_roots.add(node.module.split(".", maxsplit=1)[0])
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    called_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    called_names.add(node.func.attr)

        assert imported_roots.isdisjoint(forbidden_imports)
        assert called_names.isdisjoint(forbidden_calls)
