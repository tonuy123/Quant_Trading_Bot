"""DATA-005B-2A: pure raw-archive line conversion kernel tests."""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
from hashlib import sha256
from typing import Any

import pytest

import packages.market_data.datasets.converter as converter_module
from packages.market_data.datasets.converter import (
    ConversionFailure,
    LineConversionResult,
    RawConversionContext,
    convert_raw_archive_line,
)
from packages.market_data.datasets.research_format import ResearchCandle

_OPEN_MS = 1_704_067_200_000
_CLOSE_MS = 1_704_067_260_000
_OPEN_ISO = "2024-01-01T00:00:00+00:00"
_CLOSE_ISO = "2024-01-01T00:01:00+00:00"
_FEBRUARY_OPEN_MS = 1_706_745_600_000
_MARCH_OPEN_MS = 1_709_251_200_000
_FEBRUARY_OPEN_ISO = "2024-02-01T00:00:00+00:00"
_MARCH_OPEN_ISO = "2024-03-01T00:00:00+00:00"
_EMPTY_SHA256 = "0" * 64
_DEFAULT_ENVELOPE = object()


def make_payload(**overrides: object) -> list[object]:
    values: list[object] = [
        _OPEN_MS,
        "50000.00",
        "50001.00",
        "49999.00",
        "50000.50",
        "1.5",
        _CLOSE_MS - 1,
        "75000.75",
        100,
        "0.75",
        "37500.00",
        "0",
    ]
    for index_text, value in overrides.items():
        values[int(index_text)] = value
    return values


def make_envelope(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "symbol": "BTC/USDT",
        "interval": "1m",
        "open_time": _OPEN_ISO,
        "close_time": _CLOSE_ISO,
        "source": "binance_public_rest",
        "payload": make_payload(),
    }
    values.update(overrides)
    return values


def archive_line(
    envelope: object = _DEFAULT_ENVELOPE,
    *,
    ending: bytes = b"\n",
) -> bytes:
    record = make_envelope() if envelope is _DEFAULT_ENVELOPE else envelope
    return json.dumps(record, separators=(",", ":")).encode("utf-8") + ending


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


def converted(
    raw_line: bytes | None = None,
    *,
    context: RawConversionContext | None = None,
    previous_open_time_ms: int | None = None,
) -> LineConversionResult:
    return convert_raw_archive_line(
        archive_line() if raw_line is None else raw_line,
        line_number=7,
        context=make_context() if context is None else context,
        previous_open_time_ms=previous_open_time_ms,
    )


def candle_from(result: LineConversionResult) -> ResearchCandle:
    assert result.failure is None
    assert result.candle is not None
    return result.candle


def failure_from(result: LineConversionResult, expected_type: str) -> ConversionFailure:
    assert result.candle is None
    assert result.failure is not None
    assert result.failure.failure_type == expected_type
    return result.failure


class TestValidConversion:
    def test_valid_fixed_candle_and_exact_envelope(self) -> None:
        envelope = make_envelope()
        assert set(envelope) == {
            "symbol",
            "interval",
            "open_time",
            "close_time",
            "source",
            "payload",
        }

        candle = candle_from(converted(archive_line(envelope)))

        assert candle.symbol == "BTC/USDT"
        assert candle.interval == "1m"
        assert candle.open_time_ms == _OPEN_MS
        assert candle.close_time_ms == _CLOSE_MS

    def test_valid_february_2024_calendar_month_uses_independent_literals(self) -> None:
        payload = make_payload()
        payload[0] = _FEBRUARY_OPEN_MS
        payload[6] = _MARCH_OPEN_MS - 1
        envelope = make_envelope(
            interval="1M",
            open_time=_FEBRUARY_OPEN_ISO,
            close_time=_MARCH_OPEN_ISO,
            payload=payload,
        )
        context = make_context(
            file_name="BTC-USDT-1M.jsonl",
            interval="1M",
            range_start=datetime(2024, 2, 1, tzinfo=UTC),
            range_end=datetime(2024, 3, 1, tzinfo=UTC),
        )

        candle = candle_from(converted(archive_line(envelope), context=context))

        assert candle.open_time_ms == 1_706_745_600_000
        assert candle.close_time_ms == 1_709_251_200_000
        assert candle.close_time == "2024-03-01T00:00:00+00:00"

    def test_scientific_notation_is_canonicalized_by_existing_factory(self) -> None:
        payload = make_payload(**{"1": "1E2", "2": "1.01E2", "3": "9.9E1", "4": "1.005E2"})

        candle = candle_from(converted(archive_line(make_envelope(payload=payload))))

        assert (candle.open, candle.high, candle.low, candle.close) == (
            "100",
            "101",
            "99",
            "100.5",
        )

    def test_offset_aware_context_range_is_normalized_to_utc(self) -> None:
        plus_seven = timezone(timedelta(hours=7))
        context = make_context(
            range_start=datetime(2024, 1, 1, 7, tzinfo=plus_seven),
            range_end=datetime(2024, 1, 2, 7, tzinfo=plus_seven),
        )

        candle_from(converted(context=context))

        assert context.range_start == datetime(2024, 1, 1, tzinfo=UTC)
        assert context.range_end == datetime(2024, 1, 2, tzinfo=UTC)

    def test_input_bytes_are_not_mutated(self) -> None:
        raw_line = archive_line()
        before = raw_line[:]

        candle_from(converted(raw_line))

        assert raw_line == before


class TestJsonAndEnvelopeFailures:
    @pytest.mark.parametrize("shape", ["missing", "unknown"])
    def test_missing_or_unknown_envelope_key_is_rejected(self, shape: str) -> None:
        envelope = make_envelope()
        if shape == "missing":
            envelope.pop("source")
        else:
            envelope["unexpected"] = "field"

        failure_from(converted(archive_line(envelope)), "invalid_envelope")

    def test_duplicate_json_object_key_is_rejected(self) -> None:
        content = archive_line(ending=b"").decode("utf-8")
        duplicated = content.replace(
            '{"symbol":',
            '{"symbol":"BTC/USDT","symbol":',
            1,
        ).encode("utf-8")

        failure_from(converted(duplicated), "invalid_envelope")

    @pytest.mark.parametrize("record", [[], "text", None, 1])
    def test_non_object_json_is_rejected(self, record: object) -> None:
        failure_from(converted(archive_line(record)), "invalid_envelope")

    @pytest.mark.parametrize("raw_line", [b"{\n", b'{{"symbol":1}\n', b"not-json\n"])
    def test_malformed_json_is_rejected(self, raw_line: bytes) -> None:
        failure_from(converted(raw_line), "malformed_json")

    @pytest.mark.parametrize("raw_line", [b"NaN\n", b"Infinity\n", b"-Infinity\n"])
    def test_non_standard_json_constants_are_rejected(self, raw_line: bytes) -> None:
        failure_from(converted(raw_line), "malformed_json")

    @pytest.mark.parametrize("raw_line", [b"", b"   \t\r\n"])
    def test_empty_or_whitespace_only_line_is_rejected(self, raw_line: bytes) -> None:
        failure_from(converted(raw_line), "malformed_json")

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("symbol", 1),
            ("interval", 1),
            ("open_time", 1),
            ("close_time", 1),
            ("source", 1),
            ("payload", {}),
        ],
    )
    def test_wrong_envelope_field_type_is_rejected(self, field: str, value: object) -> None:
        failure_from(
            converted(archive_line(make_envelope(**{field: value}))),
            "invalid_envelope",
        )

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("symbol", "ETH/USDT"),
            ("interval", "1M"),
            ("source", "other_source"),
        ],
    )
    def test_wrong_envelope_identity_is_rejected(self, field: str, value: str) -> None:
        failure_from(
            converted(archive_line(make_envelope(**{field: value}))),
            "invalid_envelope",
        )

    @pytest.mark.parametrize("field", ["open_time", "close_time"])
    def test_envelope_timestamp_cross_check_is_exact(self, field: str) -> None:
        failure_from(
            converted(archive_line(make_envelope(**{field: "mismatched-time"}))),
            "invalid_envelope",
        )


class TestPayloadAndCandleFailures:
    @pytest.mark.parametrize("payload_size", [11, 13])
    def test_non_twelve_field_payload_is_rejected(self, payload_size: int) -> None:
        payload = make_payload()
        payload = payload[:payload_size] if payload_size == 11 else [*payload, "extra"]

        failure_from(
            converted(archive_line(make_envelope(payload=payload))),
            "invalid_candle",
        )

    @pytest.mark.parametrize("timestamp", [True, 1.5])
    def test_bool_or_float_timestamp_is_rejected(self, timestamp: object) -> None:
        payload = make_payload(**{"0": timestamp})

        failure_from(
            converted(archive_line(make_envelope(payload=payload))),
            "invalid_candle",
        )

    def test_malformed_raw_inclusive_close_boundary_is_rejected(self) -> None:
        payload = make_payload(**{"6": _CLOSE_MS})

        failure_from(
            converted(archive_line(make_envelope(payload=payload))),
            "invalid_candle",
        )

    def test_invalid_ohlc_is_rejected(self) -> None:
        payload = make_payload(**{"2": "49998", "3": "49999"})

        failure_from(
            converted(archive_line(make_envelope(payload=payload))),
            "invalid_candle",
        )

    def test_factory_failure_text_and_raw_value_do_not_leak(self) -> None:
        raw_value = "PRIVATE-KEY-SECRET-PRICE-987654321"
        payload = make_payload(**{"1": raw_value})

        failure = failure_from(
            converted(archive_line(make_envelope(payload=payload))),
            "invalid_candle",
        )

        assert failure.line_preview is None
        assert raw_value not in failure.reason


class TestRangeAndOrdering:
    def test_candle_opening_exactly_at_range_start_is_accepted(self) -> None:
        context = make_context(range_start=datetime(2024, 1, 1, tzinfo=UTC))
        assert candle_from(converted(context=context)).open_time_ms == _OPEN_MS

    def test_candle_opening_exactly_at_range_end_is_rejected(self) -> None:
        context = make_context(
            range_start=datetime(2023, 12, 31, tzinfo=UTC),
            range_end=datetime(2024, 1, 1, tzinfo=UTC),
        )
        failure_from(converted(context=context), "out_of_range")

    def test_candle_closing_after_range_end_is_rejected(self) -> None:
        context = make_context(
            range_end=datetime(2024, 1, 1, 0, 0, 30, tzinfo=UTC),
        )
        failure_from(converted(context=context), "out_of_range")

    def test_decreasing_open_time_is_rejected(self) -> None:
        failure_from(
            converted(previous_open_time_ms=_OPEN_MS + 1),
            "ordering_violation",
        )

    def test_equal_duplicate_open_time_is_accepted(self) -> None:
        assert candle_from(converted(previous_open_time_ms=_OPEN_MS)).open_time_ms == _OPEN_MS


class TestSecurityHashAndPreview:
    def test_sha256_hashes_exact_bytes_including_lf(self) -> None:
        raw_line = b"ordinary-malformed-json\n"
        failure = failure_from(converted(raw_line), "malformed_json")
        assert failure.line_sha256 == sha256(raw_line).hexdigest()

    def test_lf_and_crlf_produce_same_candle_but_hash_different_exact_bytes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seen: list[bytes] = []

        def recording_sha256(raw_bytes: bytes) -> Any:
            seen.append(raw_bytes)
            return sha256(raw_bytes)

        monkeypatch.setattr(converter_module, "sha256", recording_sha256)
        lf = archive_line(ending=b"\n")
        crlf = archive_line(ending=b"\r\n")

        lf_candle = candle_from(converted(lf))
        crlf_candle = candle_from(converted(crlf))

        assert lf_candle == crlf_candle
        assert seen == [lf, crlf]
        assert sha256(lf).hexdigest() != sha256(crlf).hexdigest()

    def test_invalid_utf8_has_no_preview(self) -> None:
        failure = failure_from(converted(b"\xff\xfe\n"), "invalid_utf8")
        assert failure.line_preview is None

    def test_oversized_line_has_no_preview_and_precedes_utf8_or_json(self) -> None:
        context = make_context(max_line_bytes=2)
        failure = failure_from(converted(b"\xff\xff\xff", context=context), "line_too_large")
        assert failure.line_preview is None

    def test_ordinary_safe_preview_is_preserved(self) -> None:
        raw_line = b"ordinary-malformed-json"
        failure = failure_from(converted(raw_line), "malformed_json")
        assert failure.line_preview == raw_line.decode("utf-8")

    def test_safe_preview_is_bounded_to_256_unicode_characters(self) -> None:
        failure = failure_from(converted(("é" * 300).encode("utf-8")), "malformed_json")
        assert failure.line_preview == "é" * 256
        assert len(failure.line_preview) == 256

    @pytest.mark.parametrize(
        "pattern",
        [
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
        ],
    )
    def test_secret_pattern_inside_preview_window_suppresses_preview(self, pattern: str) -> None:
        failure = failure_from(converted(f'{{"{pattern.upper()}":'.encode()), "malformed_json")
        assert failure.line_preview is None

    def test_secret_pattern_after_character_256_also_suppresses_preview(self) -> None:
        failure = failure_from(converted(("x" * 300 + "BeArEr").encode()), "malformed_json")
        assert failure.line_preview is None

    def test_unicode_escaped_api_key_suppresses_preview(self) -> None:
        raw_line = b'{"api\\u005fkey":"synthetic"'
        failure = failure_from(converted(raw_line), "malformed_json")
        assert failure.line_preview is None

    def test_unicode_escaped_marker_after_character_256_suppresses_preview(self) -> None:
        raw_line = ("x" * 300 + r"bear\u0065r=synthetic").encode("utf-8")
        failure = failure_from(converted(raw_line), "malformed_json")
        assert failure.line_preview is None

    @pytest.mark.parametrize(
        "content",
        [
            "prefix\x1b[31msuffix",
            "prefix\nsuffix",
            "prefix\rsuffix",
            "prefix\0suffix",
            "prefix\u200bsuffix",
        ],
        ids=["ansi-esc", "embedded-lf", "embedded-cr", "nul", "zero-width-format"],
    )
    def test_control_or_format_character_suppresses_preview(self, content: str) -> None:
        failure = failure_from(converted(content.encode("utf-8")), "malformed_json")
        assert failure.line_preview is None

    @pytest.mark.parametrize(
        "unsafe_preview",
        [
            "api_key=synthetic-marker",
            r"api\u005fkey=synthetic-marker",
            "synthetic\x1bmarker",
            "synthetic\u200bmarker",
            "synthetic\ud800marker",
        ],
        ids=["secret", "unicode-escape", "control", "format", "surrogate"],
    )
    def test_direct_failure_canonicalizes_unsafe_preview(
        self,
        unsafe_preview: str,
    ) -> None:
        failure = ConversionFailure(
            file="safe.jsonl",
            line_number=1,
            failure_type="malformed_json",
            line_preview=unsafe_preview,
            line_sha256=_EMPTY_SHA256,
        )
        assert failure.line_preview is None

    def test_failure_repr_does_not_retain_synthetic_secret(self) -> None:
        synthetic_secret = "bearer=synthetic-preview-marker"
        failure = ConversionFailure(
            file="safe.jsonl",
            line_number=1,
            failure_type="malformed_json",
            line_preview=synthetic_secret,
            line_sha256=_EMPTY_SHA256,
        )
        assert failure.line_preview is None
        assert synthetic_secret not in repr(failure)
        assert "bearer" not in repr(failure).casefold()

    def test_reason_and_constructor_exceptions_are_sanitized(self) -> None:
        raw_tokens = ("1704067200000", "50000.1234", "TOP-SECRET-BEARER")
        raw_line = ("{" + ",".join(raw_tokens)).encode()
        failure = failure_from(converted(raw_line), "malformed_json")
        with pytest.raises(ValueError) as exc_info:
            ConversionFailure(
                file="safe.jsonl",
                line_number=1,
                failure_type="malformed_json",
                line_preview="x" * 257,
                line_sha256=_EMPTY_SHA256,
            )

        for raw_token in raw_tokens:
            assert raw_token not in failure.reason
            assert raw_token not in str(exc_info.value)


class TestDirectContracts:
    @pytest.mark.parametrize(
        "file_name",
        [
            "",
            ".",
            "..",
            "dir/name.jsonl",
            "dir\\name.jsonl",
            "bad\0name",
            "bad\rname",
            "bad\nname",
            1,
        ],
    )
    def test_context_rejects_unsafe_file_names(self, file_name: object) -> None:
        with pytest.raises(ValueError, match="safe basename"):
            make_context(file_name=file_name)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("symbol", "BTCUSDT"),
            ("symbol", "btc/usdt"),
            ("symbol", "BTC/"),
            ("symbol", 1),
            ("interval", "1M "),
            ("interval", "1x"),
            ("interval", 1),
        ],
    )
    def test_context_rejects_noncanonical_identity(self, field: str, value: object) -> None:
        with pytest.raises(ValueError):
            make_context(**{field: value})

    @pytest.mark.parametrize(
        "overrides",
        [
            {"range_start": "2024-01-01"},
            {"range_end": None},
            {"range_start": datetime(2024, 1, 1)},
            {"range_end": datetime(2024, 1, 2)},
            {
                "range_start": datetime(2024, 1, 2, tzinfo=UTC),
                "range_end": datetime(2024, 1, 1, tzinfo=UTC),
            },
            {
                "range_start": datetime(2024, 1, 1, tzinfo=UTC),
                "range_end": datetime(2024, 1, 1, tzinfo=UTC),
            },
        ],
    )
    def test_context_rejects_invalid_ranges(self, overrides: dict[str, object]) -> None:
        with pytest.raises(ValueError):
            make_context(**overrides)

    @pytest.mark.parametrize("value", [True, False, 0, -1, 1.0, "1", None])
    def test_context_rejects_invalid_line_limit(self, value: object) -> None:
        with pytest.raises(ValueError, match="positive integer"):
            make_context(max_line_bytes=value)

    @pytest.mark.parametrize(
        "overrides",
        [
            {"file": "../bad"},
            {"line_number": True},
            {"line_number": 0},
            {"failure_type": "unknown"},
            {"line_preview": 1},
            {"line_preview": "x" * 257},
            {"line_sha256": "A" * 64},
            {"line_sha256": "0" * 63},
            {"line_sha256": "z" * 64},
        ],
    )
    def test_conversion_failure_rejects_invalid_direct_inputs(
        self,
        overrides: dict[str, object],
    ) -> None:
        values: dict[str, object] = {
            "file": "safe.jsonl",
            "line_number": 1,
            "failure_type": "malformed_json",
            "line_preview": None,
            "line_sha256": _EMPTY_SHA256,
        }
        values.update(overrides)
        with pytest.raises(ValueError):
            ConversionFailure(**values)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("failure_type", "reason"),
        [
            ("line_too_large", "raw archive line exceeds the configured byte limit"),
            ("invalid_utf8", "raw archive line is not valid UTF-8"),
            ("malformed_json", "raw archive line is not valid JSON"),
            ("invalid_envelope", "raw archive record violates the downloader envelope contract"),
            ("invalid_candle", "raw payload violates the research candle contract"),
            ("out_of_range", "candle falls outside the requested half-open range"),
            (
                "ordering_violation",
                "candle open time is earlier than the previous accepted record",
            ),
        ],
    )
    def test_conversion_failure_reason_is_fixed(self, failure_type: str, reason: str) -> None:
        failure = ConversionFailure(
            file="safe.jsonl",
            line_number=1,
            failure_type=failure_type,  # type: ignore[arg-type]
            line_preview=None,
            line_sha256=_EMPTY_SHA256,
        )
        assert failure.reason == reason

    @pytest.mark.parametrize("case", ["neither", "both", "wrong_candle", "wrong_failure"])
    def test_line_result_enforces_xor_and_populated_types(self, case: str) -> None:
        candle = candle_from(converted())
        failure = failure_from(converted(b"bad-json"), "malformed_json")
        values: dict[str, object] = {}
        if case == "both":
            values = {"candle": candle, "failure": failure}
        elif case == "wrong_candle":
            values = {"candle": object()}
        elif case == "wrong_failure":
            values = {"failure": object()}
        with pytest.raises(ValueError):
            LineConversionResult(**values)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("argument", "value"),
        [
            ("raw_line", bytearray(b"x")),
            ("raw_line", "x"),
            ("raw_line", memoryview(b"x")),
            ("raw_line", object()),
            ("raw_line", None),
            ("line_number", True),
            ("line_number", 0),
            ("line_number", 1.0),
            ("context", object()),
            ("previous_open_time_ms", True),
            ("previous_open_time_ms", -1),
            ("previous_open_time_ms", 1.0),
        ],
    )
    def test_convert_rejects_programmer_argument_errors(self, argument: str, value: object) -> None:
        arguments: dict[str, object] = {
            "raw_line": archive_line(),
            "line_number": 1,
            "context": make_context(),
            "previous_open_time_ms": None,
        }
        arguments[argument] = value
        with pytest.raises(ValueError):
            convert_raw_archive_line(**arguments)  # type: ignore[arg-type]

    def test_public_dataclasses_are_frozen(self) -> None:
        context = make_context()
        failure = failure_from(converted(b"bad-json"), "malformed_json")
        result = LineConversionResult(failure=failure)

        with pytest.raises(FrozenInstanceError):
            context.file_name = "changed.jsonl"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            failure.line_number = 2  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            result.failure = None  # type: ignore[misc]

    def test_validation_remains_active_under_python_optimized_mode(self) -> None:
        script = (
            "from packages.market_data.datasets.converter import LineConversionResult; "
            "LineConversionResult()"
        )
        result = subprocess.run(
            [sys.executable, "-O", "-c", script],
            capture_output=True,
            check=False,
            text=True,
        )
        assert result.returncode != 0
        assert "ValueError" in result.stderr

    def test_all_five_lazy_exports_import(self) -> None:
        import packages.market_data.datasets as dataset_exports

        assert dataset_exports.ConversionFailure is ConversionFailure
        assert dataset_exports.ConversionFailureType is not None
        assert dataset_exports.LineConversionResult is LineConversionResult
        assert dataset_exports.RawConversionContext is RawConversionContext
        assert dataset_exports.convert_raw_archive_line is convert_raw_archive_line
