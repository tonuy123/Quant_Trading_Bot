"""DATA-005B-1: strict research-format contract tests."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from packages.market_data.datasets import research_format
from packages.market_data.datasets.research_format import (
    RESEARCH_SCHEMA_VERSION,
    RESEARCH_SOURCE,
    DecimalInvalidError,
    ResearchCandle,
    ResearchCandleValidationError,
    canonical_decimal,
    research_candle_from_binance_kline,
)
from packages.market_data.datasets.timestamps import normalize_epoch_to_utc

_OPEN_TIME_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Test-local, hardcoded interval durations.  Never imported from production
# so an accidental production-map regression cannot silently pass.
_TEST_FIXED_MS: dict[str, int] = {
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
_TEST_INTERVALS: frozenset[str] = frozenset(_TEST_FIXED_MS) | {"1M"}


def _to_ms(value: datetime) -> int:
    delta = value.astimezone(UTC) - _EPOCH
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def _test_boundary_after(open_ms: int, interval: str) -> int:
    """Test-local expected exclusive interval end, independent of production."""
    if interval in _TEST_FIXED_MS:
        return open_ms + _TEST_FIXED_MS[interval]
    open_dt = _EPOCH + timedelta(milliseconds=open_ms)
    if open_dt.month == 12:
        next_open = open_dt.replace(year=open_dt.year + 1, month=1, day=1)
    else:
        next_open = open_dt.replace(month=open_dt.month + 1, day=1)
    return _to_ms(next_open)


def _iso_for_epoch_ms(value: object, fallback: int = _OPEN_TIME_MS) -> str:
    """Build canonical test ISO text without coercing invalid override types."""
    epoch_ms = (
        value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else fallback
    )
    return normalize_epoch_to_utc(epoch_ms, "ms").isoformat()


def make_candle(**overrides: object) -> ResearchCandle:
    """Return a valid candle unless an explicit field override violates the contract."""
    interval = overrides.get("interval", "1m")
    interval_key = interval if isinstance(interval, str) and interval in _TEST_INTERVALS else "1m"
    open_time_ms = overrides.get("open_time_ms", _OPEN_TIME_MS)
    valid_open = (
        open_time_ms
        if isinstance(open_time_ms, int)
        and not isinstance(open_time_ms, bool)
        and open_time_ms >= 0
        else _OPEN_TIME_MS
    )
    default_close_time_ms = _test_boundary_after(valid_open, interval_key)
    close_time_ms = overrides.get("close_time_ms", default_close_time_ms)
    values: dict[str, object] = {
        "symbol": "BTC/USDT",
        "interval": interval,
        "open_time": _iso_for_epoch_ms(open_time_ms),
        "open_time_ms": open_time_ms,
        "close_time": _iso_for_epoch_ms(close_time_ms),
        "close_time_ms": close_time_ms,
        "open": "50000.00",
        "high": "50001.00",
        "low": "49999.00",
        "close": "50000.50",
        "volume": "1.5",
        "quote_volume": "75000.75",
        "trade_count": 100,
        "taker_buy_base_volume": "0.75",
        "taker_buy_quote_volume": "37500.00",
        "source": RESEARCH_SOURCE,
        "schema_version": RESEARCH_SCHEMA_VERSION,
    }
    values.update(overrides)
    return ResearchCandle(**values)  # type: ignore[arg-type]


def make_binance_kline(interval: str = "1m") -> list[object]:
    """Return one structurally valid Binance public 12-field kline."""
    expected_close_ms = _test_boundary_after(_OPEN_TIME_MS, interval)
    return [
        _OPEN_TIME_MS,
        "50000.00",
        "50001.00",
        "49999.00",
        "50000.50",
        "1.5",
        expected_close_ms - 1,  # Binance inclusive close
        "75000.75",
        100,
        "0.75",
        "37500.00",
        "0",
    ]


def assert_detached_sanitized_error(
    error: ValueError,
    *sensitive_markers: str,
) -> None:
    """Assert public validation errors retain no translated source exception."""
    assert error.__cause__ is None
    assert error.__context__ is None
    public_surfaces = (str(error), repr(error), repr(vars(error)))
    for marker in sensitive_markers:
        assert all(marker not in surface for surface in public_surfaces)


class TestCanonicalDecimal:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("1E-8", "0.00000001"),
            ("3E4", "30000"),
            ("30000.00", "30000"),
            ("1.00000000", "1"),
            ("0.00100", "0.001"),
            ("0E-8", "0"),
            ("-100.50", "-100.5"),
            ("-0", "0"),
        ],
    )
    def test_normalizes_to_fixed_point(self, raw: str, expected: str) -> None:
        assert canonical_decimal(raw) == expected

    @pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity", "", " 1", "a-value"])
    def test_invalid_string_is_rejected(self, raw: str) -> None:
        with pytest.raises(DecimalInvalidError):
            canonical_decimal(raw)

    @pytest.mark.parametrize("raw", [1, 1.25, True, None, object()])
    def test_non_string_is_rejected_without_coercion(self, raw: object) -> None:
        with pytest.raises(DecimalInvalidError):
            canonical_decimal(raw)  # type: ignore[arg-type]

    def test_error_contains_no_raw_decimal_value(self) -> None:
        secret = "sensitive-number-50000.0001"
        with pytest.raises(DecimalInvalidError) as exc_info:
            canonical_decimal(secret)
        error = exc_info.value
        assert not hasattr(error, "value")
        assert secret not in str(error)
        assert secret not in error.__dict__.values()
        assert error.field == "decimal"
        assert error.reason == "invalid decimal input"

    def test_decimal_parse_error_retains_no_lower_level_context(self) -> None:
        raw = "1E+" + "9" * 20
        with pytest.raises(DecimalInvalidError) as exc_info:
            canonical_decimal(raw)

        error = exc_info.value
        assert error.field == "decimal"
        assert error.reason == "invalid decimal input"
        assert_detached_sanitized_error(error, raw)

    def test_positive_exponent_at_fixed_point_limit_is_accepted(self) -> None:
        result = canonical_decimal("1E+4095")
        assert result == "1" + "0" * 4095
        assert len(result) == 4_096

    def test_negative_exponent_at_fixed_point_limit_is_accepted(self) -> None:
        result = canonical_decimal("1E-4094")
        assert result == "0." + "0" * 4093 + "1"
        assert len(result) == 4_096

    @pytest.mark.parametrize("raw", ["1E+4096", "1E-4095"])
    def test_fixed_point_expansion_above_limit_is_rejected(self, raw: str) -> None:
        with pytest.raises(DecimalInvalidError):
            canonical_decimal(raw)

    def test_negative_sign_is_included_in_fixed_point_length(self) -> None:
        accepted = canonical_decimal("-1E+4094")
        assert accepted == "-1" + "0" * 4094
        assert len(accepted) == 4_096
        with pytest.raises(DecimalInvalidError):
            canonical_decimal("-1E+4095")

    def test_over_bound_error_is_typed_sanitized_and_retains_no_value(self) -> None:
        raw = "1E-200000"
        with pytest.raises(DecimalInvalidError) as exc_info:
            canonical_decimal(raw)

        error = exc_info.value
        assert error.field == "decimal"
        assert error.reason == "invalid decimal input"
        assert not hasattr(error, "value")
        assert raw not in str(error)
        assert raw not in repr(error)
        assert raw not in repr(vars(error))

    def test_large_expansion_is_rejected_before_fixed_point_format(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        format_called = False

        def forbidden_format(_value: object, _specification: str) -> str:
            nonlocal format_called
            format_called = True
            raise AssertionError("fixed-point format must not run")

        monkeypatch.setattr(research_format, "format", forbidden_format, raising=False)
        with pytest.raises(DecimalInvalidError):
            canonical_decimal("1E-200000")
        assert not format_called

    def test_canonicalization_uses_no_float_rounding_or_numeric_truncation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def forbidden_numeric_operation(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("binary float and rounding are forbidden")

        monkeypatch.setattr(research_format, "float", forbidden_numeric_operation, raising=False)
        monkeypatch.setattr(research_format, "round", forbidden_numeric_operation, raising=False)

        assert canonical_decimal("1.2300E-8") == "0.0000000123"


class TestResearchCandleValid:
    def test_valid_candle_is_canonical_and_immutable(self) -> None:
        candle = make_candle(open="1E2", high="100.00", low="99.50", close="1E2")
        assert candle.symbol == "BTC/USDT"
        assert candle.open == "100"
        assert candle.high == "100"
        assert candle.low == "99.5"
        assert candle.close == "100"
        with pytest.raises(AttributeError):
            candle.symbol = "ETH/USDT"  # type: ignore[misc]

    @pytest.mark.parametrize("interval", sorted(_TEST_INTERVALS))
    def test_all_intervals_calculate_close_boundary_and_iso(self, interval: str) -> None:
        candle = make_candle(interval=interval)
        expected_close_ms = _test_boundary_after(_OPEN_TIME_MS, interval)
        assert candle.close_time_ms == expected_close_ms
        assert candle.close_time == _iso_for_epoch_ms(expected_close_ms)


class TestResearchCandleContract:
    @pytest.mark.parametrize("symbol", ["BTCUSDT", "BTC/", "/USDT", "", "btc/usdt", "BTC/USDT/X"])
    def test_noncanonical_symbol_is_rejected(self, symbol: str) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            make_candle(symbol=symbol)
        assert exc_info.value.field == "symbol"

    def test_symbol_dependency_error_retains_no_lower_level_context(self) -> None:
        raw_symbol = "SYNTHETIC/"
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            make_candle(symbol=raw_symbol)

        error = exc_info.value
        assert error.field == "symbol"
        assert_detached_sanitized_error(error, raw_symbol)

    def test_epoch_dependency_error_retains_no_lower_level_context(self) -> None:
        raw_epoch = 10**100
        record = make_candle().to_record()
        record["open_time_ms"] = raw_epoch

        with pytest.raises(ResearchCandleValidationError) as exc_info:
            ResearchCandle.from_record(record)

        error = exc_info.value
        assert error.field == "open_time_ms"
        assert_detached_sanitized_error(error, str(raw_epoch))

    def test_decimal_field_translation_retains_no_lower_level_context(self) -> None:
        raw_decimal = "1E+" + "9" * 20
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            make_candle(open=raw_decimal)

        error = exc_info.value
        assert error.field == "open"
        assert error.reason == "invalid decimal representation"
        assert_detached_sanitized_error(error, raw_decimal)

    def test_monthly_constructor_boundary_error_retains_no_context(self) -> None:
        misaligned_open_ms = _OPEN_TIME_MS + 60_000
        monthly_close_ms = _test_boundary_after(_OPEN_TIME_MS, "1M")
        record = make_candle().to_record()
        record.update(
            {
                "interval": "1M",
                "open_time": _iso_for_epoch_ms(misaligned_open_ms),
                "open_time_ms": misaligned_open_ms,
                "close_time": _iso_for_epoch_ms(monthly_close_ms),
                "close_time_ms": monthly_close_ms,
            }
        )

        with pytest.raises(ResearchCandleValidationError) as exc_info:
            ResearchCandle.from_record(record)

        error = exc_info.value
        assert error.field == "open_time_ms"
        assert_detached_sanitized_error(error, str(misaligned_open_ms))

    @pytest.mark.parametrize("field", ["symbol", "interval", "open_time", "close_time", "source"])
    @pytest.mark.parametrize("value", [None, 1, object()])
    def test_string_fields_reject_non_strings(self, field: str, value: object) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            make_candle(**{field: value})
        assert exc_info.value.field == field

    @pytest.mark.parametrize(
        "field", ["open_time_ms", "close_time_ms", "trade_count", "schema_version"]
    )
    @pytest.mark.parametrize("value", [True, False, 1.5, "1", None])
    def test_integer_fields_reject_bool_and_nonintegers(self, field: str, value: object) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            make_candle(**{field: value})
        assert exc_info.value.field == field

    @pytest.mark.parametrize(
        "field",
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        ],
    )
    @pytest.mark.parametrize("value", [None, 1, 1.5, object()])
    def test_decimal_fields_reject_non_strings(self, field: str, value: object) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            make_candle(**{field: value})
        assert exc_info.value.field == field

    @pytest.mark.parametrize(
        ("overrides", "field"),
        [
            ({"interval": "7m"}, "interval"),
            ({"source": "other"}, "source"),
            ({"schema_version": 2}, "schema_version"),
            ({"open_time": "not-a-timestamp"}, "open_time/open_time_ms"),
            ({"open_time_ms": -1}, "open_time_ms"),
            ({"close_time_ms": _OPEN_TIME_MS}, "close_time_ms"),
            ({"close_time_ms": _OPEN_TIME_MS + 300_000}, "close_time_ms"),
            ({"trade_count": -1}, "trade_count"),
            ({"open": "NaN"}, "open"),
            ({"high": "49998", "low": "49999"}, "OHLC"),
            ({"volume": "-1"}, "volume"),
        ],
    )
    def test_invalid_contract_values_are_sanitized(
        self,
        overrides: dict[str, object],
        field: str,
    ) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            make_candle(**overrides)
        error = exc_info.value
        assert error.field == field
        assert "49998" not in str(error)
        assert "NaN" not in str(error)

    def test_validation_runs_under_python_optimized_mode(self) -> None:
        script = (
            "from packages.market_data.datasets.research_format import ResearchCandle, "
            "RESEARCH_SOURCE, RESEARCH_SCHEMA_VERSION; "
            "ResearchCandle(symbol='BTC/USDT', interval='1m', "
            "open_time='2024-01-01T00:00:00+00:00', open_time_ms=1704067200000, "
            "close_time='2024-01-01T00:00:00+00:00', close_time_ms=1704067200000, "
            "open='50000', high='50000', low='50000', close='50000', volume='1', "
            "quote_volume='50000', trade_count=1, taker_buy_base_volume='0.5', "
            "taker_buy_quote_volume='25000', source=RESEARCH_SOURCE, "
            "schema_version=RESEARCH_SCHEMA_VERSION)"
        )
        result = subprocess.run(
            [sys.executable, "-O", "-c", script],
            capture_output=True,
            check=False,
            text=True,
        )
        assert result.returncode != 0
        assert "ResearchCandleValidationError" in result.stderr


class TestSerialization:
    def test_to_record_is_alphabetically_ordered(self) -> None:
        record = make_candle().to_record()
        assert list(record) == sorted(record)

    def test_to_json_is_compact_deterministic_and_float_free(self) -> None:
        candle = make_candle()
        expected = json.dumps(candle.to_record(), separators=(",", ":"), sort_keys=True)
        assert candle.to_json() == expected
        assert "e+" not in candle.to_json().lower()
        assert candle.to_ndjson_line() == f"{expected}\n"

    def test_round_trip_from_record_and_json(self) -> None:
        candle = make_candle()
        assert ResearchCandle.from_record(candle.to_record()) == candle
        assert ResearchCandle.from_json(candle.to_json()) == candle

    @pytest.mark.parametrize(
        ("record", "field"),
        [
            ([], "record"),
            ({"not": "a candle"}, "record"),
        ],
    )
    def test_from_record_requires_an_exact_dict_shape(self, record: object, field: str) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            ResearchCandle.from_record(record)
        assert exc_info.value.field == field

    def test_from_record_rejects_unknown_and_missing_fields(self) -> None:
        record = make_candle().to_record()
        missing = dict(record)
        missing.pop("symbol")
        unknown = dict(record)
        unknown["unexpected"] = "value"

        with pytest.raises(ResearchCandleValidationError, match="missing required fields"):
            ResearchCandle.from_record(missing)
        with pytest.raises(ResearchCandleValidationError, match="unknown fields are not allowed"):
            ResearchCandle.from_record(unknown)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("open_time_ms", "1704067200000"),
            ("close_time_ms", 1.5),
            ("trade_count", True),
            ("schema_version", "1"),
            ("open", 50000),
            ("symbol", 123),
        ],
    )
    def test_from_record_never_coerces_values(self, field: str, value: object) -> None:
        record = make_candle().to_record()
        record[field] = value
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            ResearchCandle.from_record(record)
        assert exc_info.value.field == field

    @pytest.mark.parametrize("text", [None, "[]", "not-json"])
    def test_from_json_uses_sanitized_validation_errors(self, text: object) -> None:
        with pytest.raises(ResearchCandleValidationError):
            ResearchCandle.from_json(text)

    def test_malformed_sensitive_json_retains_no_parser_context(self) -> None:
        marker = "synthetic-private-marker"
        malformed = f'{{"api_key":"{marker}"'
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            ResearchCandle.from_json(malformed)

        error = exc_info.value
        assert error.field == "json"
        assert error.reason == "invalid JSON object"
        assert_detached_sanitized_error(error, "api_key", marker, malformed)

    def test_incorrect_iso_helper_was_removed(self) -> None:
        assert not hasattr(research_format, "_iso_to_epoch_ms")


class TestResearchCandleFromBinanceKline:
    @pytest.mark.parametrize("interval", sorted(_TEST_INTERVALS))
    def test_factory_calculates_exclusive_close_boundary_for_all_intervals(
        self, interval: str
    ) -> None:
        candle = research_candle_from_binance_kline(
            make_binance_kline(interval), "BTC/USDT", interval
        )
        expected_close_ms = _test_boundary_after(_OPEN_TIME_MS, interval)
        assert candle.close_time_ms == expected_close_ms
        assert candle.close_time == _iso_for_epoch_ms(expected_close_ms)

    def test_factory_converts_inclusive_close_plus_one_ms(self) -> None:
        raw = make_binance_kline()
        canonical = research_candle_from_binance_kline(raw, "BTC/USDT", "1m")
        assert canonical.close_time_ms == raw[6] + 1

    def test_factory_rejects_mismatched_raw_close(self) -> None:
        raw = make_binance_kline()
        raw[6] = _OPEN_TIME_MS + 123
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(raw, "BTC/USDT", "1m")
        assert exc_info.value.field == "raw_payload"

    def test_factory_rejects_malformed_monthly_raw_close(self) -> None:
        raw = make_binance_kline("1M")
        raw[6] = raw[6] - 86_400_000  # shifted by one day, not silently corrected
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(raw, "BTC/USDT", "1M")
        assert exc_info.value.field == "raw_payload"

    def test_factory_rejects_monthly_open_not_aligned_to_utc_month(self) -> None:
        raw = make_binance_kline("1m")
        raw[0] = _OPEN_TIME_MS + 60_000
        raw[6] = _test_boundary_after(_OPEN_TIME_MS, "1M") - 1
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(raw, "BTC/USDT", "1M")
        error = exc_info.value
        assert error.field == "raw_payload"
        assert_detached_sanitized_error(error, str(raw[0]))

    def test_factory_monthly_february_2024_inclusive_close(self) -> None:
        feb_open = _to_ms(datetime(2024, 2, 1, tzinfo=UTC))
        raw = make_binance_kline("1M")
        raw[0] = feb_open
        raw[6] = _to_ms(datetime(2024, 3, 1, tzinfo=UTC)) - 1
        candle = research_candle_from_binance_kline(raw, "BTC/USDT", "1M")
        assert candle.open_time_ms == feb_open
        assert candle.close_time_ms == _to_ms(datetime(2024, 3, 1, tzinfo=UTC))

    @pytest.mark.parametrize(
        "raw", [[], [object()] * 11, [object()] * 13, tuple(make_binance_kline())]
    )
    def test_factory_requires_exact_list_with_twelve_fields(self, raw: object) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(raw, "BTC/USDT", "1m")  # type: ignore[arg-type]
        assert exc_info.value.field == "raw_payload"

    @pytest.mark.parametrize(
        ("index", "value"),
        [
            (0, True),
            (0, 1.5),
            (0, "1704067200000"),
            (6, False),
            (6, "1704067259999"),
            (8, True),
            (8, 1.5),
            (8, "100"),
            (11, 0),
        ],
    )
    def test_factory_rejects_malformed_timestamp_trade_count_and_ignore_type(
        self,
        index: int,
        value: object,
    ) -> None:
        raw = make_binance_kline()
        raw[index] = value
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(raw, "BTC/USDT", "1m")
        assert exc_info.value.field == "raw_payload"

    @pytest.mark.parametrize("index", [1, 2, 3, 4, 5, 7, 9, 10])
    def test_factory_rejects_nonstring_numeric_fields(self, index: int) -> None:
        raw = make_binance_kline()
        raw[index] = 1
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(raw, "BTC/USDT", "1m")
        assert exc_info.value.field == "raw_payload"

    def test_factory_rejects_invalid_decimal_string(self) -> None:
        raw = make_binance_kline()
        raw[1] = "not-a-decimal"
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(raw, "BTC/USDT", "1m")
        assert exc_info.value.field == "open"

    @pytest.mark.parametrize("symbol", ["BTCUSDT", "BTC/", "/USDT", "", 1])
    def test_factory_rejects_noncanonical_symbol(self, symbol: Any) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(make_binance_kline(), symbol, "1m")
        assert exc_info.value.field == "symbol"

    def test_factory_validates_interval_before_duration_lookup(self) -> None:
        with pytest.raises(ResearchCandleValidationError) as exc_info:
            research_candle_from_binance_kline(make_binance_kline(), "BTC/USDT", "7m")
        assert exc_info.value.field == "interval"
