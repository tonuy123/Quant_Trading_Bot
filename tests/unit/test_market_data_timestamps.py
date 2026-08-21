"""DATA-004: strict timestamp unit and UTC normalization tests (A-H)."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta, timezone

import pytest

from packages.market_data.datasets import (
    NormalizedTimestampRange,
    TimestampNormalizationError,
    normalize_datetime_to_utc,
    normalize_epoch_to_utc,
    normalize_range_to_utc,
    normalize_timestamp_to_utc,
    utc_to_epoch_ms,
)
from packages.market_data.datasets.metadata import (
    DatasetMetadata,
    compute_dataset_checksum,
    derive_dataset_id,
)
from packages.market_data.datasets.timestamps import _EPOCH

KNOWN_MS = 1_690_000_000_000  # 2023-07-22T02:26:40Z


class TestUnitConversion:
    """A. Every explicit unit maps to the same instant."""

    def test_seconds_to_utc(self) -> None:
        result = normalize_epoch_to_utc(1_690_000_000, "s")

        assert result == datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC)
        assert result.tzinfo is UTC

    def test_milliseconds_to_utc(self) -> None:
        result = normalize_epoch_to_utc(KNOWN_MS, "ms")

        assert result == datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC)

    def test_microseconds_to_utc(self) -> None:
        result = normalize_epoch_to_utc(KNOWN_MS * 1000, "us")

        assert result == datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC)

    def test_nanoseconds_to_utc(self) -> None:
        result = normalize_epoch_to_utc(KNOWN_MS * 1_000_000, "ns")

        assert result == datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC)

    def test_same_instant_in_different_units(self) -> None:
        by_s = normalize_epoch_to_utc(1_690_000_000, "s")
        by_ms = normalize_epoch_to_utc(KNOWN_MS, "ms")
        by_us = normalize_epoch_to_utc(KNOWN_MS * 1000, "us")
        by_ns = normalize_epoch_to_utc(KNOWN_MS * 1_000_000, "ns")

        assert by_s == by_ms == by_us == by_ns

    def test_epoch_ms_output_deterministic(self) -> None:
        instant = datetime(2023, 7, 22, 4, 26, 40, 123456, tzinfo=UTC)

        first = utc_to_epoch_ms(instant)
        second = utc_to_epoch_ms(instant)

        assert first == second
        assert first == KNOWN_MS + 123
        assert isinstance(first, int)

    def test_epoch_ms_round_trip(self) -> None:
        original = normalize_epoch_to_utc(KNOWN_MS, "ms")

        assert utc_to_epoch_ms(original) == KNOWN_MS


class TestExplicitUnitRequirement:
    """B. Units are mandatory and exact -- never inferred."""

    def test_missing_unit_rejected_for_int(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="unit is required"):
            normalize_timestamp_to_utc(KNOWN_MS)

    def test_missing_unit_rejected_for_epoch(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="unit"):
            normalize_epoch_to_utc(KNOWN_MS, None)  # type: ignore[arg-type]

    def test_unsupported_unit_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="unit must be one of"):
            normalize_epoch_to_utc(KNOWN_MS, "minutes")  # type: ignore[arg-type]

    def test_whitespace_unit_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="unit must be one of"):
            normalize_epoch_to_utc(KNOWN_MS, " ms")  # type: ignore[arg-type]

    def test_uppercase_unit_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="unit must be one of"):
            normalize_epoch_to_utc(KNOWN_MS, "MS")  # type: ignore[arg-type]

    def test_no_magnitude_based_inference(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="unit is required"):
            normalize_timestamp_to_utc(1_690_000_000_000_000)
        with pytest.raises(TimestampNormalizationError, match="unit is required"):
            normalize_timestamp_to_utc(1_690_000_000)

    def test_unit_with_datetime_rejected(self) -> None:
        instant = datetime(2023, 7, 22, tzinfo=UTC)

        with pytest.raises(TimestampNormalizationError, match="unit must be None"):
            normalize_timestamp_to_utc(instant, "ms")


class TestStrictTypes:
    """C. Only real integers are accepted as epoch values."""

    def test_bool_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="integer"):
            normalize_epoch_to_utc(True, "ms")

    def test_float_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="integer"):
            normalize_epoch_to_utc(1_690_000_000_000.0, "ms")

    def test_string_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="integer"):
            normalize_epoch_to_utc("1690000000000", "ms")

    def test_none_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="integer"):
            normalize_epoch_to_utc(None, "ms")  # type: ignore[arg-type]

    def test_invalid_object_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="integer"):
            normalize_epoch_to_utc(object(), "ms")  # type: ignore[arg-type]

    def test_decimal_rejected(self) -> None:
        from decimal import Decimal

        with pytest.raises(TimestampNormalizationError, match="integer"):
            normalize_epoch_to_utc(Decimal("1690000000000"), "ms")  # type: ignore[arg-type]


class TestTimezone:
    """D. Aware input preserves the instant; naive input is rejected."""

    def test_naive_datetime_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="naive"):
            normalize_datetime_to_utc(datetime(2023, 7, 22, 4, 26, 40))

    def test_utc_aware_accepted(self) -> None:
        instant = datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC)

        result = normalize_datetime_to_utc(instant)

        assert result == instant
        assert result.tzinfo is UTC

    def test_plus0700_normalized_to_utc(self) -> None:
        plus7 = datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7)))

        result = normalize_datetime_to_utc(plus7)

        assert result == datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)

    def test_minus0500_normalized_to_utc(self) -> None:
        minus5 = datetime(2023, 7, 21, 21, 26, 40, tzinfo=timezone(timedelta(hours=-5)))

        result = normalize_datetime_to_utc(minus5)

        assert result == datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)

    def test_plus0700_equivalent_to_utc_input(self) -> None:
        plus7 = datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7)))
        utc = datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)

        assert normalize_datetime_to_utc(plus7) == normalize_datetime_to_utc(utc)

    def test_output_always_aware_with_zero_offset(self) -> None:
        for raw in (
            datetime(2023, 7, 22, tzinfo=UTC),
            datetime(2023, 7, 22, 9, tzinfo=timezone(timedelta(hours=7))),
            normalize_epoch_to_utc(KNOWN_MS, "ms"),
        ):
            result = normalize_datetime_to_utc(raw)
            assert result.tzinfo is not None
            assert result.utcoffset() == timedelta(0)


class TestPrecision:
    """E. Integer arithmetic only; sub-millisecond policy is explicit."""

    def test_no_float_arithmetic_in_epoch_ms(self) -> None:
        instant = datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)

        result = utc_to_epoch_ms(instant)

        assert result == 253_402_300_799_999
        assert isinstance(result, int)

    def test_microsecond_exact_epoch_ms(self) -> None:
        instant = datetime(2023, 7, 22, 4, 26, 40, 123456, tzinfo=UTC)

        assert utc_to_epoch_ms(instant) == KNOWN_MS + 123

    def test_microsecond_truncated_toward_zero(self) -> None:
        instant = datetime(2023, 7, 22, 4, 26, 40, 999, tzinfo=UTC)

        assert utc_to_epoch_ms(instant) == KNOWN_MS

    def test_nanosecond_sub_microsecond_residue_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="multiple of 1000"):
            normalize_epoch_to_utc(KNOWN_MS * 1_000_000 + 1, "ns")

    def test_nanosecond_exact_multiple_accepted(self) -> None:
        result = normalize_epoch_to_utc(KNOWN_MS * 1_000_000 + 500_000, "ns")

        assert result == datetime(2023, 7, 22, 4, 26, 40, 500, tzinfo=UTC)

    def test_epoch_from_datetime_uses_int_path(self) -> None:
        result = normalize_timestamp_to_utc(KNOWN_MS, "ms")

        assert result == normalize_datetime_to_utc(datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC))


class TestRange:
    """F. Half-open ``[start, end)`` ranges with strict ordering."""

    def test_valid_range(self) -> None:
        result = normalize_range_to_utc(KNOWN_MS, KNOWN_MS + 60_000, "ms")

        assert isinstance(result, NormalizedTimestampRange)
        assert result.start == datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC)
        assert result.end == datetime(2023, 7, 22, 4, 27, 40, tzinfo=UTC)

    def test_start_equals_end_is_empty_range(self) -> None:
        result = normalize_range_to_utc(KNOWN_MS, KNOWN_MS, "ms")

        assert result.start == result.end

    def test_end_before_start_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="must not precede"):
            normalize_range_to_utc(KNOWN_MS + 60_000, KNOWN_MS, "ms")

    def test_range_timezone_normalization(self) -> None:
        start = datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7)))
        end = datetime(2023, 7, 22, 9, 27, 40, tzinfo=timezone(timedelta(hours=7)))

        result = normalize_range_to_utc(start, end)

        assert result.start == datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)
        assert result.end == datetime(2023, 7, 22, 2, 27, 40, tzinfo=UTC)

    def test_mixed_offsets_same_instant(self) -> None:
        start_utc = datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)
        end_plus7 = datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7)))

        result = normalize_range_to_utc(start_utc, end_plus7)

        assert result.start == result.end

    def test_negative_epoch_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="non-negative"):
            normalize_epoch_to_utc(-1, "s")

    def test_overflow_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="out of supported datetime range"):
            normalize_epoch_to_utc(10**19, "s")
        with pytest.raises(TimestampNormalizationError, match="out of supported datetime range"):
            normalize_epoch_to_utc(2**63, "ms")

    def test_pre_1970_datetime_rejected_for_epoch_ms(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="non-negative"):
            utc_to_epoch_ms(datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC))

    def test_epoch_zero_is_epoch(self) -> None:
        assert normalize_epoch_to_utc(0, "s") == _EPOCH
        assert utc_to_epoch_ms(_EPOCH) == 0


class TestNormalizedTimestampRangeDirectConstruction:
    """Direct construction enforces the same invariants as the normalizer."""

    def _aware(self) -> datetime:
        return datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC)

    def test_direct_naive_start_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="naive"):
            NormalizedTimestampRange(start=datetime(2023, 7, 22, 4, 26, 40), end=self._aware())

    def test_direct_naive_end_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="naive"):
            NormalizedTimestampRange(start=self._aware(), end=datetime(2023, 7, 22, 4, 27, 40))

    def test_direct_non_datetime_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="datetime"):
            NormalizedTimestampRange(start=KNOWN_MS, end=self._aware())  # type: ignore[arg-type]
        with pytest.raises(TimestampNormalizationError, match="datetime"):
            NormalizedTimestampRange(start="2023-07-22T04:26:40Z", end=self._aware())  # type: ignore[arg-type]
        with pytest.raises(TimestampNormalizationError, match="datetime"):
            NormalizedTimestampRange(start=self._aware(), end=None)  # type: ignore[arg-type]

    def test_direct_offset_aware_normalized_to_utc(self) -> None:
        plus7 = datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7)))

        result = NormalizedTimestampRange(start=plus7, end=plus7 + timedelta(hours=1))

        assert result.start == datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)
        assert result.start.tzinfo is UTC
        assert result.end == datetime(2023, 7, 22, 3, 26, 40, tzinfo=UTC)

    def test_direct_offset_mixed_offsets_same_instant(self) -> None:
        utc = datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)
        plus7 = datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7)))

        result = NormalizedTimestampRange(start=utc, end=plus7)

        assert result.start == result.end

    def test_direct_end_before_start_rejected(self) -> None:
        with pytest.raises(TimestampNormalizationError, match="must not precede"):
            NormalizedTimestampRange(start=self._aware() + timedelta(hours=1), end=self._aware())

    def test_direct_start_equals_end_accepted(self) -> None:
        result = NormalizedTimestampRange(start=self._aware(), end=self._aware())

        assert result.start == result.end

    def test_direct_construction_matches_normalizer_output(self) -> None:
        direct = NormalizedTimestampRange(
            start=datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7))),
            end=datetime(2023, 7, 22, 9, 27, 40, tzinfo=timezone(timedelta(hours=7))),
        )
        normalized = normalize_range_to_utc(
            datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7))),
            datetime(2023, 7, 22, 9, 27, 40, tzinfo=timezone(timedelta(hours=7))),
        )

        assert direct == normalized


class TestDatasetCompatibility:
    """G. DATA-001 identity and DATA-002 contracts stay untouched."""

    def test_normalized_coverage_drives_same_dataset_id(self) -> None:
        plus7 = datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7)))
        utc = datetime(2023, 7, 22, 2, 26, 40, tzinfo=UTC)
        end = datetime(2023, 7, 23, tzinfo=UTC)
        kwargs = {
            "source": "binance_public_rest",
            "exchange": "binance",
            "market_type": "spot",
            "symbols": ("BTC/USDT",),
            "intervals": ("1m",),
        }

        id_plus7 = derive_dataset_id(
            coverage_start=normalize_datetime_to_utc(plus7), coverage_end=end, **kwargs
        )
        id_utc = derive_dataset_id(coverage_start=utc, coverage_end=end, **kwargs)

        assert id_plus7 == id_utc

    def test_metadata_coverage_remains_utc_aware(self) -> None:
        start = normalize_epoch_to_utc(KNOWN_MS, "ms")
        end = normalize_epoch_to_utc(KNOWN_MS + 60_000, "ms")
        payload = b'{"open_time": 1690000000000}\n'
        metadata = DatasetMetadata.create(
            dataset_version="1.0.0",
            source="binance_public_rest",
            exchange="binance",
            market_type="spot",
            symbols=("BTC/USDT",),
            intervals=("1m",),
            coverage_start=start,
            coverage_end=end,
            checksum=compute_dataset_checksum(payload),
            record_count=1,
            quality_status="complete",
        )

        assert metadata.coverage_start == start
        assert metadata.coverage_end == end
        assert metadata.coverage_start.tzinfo is UTC
        assert metadata.coverage_end.utcoffset() == timedelta(0)
        assert metadata.to_record()["coverage_start"].endswith("+00:00")

    def test_data_002_manifest_schema_untouched(self) -> None:
        from packages.market_data.datasets import DownloadManifest

        manifest = DownloadManifest(
            dataset_id="dataset-id",
            dataset_version="1.0.0",
            downloader_version="1.0.0",
            schema_version=1,
            source="binance_public_rest",
            exchange="binance",
            market_type="spot",
            symbols=("BTC/USDT",),
            intervals=("1m",),
            requested_start=datetime(2023, 7, 22, 4, 26, 40, tzinfo=UTC),
            requested_end=datetime(2023, 7, 23, tzinfo=UTC),
            actual_start=None,
            actual_end=None,
            record_count=0,
            files=(),
            completion_status="complete",
            failure=None,
            page_limit=1000,
            resume=False,
            server_time=None,
        )

        assert manifest.completion_status == "complete"

    def test_no_checksum_mutation(self) -> None:
        payload = b"raw-record-bytes"
        before = compute_dataset_checksum(payload)

        normalize_epoch_to_utc(KNOWN_MS, "ms")
        normalize_datetime_to_utc(datetime(2023, 7, 22, tzinfo=UTC))
        utc_to_epoch_ms(datetime(2023, 7, 22, tzinfo=UTC))

        assert compute_dataset_checksum(payload) == before

    def test_no_filesystem_side_effects(self, tmp_path: pytest.TempPathFactory) -> None:
        import glob

        before = sorted(glob.glob(str(tmp_path / "**"), recursive=True))

        normalize_range_to_utc(KNOWN_MS, KNOWN_MS + 60_000, "ms")

        after = sorted(glob.glob(str(tmp_path / "**"), recursive=True))
        assert after == before


class TestCanonicalSerialization:
    """H. Canonical output serializes with an explicit UTC offset."""

    def test_isoformat_contains_utc_offset(self) -> None:
        result = normalize_epoch_to_utc(KNOWN_MS, "ms")

        assert result.isoformat().endswith("+00:00")
        assert re.search(r"[+-]\d{2}:\d{2}$", result.isoformat())

    def test_normalized_range_never_naive(self) -> None:
        result = normalize_range_to_utc(
            datetime(2023, 7, 22, 9, 26, 40, tzinfo=timezone(timedelta(hours=7))),
            datetime(2023, 7, 22, 9, 27, 40, tzinfo=timezone(timedelta(hours=7))),
        )

        assert result.start.tzinfo is not None
        assert result.end.tzinfo is not None
        assert result.start.utcoffset() == timedelta(0)
        assert result.end.utcoffset() == timedelta(0)

    def test_range_frozen(self) -> None:
        result = normalize_range_to_utc(KNOWN_MS, KNOWN_MS + 60_000, "ms")

        with pytest.raises((AttributeError, TypeError)):
            result.start = datetime(2024, 1, 1, tzinfo=UTC)  # type: ignore[misc]
