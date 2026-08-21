"""DATA-001 dataset metadata: validation, determinism, and round-trip safety."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from packages.market_data.datasets.metadata import (
    DATASET_SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
    DatasetMetadata,
    compute_dataset_checksum,
    derive_dataset_id,
)

COVERAGE_START = datetime(2026, 8, 1, tzinfo=UTC)
COVERAGE_END = COVERAGE_START + timedelta(days=7)
CHECKSUM = "a" * 64


def build_metadata(**overrides) -> DatasetMetadata:
    """Create valid metadata with per-field overrides for negative tests."""
    values = {
        "dataset_version": "1.0.0",
        "source": "websocket_archive",
        "exchange": "binance",
        "market_type": "spot",
        "symbols": ("btc/usdt", "ETH/USDT"),
        "intervals": ("1h", "1m"),
        "coverage_start": COVERAGE_START,
        "coverage_end": COVERAGE_END,
        "checksum": CHECKSUM,
        "record_count": 10_080,
        "quality_status": "complete",
    }
    values.update(overrides)
    return DatasetMetadata.create(**values)


class TestValidMetadata:
    """The happy path: fields, normalization, and identity derivation."""

    def test_create_normalizes_symbols_and_intervals(self) -> None:
        metadata = build_metadata(symbols=("eth/usdt", "btc/usdt"), intervals=("1m", "1h"))

        assert metadata.symbols == ("BTC/USDT", "ETH/USDT")
        assert metadata.intervals == ("1h", "1m")
        assert metadata.record_count == 10_080
        assert metadata.quality_status == "complete"
        assert metadata.schema_version == DATASET_SCHEMA_VERSION

    def test_dataset_id_is_deterministic_and_identity_based(self) -> None:
        first = build_metadata()
        second = build_metadata()

        assert first.dataset_id == second.dataset_id
        assert first.dataset_id == derive_dataset_id(
            source=first.source,
            exchange=first.exchange,
            market_type=first.market_type,
            symbols=first.symbols,
            intervals=first.intervals,
            coverage_start=first.coverage_start,
            coverage_end=first.coverage_end,
            schema_version=first.schema_version,
        )

    def test_dataset_id_stable_across_repaired_reexport(self) -> None:
        original = build_metadata(record_count=10_080, quality_status="suspected_gaps")
        repaired = build_metadata(record_count=10_081, quality_status="complete")

        assert repaired.dataset_id == original.dataset_id
        assert repaired.dataset_version == original.dataset_version

    def test_dataset_id_changes_when_coverage_changes(self) -> None:
        extended = build_metadata(coverage_end=COVERAGE_END + timedelta(days=1))

        assert extended.dataset_id != build_metadata().dataset_id


class TestTimestampValidation:
    """Requirement 4: timezone-aware UTC only."""

    def test_naive_coverage_start_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            build_metadata(coverage_start=datetime(2026, 8, 1))

    def test_naive_coverage_end_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            build_metadata(coverage_end=datetime(2026, 8, 8))

    def test_reversed_coverage_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="coverage_end must follow coverage_start"):
            build_metadata(coverage_end=COVERAGE_START - timedelta(hours=1))

    def test_offset_aware_timestamps_normalize_to_utc(self) -> None:
        offset_start = datetime(2026, 8, 1, 7, tzinfo=UTC) - timedelta(hours=7)
        metadata = build_metadata(coverage_start=offset_start)

        assert metadata.coverage_start == COVERAGE_START
        assert metadata.coverage_start.tzinfo is UTC


class TestVersionAndIdentityFields:
    """Requirement 2: deterministic, documented version format."""

    @pytest.mark.parametrize(
        "version",
        ["1.0.0", "0.0.1", "12.34.56", "999.999.999"],
    )
    def test_valid_versions_accepted(self, version: str) -> None:
        assert build_metadata(dataset_version=version).dataset_version == version

    @pytest.mark.parametrize(
        "version",
        ["1.0", "1", "v1.0.0", "1.0.0-rc1", "1.0.0+build5", "1..0", "1.0.", ""],
    )
    def test_invalid_versions_rejected(self, version: str) -> None:
        with pytest.raises(ValueError, match=r"MAJOR\.MINOR\.PATCH"):
            build_metadata(dataset_version=version)

    @pytest.mark.parametrize(
        "field",
        ["source", "symbols", "intervals"],
    )
    def test_empty_identity_fields_rejected(self, field: str) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            build_metadata(**{field: "" if field == "source" else ()})

    def test_blank_symbols_and_intervals_rejected(self) -> None:
        with pytest.raises(ValueError, match="canonical BASE/QUOTE"):
            build_metadata(symbols=("  ",))
        with pytest.raises(ValueError, match="non-empty"):
            build_metadata(intervals=(" ", ""))

    def test_invalid_quality_status_rejected(self) -> None:
        with pytest.raises(ValueError, match="invalid quality status"):
            build_metadata(quality_status="perfect")  # type: ignore[arg-type]

    def test_negative_record_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="record_count must be non-negative"):
            build_metadata(record_count=-1)


class TestSerialization:
    """Requirement 3 and 5: deterministic JSON, exact round-trip, no floats."""

    def test_to_json_is_deterministic_across_calls(self) -> None:
        metadata = build_metadata()

        assert metadata.to_json() == metadata.to_json()

    def test_json_round_trip_is_exact(self) -> None:
        metadata = build_metadata(symbols=("eth/usdt", "btc/usdt"), intervals=("1m", "1h"))

        restored = DatasetMetadata.from_json(metadata.to_json())

        assert restored == metadata
        assert restored.to_json() == metadata.to_json()

    def test_record_contains_no_floats(self) -> None:
        record = build_metadata().to_record()

        assert all(not isinstance(value, float) for value in record.values())

    def test_naive_iso_timestamp_in_json_is_rejected(self) -> None:
        metadata = build_metadata()
        record = metadata.to_record()
        record["coverage_start"] = "2026-08-01T00:00:00"

        with pytest.raises(ValueError, match="timezone-aware UTC"):
            DatasetMetadata.from_record(record)

    def test_missing_field_in_json_is_rejected(self) -> None:
        record = build_metadata().to_record()
        del record["checksum"]

        with pytest.raises(ValueError, match="missing field checksum"):
            DatasetMetadata.from_record(record)

    def test_non_object_json_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be an object"):
            DatasetMetadata.from_json("[1, 2, 3]")

    def test_checksum_hexdigest_validation(self) -> None:
        with pytest.raises(ValueError, match="SHA-256 hex digest"):
            build_metadata(checksum="A" * 64)
        with pytest.raises(ValueError, match="SHA-256 hex digest"):
            build_metadata(checksum="abc")


class TestDeriveDatasetIdCanonicalization:
    """Direct derive_dataset_id calls canonicalize identity inputs."""

    def test_mixed_case_and_duplicate_symbols_canonicalize_to_same_id(self) -> None:
        mixed = derive_dataset_id(
            source="websocket_archive",
            exchange="binance",
            market_type="spot",
            symbols=("btc/usdt", "BTC/USDT"),
            intervals=("1m", "1h"),
            coverage_start=COVERAGE_START,
            coverage_end=COVERAGE_END,
        )
        single = derive_dataset_id(
            source="websocket_archive",
            exchange="binance",
            market_type="spot",
            symbols=("BTC/USDT",),
            intervals=("1m", "1h"),
            coverage_start=COVERAGE_START,
            coverage_end=COVERAGE_END,
        )

        assert mixed == single
        assert mixed == build_metadata(symbols=("btc/usdt",)).dataset_id

    def test_aware_timestamps_normalize_to_utc_before_identity(self) -> None:
        offset_plus_7 = timezone(timedelta(hours=7))
        utc_id = derive_dataset_id(
            source="websocket_archive",
            exchange="binance",
            market_type="spot",
            symbols=("BTC/USDT",),
            intervals=("1m",),
            coverage_start=COVERAGE_START,
            coverage_end=COVERAGE_END,
        )
        offset_id = derive_dataset_id(
            source="websocket_archive",
            exchange="binance",
            market_type="spot",
            symbols=("BTC/USDT",),
            intervals=("1m",),
            coverage_start=datetime(2026, 8, 1, 7, tzinfo=offset_plus_7),
            coverage_end=datetime(2026, 8, 8, 7, tzinfo=offset_plus_7),
        )

        assert offset_id == utc_id

    def test_naive_timestamps_rejected_by_derive(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware UTC"):
            derive_dataset_id(
                source="websocket_archive",
                exchange="binance",
                market_type="spot",
                symbols=("BTC/USDT",),
                intervals=("1m",),
                coverage_start=datetime(2026, 8, 1),
                coverage_end=COVERAGE_END,
            )

    def test_empty_symbols_rejected_by_derive(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            derive_dataset_id(
                source="websocket_archive",
                exchange="binance",
                market_type="spot",
                symbols=(),
                intervals=("1m",),
                coverage_start=COVERAGE_START,
                coverage_end=COVERAGE_END,
            )

    def test_bool_schema_version_rejected_by_derive(self) -> None:
        with pytest.raises(ValueError, match="schema_version must be an integer"):
            derive_dataset_id(
                source="websocket_archive",
                exchange="binance",
                market_type="spot",
                symbols=("BTC/USDT",),
                intervals=("1m",),
                coverage_start=COVERAGE_START,
                coverage_end=COVERAGE_END,
                schema_version=True,  # type: ignore[arg-type]
            )


class TestFromRecordStrictTypes:
    """from_record rejects wrong runtime types instead of coercing them."""

    @pytest.mark.parametrize("bad", [1.5, True, "100"])
    def test_record_count_rejects_non_integer_types(self, bad: object) -> None:
        record = build_metadata().to_record()
        record["record_count"] = bad

        with pytest.raises(ValueError, match="record_count must be an integer"):
            DatasetMetadata.from_record(record)

    @pytest.mark.parametrize("bad", [1.5, True, "1"])
    def test_schema_version_rejects_non_integer_types(self, bad: object) -> None:
        record = build_metadata().to_record()
        record["schema_version"] = bad

        with pytest.raises(ValueError, match="schema_version must be an integer"):
            DatasetMetadata.from_record(record)

    @pytest.mark.parametrize(
        "field,value",
        [("source", 123), ("dataset_id", 5), ("checksum", None), ("quality_status", 7)],
    )
    def test_string_fields_reject_non_string_types(self, field: str, value: object) -> None:
        record = build_metadata().to_record()
        record[field] = value

        with pytest.raises(ValueError, match=f"{field} must be a string"):
            DatasetMetadata.from_record(record)

    def test_symbol_items_reject_non_string_types(self) -> None:
        record = build_metadata().to_record()
        record["symbols"] = ["BTC/USDT", 5]

        with pytest.raises(ValueError, match="canonical BASE/QUOTE"):
            DatasetMetadata.from_record(record)

    def test_interval_items_reject_non_string_types(self) -> None:
        record = build_metadata().to_record()
        record["intervals"] = ["1m", 5]

        with pytest.raises(ValueError, match="intervals must be strings"):
            DatasetMetadata.from_record(record)

    def test_valid_integer_record_round_trips_unaffected(self) -> None:
        metadata = build_metadata(record_count=10_080, schema_version=1)

        restored = DatasetMetadata.from_record(metadata.to_record())

        assert restored.record_count == 10_080
        assert restored.schema_version == 1


class TestExchangeAndMarketTypeMembership:
    """exchange/market_type must be exact public identifiers across all paths."""

    @pytest.mark.parametrize("exchange", ["okx", "coinbase", 123, None, True, ""])
    def test_invalid_exchange_rejected_by_create(self, exchange: object) -> None:
        with pytest.raises(ValueError, match="exchange"):
            build_metadata(exchange=exchange)  # type: ignore[arg-type]

    @pytest.mark.parametrize("market_type", ["futures", "margin", 5, None, False, ""])
    def test_invalid_market_type_rejected_by_create(self, market_type: object) -> None:
        with pytest.raises(ValueError, match="market_type"):
            build_metadata(market_type=market_type)  # type: ignore[arg-type]

    @pytest.mark.parametrize("exchange", ["okx", 123])
    def test_invalid_exchange_rejected_by_from_record(self, exchange: object) -> None:
        record = build_metadata().to_record()
        record["exchange"] = exchange

        with pytest.raises(ValueError, match="exchange must be"):
            DatasetMetadata.from_record(record)

    @pytest.mark.parametrize("market_type", ["futures", 123])
    def test_invalid_market_type_rejected_by_from_record(self, market_type: object) -> None:
        record = build_metadata().to_record()
        record["market_type"] = market_type

        with pytest.raises(ValueError, match="market_type must be"):
            DatasetMetadata.from_record(record)

    @pytest.mark.parametrize("exchange", ["okx", 123])
    def test_invalid_exchange_rejected_by_derive(self, exchange: object) -> None:
        with pytest.raises(ValueError, match="exchange must be"):
            derive_dataset_id(
                source="websocket_archive",
                exchange=exchange,  # type: ignore[arg-type]
                market_type="spot",
                symbols=("BTC/USDT",),
                intervals=("1m",),
                coverage_start=COVERAGE_START,
                coverage_end=COVERAGE_END,
            )

    @pytest.mark.parametrize("market_type", ["futures", 123])
    def test_invalid_market_type_rejected_by_derive(self, market_type: object) -> None:
        with pytest.raises(ValueError, match="market_type must be"):
            derive_dataset_id(
                source="websocket_archive",
                exchange="binance",
                market_type=market_type,  # type: ignore[arg-type]
                symbols=("BTC/USDT",),
                intervals=("1m",),
                coverage_start=COVERAGE_START,
                coverage_end=COVERAGE_END,
            )

    def test_direct_construction_rejects_invalid_exchange(self) -> None:
        with pytest.raises(ValueError, match="exchange must be 'binance'"):
            DatasetMetadata(
                dataset_id="x",
                dataset_version="1.0.0",
                source="websocket_archive",
                exchange="okx",  # type: ignore[arg-type]
                market_type="spot",
                symbols=("BTC/USDT",),
                intervals=("1m",),
                coverage_start=COVERAGE_START,
                coverage_end=COVERAGE_END,
                checksum=CHECKSUM,
                record_count=1,
                quality_status="complete",
            )

    def test_direct_construction_rejects_invalid_market_type(self) -> None:
        with pytest.raises(ValueError, match="market_type must be 'spot'"):
            DatasetMetadata(
                dataset_id="x",
                dataset_version="1.0.0",
                source="websocket_archive",
                exchange="binance",
                market_type="futures",  # type: ignore[arg-type]
                symbols=("BTC/USDT",),
                intervals=("1m",),
                coverage_start=COVERAGE_START,
                coverage_end=COVERAGE_END,
                checksum=CHECKSUM,
                record_count=1,
                quality_status="complete",
            )

    def test_valid_exchange_and_market_type_round_trip(self) -> None:
        metadata = build_metadata()

        restored = DatasetMetadata.from_json(metadata.to_json())

        assert restored.exchange == "binance"
        assert restored.market_type == "spot"


class TestChecksumsAndSchemaCompatibility:
    """Requirement 6 and 7: integrity helpers and schema-version policy."""

    def test_record_checksum_is_deterministic_sha256(self) -> None:
        payload = b'{"kline": true}'

        assert compute_dataset_checksum(payload) == compute_dataset_checksum(payload)
        assert len(compute_dataset_checksum(payload)) == 64
        assert compute_dataset_checksum(b"") != compute_dataset_checksum(payload)

    def test_metadata_checksum_is_deterministic(self) -> None:
        metadata = build_metadata()

        assert metadata.metadata_checksum() == metadata.metadata_checksum()
        assert len(metadata.metadata_checksum()) == 64
        assert build_metadata(record_count=1).metadata_checksum() != metadata.metadata_checksum()

    def test_supported_schema_versions_policy(self) -> None:
        assert SUPPORTED_SCHEMA_VERSIONS == {DATASET_SCHEMA_VERSION}

    def test_unsupported_schema_version_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="unsupported schema version 2"):
            build_metadata(schema_version=2)

    def test_schema_version_round_trips_preserved(self) -> None:
        metadata = build_metadata()

        restored = DatasetMetadata.from_json(metadata.to_json())

        assert restored.schema_version == DATASET_SCHEMA_VERSION

    def test_metadata_never_contains_secret_shaped_fields(self) -> None:
        record = build_metadata().to_record()

        assert set(record) == {
            "dataset_id",
            "dataset_version",
            "source",
            "exchange",
            "market_type",
            "symbols",
            "intervals",
            "coverage_start",
            "coverage_end",
            "schema_version",
            "checksum",
            "record_count",
            "quality_status",
        }
