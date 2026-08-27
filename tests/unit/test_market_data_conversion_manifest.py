"""DATA-005 B-2B slice 2A: pure research-manifest contract tests."""

from __future__ import annotations

import ast
import inspect
import json
import subprocess
import sys
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import UTC, datetime, timedelta, timezone
from datetime import (
    tzinfo as tzinfo_base,  # the ABC base class, NOT the getset_descriptor
)
from hashlib import sha256

import pytest

import packages.market_data.datasets.conversion_manifest as manifest_module
from packages.market_data.datasets.conversion_manifest import (
    RESEARCH_CONVERTER_VERSION,
    RESEARCH_DATASET_VERSION,
    RESEARCH_MANIFEST_FILE,
    ResearchDatasetManifest,
    ResearchFileArtifact,
    ResearchFilePlan,
    ResearchManifestValidationError,
    build_research_manifest,
)
from packages.market_data.datasets.conversion_stream import StreamConversionReport
from packages.market_data.datasets.downloader import (
    DATASET_DOWNLOAD_VERSION,
    DOWNLOADER_VERSION,
    DownloadFailure,
    DownloadManifest,
    OutputFileInfo,
)
from packages.market_data.datasets.research_format import (
    RESEARCH_SCHEMA_VERSION,
    RESEARCH_SOURCE,
)

START = datetime(2024, 1, 1, tzinfo=UTC)
END = START + timedelta(days=1)
OPEN_MS = 1_704_067_200_000
CLOSE_MS = OPEN_MS + 60_000
EMPTY_SHA256 = sha256(b"").hexdigest()
RAW_SHA256 = "a" * 64
RAW_MANIFEST_SHA256 = "b" * 64
RESEARCH_SHA256 = "c" * 64
FAILURE_SHA256 = "d" * 64
AGGREGATE_RESEARCH_SHA256 = "e" * 64
AGGREGATE_FAILURE_SHA256 = "f" * 64


def make_report(**overrides: object) -> StreamConversionReport:
    values: dict[str, object] = {
        "file": "BTC-USDT-1m.jsonl",
        "lines_seen": 1,
        "records_written": 1,
        "records_quarantined": 0,
        "coverage_start_ms": OPEN_MS,
        "coverage_end_ms": CLOSE_MS,
        "research_sha256": RESEARCH_SHA256,
        "failure_sha256": EMPTY_SHA256,
        "research_bytes": 128,
        "failure_bytes": 0,
        "status": "success",
    }
    values.update(overrides)
    return StreamConversionReport(**values)  # type: ignore[arg-type]


def output_names(symbol: str, interval: str) -> tuple[str, str, str]:
    prefix = f"{symbol.replace('/', '-')}-{interval}"
    return f"{prefix}.jsonl", f"{prefix}.jsonl", f"{prefix}.failures.jsonl"


def make_artifact(
    *,
    symbol: str = "BTC/USDT",
    interval: str = "1m",
    report: StreamConversionReport | None = None,
    **overrides: object,
) -> ResearchFileArtifact:
    raw_name, research_name, failure_name = output_names(symbol, interval)
    values: dict[str, object] = {
        "raw_name": raw_name,
        "research_name": research_name,
        "failure_name": failure_name,
        "symbol": symbol,
        "interval": interval,
        "raw_sha256": RAW_SHA256,
        "raw_bytes": 256,
        "report": make_report(file=raw_name) if report is None else report,
    }
    values.update(overrides)
    return ResearchFileArtifact.from_stream_report(**values)  # type: ignore[arg-type]


def make_output_file(
    name: str = "BTC-USDT-1m.jsonl",
    *,
    records: object = 1,
    range_start: object = START,
    range_end: object = START + timedelta(minutes=1),
) -> OutputFileInfo:
    return OutputFileInfo(
        name=name,
        records=records,  # type: ignore[arg-type]
        range_start=range_start,  # type: ignore[arg-type]
        range_end=range_end,  # type: ignore[arg-type]
    )


def make_raw_manifest(**overrides: object) -> DownloadManifest:
    files = overrides.pop("files", (make_output_file(),))
    record_count = overrides.pop(
        "record_count",
        sum(file_info.records for file_info in files),  # type: ignore[union-attr]
    )
    values: dict[str, object] = {
        "dataset_id": "raw-request-identity",
        "dataset_version": DATASET_DOWNLOAD_VERSION,
        "downloader_version": DOWNLOADER_VERSION,
        "schema_version": 1,
        "source": RESEARCH_SOURCE,
        "exchange": "binance",
        "market_type": "spot",
        "symbols": ("BTC/USDT",),
        "intervals": ("1m",),
        "requested_start": START,
        "requested_end": END,
        "actual_start": START if files else None,
        "actual_end": START + timedelta(minutes=1) if files else None,
        "record_count": record_count,
        "files": files,
        "completion_status": "complete",
        "failure": None,
        "page_limit": 1000,
        "resume": False,
        "server_time": END,
    }
    values.update(overrides)
    return DownloadManifest(**values)  # type: ignore[arg-type]


def build_manifest(
    *,
    raw_manifest: DownloadManifest | None = None,
    artifacts: list[ResearchFileArtifact] | tuple[ResearchFileArtifact, ...] | None = None,
    completion_status: str = "complete",
    research_checksum: str | None = None,
    failure_checksum: str | None = None,
) -> ResearchDatasetManifest:
    raw = make_raw_manifest() if raw_manifest is None else raw_manifest
    completed = [make_artifact()] if artifacts is None else artifacts
    research_bytes = sum(artifact.research_bytes for artifact in completed)
    failure_bytes = sum(artifact.failure_bytes for artifact in completed)
    return build_research_manifest(
        raw,
        raw_manifest_sha256=RAW_MANIFEST_SHA256,
        files=completed,
        research_checksum=(EMPTY_SHA256 if research_bytes == 0 else AGGREGATE_RESEARCH_SHA256)
        if research_checksum is None
        else research_checksum,
        failure_checksum=(EMPTY_SHA256 if failure_bytes == 0 else AGGREGATE_FAILURE_SHA256)
        if failure_checksum is None
        else failure_checksum,
        max_line_bytes=1_048_576,
        completion_status=completion_status,  # type: ignore[arg-type]
    )


def make_two_file_inputs(
    *,
    second_report: StreamConversionReport | None = None,
) -> tuple[DownloadManifest, tuple[ResearchFileArtifact, ResearchFileArtifact]]:
    btc = make_artifact()
    eth_report = (
        make_report(
            file="ETH-USDT-1m.jsonl",
            coverage_start_ms=OPEN_MS + 60_000,
            coverage_end_ms=CLOSE_MS + 60_000,
        )
        if second_report is None
        else second_report
    )
    eth = make_artifact(symbol="ETH/USDT", report=eth_report)
    raw = make_raw_manifest(
        symbols=("BTC/USDT", "ETH/USDT"),
        files=(
            make_output_file("BTC-USDT-1m.jsonl"),
            make_output_file("ETH-USDT-1m.jsonl"),
        ),
        record_count=2,
    )
    return raw, (btc, eth)


def assert_no_float(value: object) -> None:
    assert not isinstance(value, float)
    if isinstance(value, dict):
        for nested in value.values():
            assert_no_float(nested)
    elif isinstance(value, list):
        for nested in value:
            assert_no_float(nested)


def test_public_constants_are_exact() -> None:
    assert RESEARCH_DATASET_VERSION == "1.0.1"
    assert RESEARCH_CONVERTER_VERSION == "1.0.0"
    assert RESEARCH_MANIFEST_FILE == "research_manifest.json"


def test_artifact_from_success_report_copies_exact_fields() -> None:
    report = make_report()
    artifact = make_artifact(report=report)

    assert artifact.lines_seen == report.lines_seen
    assert artifact.records_written == report.records_written
    assert artifact.records_quarantined == report.records_quarantined
    assert artifact.research_sha256 == report.research_sha256
    assert artifact.failure_sha256 == report.failure_sha256
    assert artifact.research_bytes == report.research_bytes
    assert artifact.failure_bytes == report.failure_bytes
    assert artifact.status == "success"


def test_valid_partial_artifact() -> None:
    report = make_report(
        lines_seen=2,
        records_quarantined=1,
        failure_sha256=FAILURE_SHA256,
        failure_bytes=64,
        status="partial",
    )
    artifact = make_artifact(report=report)

    assert artifact.records_written == 1
    assert artifact.records_quarantined == 1
    assert artifact.status == "partial"


def test_valid_all_quarantined_artifact_has_no_coverage() -> None:
    report = make_report(
        lines_seen=2,
        records_written=0,
        records_quarantined=2,
        coverage_start_ms=None,
        coverage_end_ms=None,
        research_sha256=EMPTY_SHA256,
        failure_sha256=FAILURE_SHA256,
        research_bytes=0,
        failure_bytes=64,
        status="partial",
    )
    artifact = make_artifact(report=report)

    assert artifact.coverage_start_ms is None
    assert artifact.coverage_end_ms is None
    assert artifact.status == "partial"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("raw_bytes", True),
        ("raw_bytes", -1),
        ("raw_bytes", 1.5),
        ("raw_bytes", "1"),
    ],
)
def test_artifact_rejects_non_real_non_negative_integers(field: str, value: object) -> None:
    with pytest.raises(ResearchManifestValidationError) as raised:
        make_artifact(**{field: value})

    assert raised.value.field == field


@pytest.mark.parametrize(
    ("field", "value"),
    [("raw_sha256", "A" * 64), ("raw_sha256", "a" * 63), ("raw_sha256", 7)],
)
def test_artifact_rejects_invalid_lowercase_sha256(field: str, value: object) -> None:
    with pytest.raises(ResearchManifestValidationError) as raised:
        make_artifact(**{field: value})

    assert raised.value.field == field


@pytest.mark.parametrize(
    "raw_name",
    [
        "",
        ".",
        "..",
        "/absolute.jsonl",
        "dir/file.jsonl",
        "dir\\file.jsonl",
        "bad\0x",
        "bad\rx",
        "bad\nx",
    ],
)
def test_artifact_rejects_unsafe_basename(raw_name: str) -> None:
    with pytest.raises(ResearchManifestValidationError, match="safe basename"):
        make_artifact(raw_name=raw_name)


@pytest.mark.parametrize("symbol", ["btc/usdt", "BTCUSDT", "BTC/", "/USDT"])
def test_artifact_rejects_noncanonical_symbol(symbol: str) -> None:
    with pytest.raises(ResearchManifestValidationError) as raised:
        make_artifact(symbol=symbol)

    assert raised.value.field == "symbol"


def test_artifact_rejects_unsupported_interval() -> None:
    with pytest.raises(ResearchManifestValidationError) as raised:
        make_artifact(interval="7m")

    assert raised.value.field == "interval"


def test_month_and_minute_filenames_remain_case_sensitive() -> None:
    minute = make_artifact()
    month = make_artifact(
        interval="1M",
        report=make_report(file="BTC-USDT-1M.jsonl"),
    )

    assert minute.raw_name == "BTC-USDT-1m.jsonl"
    assert month.raw_name == "BTC-USDT-1M.jsonl"
    assert minute.raw_name != month.raw_name


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("raw_name", "BTC-USDT-5m.jsonl"),
        ("research_name", "other.jsonl"),
        ("failure_name", "BTC-USDT-1m.failed.jsonl"),
    ],
)
def test_artifact_requires_exact_output_naming(field: str, value: str) -> None:
    report = make_report(file=value) if field == "raw_name" else None
    with pytest.raises(ResearchManifestValidationError) as raised:
        make_artifact(report=report, **{field: value})

    assert raised.value.field == field


@pytest.mark.parametrize("report", [make_report(file="other.jsonl"), object()])
def test_artifact_rejects_report_file_or_type_mismatch(report: object) -> None:
    with pytest.raises(ResearchManifestValidationError) as raised:
        make_artifact(report=report)  # type: ignore[arg-type]

    assert raised.value.field == "report"


@pytest.mark.parametrize(
    "overrides",
    [
        {"lines_seen": 2},
        {"status": "partial"},
        {"coverage_start_ms": None},
        {"coverage_end_ms": OPEN_MS},
    ],
)
def test_artifact_direct_construction_reuses_stream_invariants(
    overrides: dict[str, object],
) -> None:
    values = asdict(make_artifact())
    values.update(overrides)

    with pytest.raises(ResearchManifestValidationError) as raised:
        ResearchFileArtifact(**values)

    assert raised.value.field == "report"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.parametrize(
    ("expected_field", "artifact_overrides", "report_overrides"),
    [
        ("research_bytes", {}, {"research_bytes": 0}),
        (
            "failure_bytes",
            {},
            {
                "lines_seen": 2,
                "records_quarantined": 1,
                "failure_bytes": 0,
                "status": "partial",
            },
        ),
        ("raw_bytes", {"raw_bytes": 0}, {}),
    ],
)
def test_artifact_rejects_counter_and_byte_presence_mismatch(
    expected_field: str,
    artifact_overrides: dict[str, object],
    report_overrides: dict[str, object],
) -> None:
    report = make_report(**report_overrides)

    with pytest.raises(ResearchManifestValidationError) as raised:
        make_artifact(report=report, **artifact_overrides)

    assert raised.value.field == expected_field
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


@pytest.mark.parametrize(
    ("channel", "byte_presence"),
    [
        ("raw", "zero"),
        ("research", "zero"),
        ("failure", "zero"),
        ("raw", "nonzero"),
        ("research", "nonzero"),
        ("failure", "nonzero"),
    ],
)
def test_artifact_digest_must_match_exact_byte_presence(
    channel: str,
    byte_presence: str,
) -> None:
    values = asdict(make_artifact())
    if byte_presence == "zero":
        if channel == "raw":
            values.update(lines_seen=0, raw_bytes=0)
        elif channel == "research":
            values.update(records_written=0, research_bytes=0)
        else:
            values["failure_sha256"] = FAILURE_SHA256
    elif channel == "raw":
        values["raw_sha256"] = EMPTY_SHA256
    elif channel == "research":
        values["research_sha256"] = EMPTY_SHA256
    else:
        values.update(
            lines_seen=2,
            records_quarantined=1,
            failure_bytes=32,
            failure_sha256=EMPTY_SHA256,
            status="partial",
        )

    with pytest.raises(ResearchManifestValidationError) as raised:
        ResearchFileArtifact(**values)

    assert raised.value.field == f"{channel}_sha256"


def test_artifact_and_manifest_are_frozen() -> None:
    artifact = make_artifact()
    with pytest.raises(FrozenInstanceError):
        artifact.raw_bytes = 0  # type: ignore[misc]
    manifest = build_manifest()
    with pytest.raises(FrozenInstanceError):
        manifest.lines_seen = 0  # type: ignore[misc]


def test_valid_complete_manifest() -> None:
    manifest = build_manifest()

    assert manifest.completion_status == "complete"
    assert manifest.conversion_status == "success"
    assert manifest.schema_version == RESEARCH_SCHEMA_VERSION
    assert manifest.expected_raw_files == ("BTC-USDT-1m.jsonl",)
    assert manifest.files == (make_artifact(),)
    for field in ("raw_manifest_sha256", "research_checksum", "failure_checksum"):
        with pytest.raises(ResearchManifestValidationError):
            replace(manifest, **{field: "A" * 64})


def test_valid_incomplete_manifest_uses_proper_subset() -> None:
    raw, artifacts = make_two_file_inputs()
    manifest = build_manifest(
        raw_manifest=raw,
        artifacts=[artifacts[0]],
        completion_status="incomplete",
    )

    assert manifest.completion_status == "incomplete"
    assert manifest.files == (artifacts[0],)
    assert manifest.expected_raw_files == ("BTC-USDT-1m.jsonl", "ETH-USDT-1m.jsonl")


def test_empty_raw_dataset_can_be_physically_complete() -> None:
    raw = make_raw_manifest(files=(), record_count=0, actual_start=None, actual_end=None)
    manifest = build_manifest(raw_manifest=raw, artifacts=[])

    assert manifest.expected_raw_files == ()
    assert manifest.files == ()
    assert manifest.coverage_start is None
    assert manifest.research_checksum == EMPTY_SHA256
    assert manifest.failure_checksum == EMPTY_SHA256


@pytest.mark.parametrize(
    ("completion_status", "artifact_count"),
    [("complete", 1), ("incomplete", 2)],
)
def test_completion_status_enforces_exact_set_relation(
    completion_status: str,
    artifact_count: int,
) -> None:
    raw, artifacts = make_two_file_inputs()
    with pytest.raises(ResearchManifestValidationError) as raised:
        build_manifest(
            raw_manifest=raw,
            artifacts=list(artifacts[:artifact_count]),
            completion_status=completion_status,
        )

    assert raised.value.field == "completion_status"


def test_builder_rejects_unknown_completed_raw_file() -> None:
    raw = make_raw_manifest(symbols=("BTC/USDT", "ETH/USDT"))
    unknown = make_artifact(
        symbol="ETH/USDT",
        report=make_report(file="ETH-USDT-1m.jsonl"),
    )

    with pytest.raises(ResearchManifestValidationError) as raised:
        build_manifest(raw_manifest=raw, artifacts=[unknown])

    assert raised.value.field == "files"


@pytest.mark.parametrize("field", ["raw_name", "research_name", "failure_name"])
def test_manifest_rejects_duplicate_artifact_names(field: str) -> None:
    raw, artifacts = make_two_file_inputs()
    first, second = artifacts
    object.__setattr__(second, field, getattr(first, field))

    with pytest.raises(ResearchManifestValidationError, match="unique"):
        build_manifest(raw_manifest=raw, artifacts=[first, second])


def test_manifest_rejects_duplicate_symbol_interval_pair() -> None:
    raw, artifacts = make_two_file_inputs()
    first, second = artifacts
    object.__setattr__(second, "symbol", first.symbol)

    with pytest.raises(ResearchManifestValidationError, match="pairs must be unique"):
        build_manifest(raw_manifest=raw, artifacts=[first, second])


def test_manifest_rejects_unsorted_files() -> None:
    raw, artifacts = make_two_file_inputs()
    manifest = build_manifest(raw_manifest=raw, artifacts=list(artifacts))

    with pytest.raises(ResearchManifestValidationError, match="sorted by raw_name"):
        replace(manifest, files=tuple(reversed(manifest.files)))


@pytest.mark.parametrize(
    "expected",
    [
        ("ETH-USDT-1m.jsonl", "BTC-USDT-1m.jsonl"),
        ("BTC-USDT-1m.jsonl", "BTC-USDT-1m.jsonl"),
    ],
)
def test_manifest_rejects_unsorted_or_duplicate_expected_files(
    expected: tuple[str, ...],
) -> None:
    raw, artifacts = make_two_file_inputs()
    manifest = build_manifest(raw_manifest=raw, artifacts=list(artifacts))

    with pytest.raises(ResearchManifestValidationError) as raised:
        replace(manifest, expected_raw_files=expected)

    assert raised.value.field == "expected_raw_files"


def test_manifest_aggregate_counters_are_exact() -> None:
    partial_report = make_report(
        file="ETH-USDT-1m.jsonl",
        lines_seen=2,
        records_quarantined=1,
        failure_sha256=FAILURE_SHA256,
        failure_bytes=32,
        status="partial",
    )
    raw, artifacts = make_two_file_inputs(second_report=partial_report)
    raw = replace(raw, record_count=3, files=(raw.files[0], replace(raw.files[1], records=2)))
    manifest = build_manifest(raw_manifest=raw, artifacts=list(artifacts))

    assert manifest.lines_seen == 3
    assert manifest.records_written == 2
    assert manifest.records_quarantined == 1
    assert manifest.research_bytes == 256
    assert manifest.failure_bytes == 32


def test_manifest_aggregate_conversion_status_is_exact() -> None:
    report = make_report(
        lines_seen=2,
        records_quarantined=1,
        failure_sha256=FAILURE_SHA256,
        failure_bytes=32,
        status="partial",
    )
    raw = make_raw_manifest(files=(make_output_file(records=2),), record_count=2)
    manifest = build_manifest(raw_manifest=raw, artifacts=[make_artifact(report=report)])

    assert manifest.conversion_status == "partial"
    with pytest.raises(ResearchManifestValidationError):
        replace(manifest, conversion_status="success")


def test_completion_status_is_independent_from_conversion_status() -> None:
    report = make_report(
        lines_seen=2,
        records_quarantined=1,
        failure_sha256=FAILURE_SHA256,
        failure_bytes=32,
        status="partial",
    )
    raw = make_raw_manifest(files=(make_output_file(records=2),), record_count=2)
    manifest = build_manifest(raw_manifest=raw, artifacts=[make_artifact(report=report)])

    assert manifest.completion_status == "complete"
    assert manifest.conversion_status == "partial"


def test_coverage_uses_minimum_and_maximum_accepted_artifact_bounds() -> None:
    btc = make_artifact(
        report=make_report(coverage_start_ms=OPEN_MS + 60_000, coverage_end_ms=CLOSE_MS + 60_000)
    )
    eth = make_artifact(
        symbol="ETH/USDT",
        report=make_report(
            file="ETH-USDT-1m.jsonl",
            coverage_start_ms=OPEN_MS - 60_000,
            coverage_end_ms=CLOSE_MS + 120_000,
        ),
    )
    raw, _ = make_two_file_inputs()
    raw = replace(raw, requested_start=START - timedelta(minutes=1))
    manifest = build_manifest(raw_manifest=raw, artifacts=[btc, eth])

    assert manifest.coverage_start == datetime(2023, 12, 31, 23, 59, tzinfo=UTC)
    assert manifest.coverage_end == datetime(2024, 1, 1, 0, 3, tzinfo=UTC)


def test_manifest_rejects_coverage_before_requested_start() -> None:
    manifest = build_manifest()
    assert manifest.coverage_start is not None

    with pytest.raises(ResearchManifestValidationError) as raised:
        replace(
            manifest,
            requested_start=manifest.coverage_start + timedelta(milliseconds=1),
        )

    assert raised.value.field == "coverage_start"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_manifest_rejects_coverage_after_requested_end() -> None:
    manifest = build_manifest()
    assert manifest.coverage_end is not None

    with pytest.raises(ResearchManifestValidationError) as raised:
        replace(
            manifest,
            requested_end=manifest.coverage_end - timedelta(milliseconds=1),
        )

    assert raised.value.field == "coverage_end"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_manifest_accepts_exact_requested_coverage_boundaries() -> None:
    raw = make_raw_manifest(
        requested_start=START,
        requested_end=START + timedelta(minutes=1),
    )
    bounded = build_manifest(raw_manifest=raw)

    assert bounded.coverage_start == bounded.requested_start
    assert bounded.coverage_end == bounded.requested_end


def test_quarantined_only_artifact_does_not_expand_coverage() -> None:
    quarantined_report = make_report(
        file="ETH-USDT-1m.jsonl",
        lines_seen=1,
        records_written=0,
        records_quarantined=1,
        coverage_start_ms=None,
        coverage_end_ms=None,
        research_sha256=EMPTY_SHA256,
        failure_sha256=FAILURE_SHA256,
        research_bytes=0,
        failure_bytes=32,
        status="partial",
    )
    raw, artifacts = make_two_file_inputs(second_report=quarantined_report)
    manifest = build_manifest(raw_manifest=raw, artifacts=list(artifacts))

    assert manifest.coverage_start == START
    assert manifest.coverage_end == START + timedelta(minutes=1)


def test_no_accepted_records_require_absent_coverage() -> None:
    report = make_report(
        lines_seen=1,
        records_written=0,
        records_quarantined=1,
        coverage_start_ms=None,
        coverage_end_ms=None,
        research_sha256=EMPTY_SHA256,
        failure_sha256=FAILURE_SHA256,
        research_bytes=0,
        failure_bytes=32,
        status="partial",
    )
    manifest = build_manifest(artifacts=[make_artifact(report=report)])

    assert manifest.coverage_start is None
    assert manifest.coverage_end is None
    with pytest.raises(ResearchManifestValidationError):
        replace(manifest, coverage_start=START, coverage_end=START + timedelta(minutes=1))


def test_aware_offsets_normalize_to_utc() -> None:
    manifest = build_manifest()
    plus_seven = timezone(timedelta(hours=7))
    normalized = replace(
        manifest,
        requested_start=manifest.requested_start.astimezone(plus_seven),
        requested_end=manifest.requested_end.astimezone(plus_seven),
        coverage_start=manifest.coverage_start.astimezone(plus_seven),  # type: ignore[union-attr]
        coverage_end=manifest.coverage_end.astimezone(plus_seven),  # type: ignore[union-attr]
    )

    assert normalized.requested_start.tzinfo is UTC
    assert normalized.coverage_start is not None
    assert normalized.coverage_start.tzinfo is UTC


@pytest.mark.parametrize("field", ["requested_start", "requested_end", "coverage_start"])
def test_manifest_rejects_naive_timestamps(field: str) -> None:
    manifest = build_manifest()
    with pytest.raises(ResearchManifestValidationError) as raised:
        replace(manifest, **{field: datetime(2024, 1, 1)})

    assert raised.value.field == field


def test_dataset_id_is_preserved_from_raw_manifest() -> None:
    raw = make_raw_manifest(dataset_id="requested-range-identity-not-coverage-identity")
    manifest = build_manifest(raw_manifest=raw)

    assert manifest.dataset_id == raw.dataset_id


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("raw", "dataset_version", "1.0.0"),
        ("raw", "downloader_version", "1.0.0"),
        ("raw", "schema_version", 2),
        ("raw", "source", "other"),
        ("manifest", "dataset_version", "1.0.0"),
        ("manifest", "converter_version", "2.0.0"),
        ("manifest", "schema_version", True),
        ("manifest", "source", "other"),
        ("manifest", "symbols", ["BTC/USDT"]),
        ("manifest", "intervals", ["1m"]),
    ],
)
def test_current_version_source_schema_and_tuple_contracts_are_strict(
    target: str,
    field: str,
    value: object,
) -> None:
    if target == "raw":
        raw = replace(make_raw_manifest(), **{field: value})
        with pytest.raises(ResearchManifestValidationError):
            build_manifest(raw_manifest=raw)
        return

    manifest = build_manifest()
    with pytest.raises(ResearchManifestValidationError):
        replace(manifest, **{field: value})


@pytest.mark.parametrize("kind", ["incomplete", "failure"])
def test_builder_rejects_incomplete_or_failing_raw_manifest(kind: str) -> None:
    raw = make_raw_manifest()
    if kind == "incomplete":
        raw = replace(raw, completion_status="incomplete")
    else:
        failure = DownloadFailure(
            symbol="BTC/USDT",
            interval="1m",
            range_start=START,
            range_end=END,
            endpoint="/api/v3/klines",
            error_type="request_error",
            message="fixed",
            attempts=1,
        )
        raw = replace(raw, failure=failure)

    with pytest.raises(ResearchManifestValidationError) as raised:
        build_manifest(raw_manifest=raw)

    assert raised.value.field == "raw_manifest"


def test_builder_rejects_raw_record_count_mismatch() -> None:
    raw = make_raw_manifest(record_count=2)

    with pytest.raises(ResearchManifestValidationError, match="file total"):
        build_manifest(raw_manifest=raw)


def test_builder_rejects_unrelated_raw_file_name() -> None:
    raw = make_raw_manifest(files=(make_output_file("unrelated.jsonl"),))

    with pytest.raises(ResearchManifestValidationError) as raised:
        build_manifest(raw_manifest=raw)

    assert raised.value.field == "raw_manifest"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_builder_accepts_valid_subset_of_requested_raw_files() -> None:
    raw = make_raw_manifest(
        symbols=("BTC/USDT", "ETH/USDT"),
        files=(make_output_file("BTC-USDT-1m.jsonl"),),
        record_count=1,
    )

    manifest = build_manifest(raw_manifest=raw, artifacts=[make_artifact()])

    assert manifest.symbols == ("BTC/USDT", "ETH/USDT")
    assert manifest.expected_raw_files == ("BTC-USDT-1m.jsonl",)
    assert manifest.completion_status == "complete"


@pytest.mark.parametrize("records", [True, -1])
def test_builder_rejects_bool_or_negative_output_file_records(records: object) -> None:
    raw = make_raw_manifest(files=(make_output_file(records=records),), record_count=records)

    with pytest.raises(ResearchManifestValidationError) as raised:
        build_manifest(raw_manifest=raw)

    assert raised.value.field == "raw_manifest"


@pytest.mark.parametrize(
    ("level", "change"),
    [("top", "missing"), ("top", "unknown"), ("nested", "missing"), ("nested", "unknown")],
)
def test_from_record_requires_exact_keys_at_both_levels(level: str, change: str) -> None:
    record = build_manifest().to_record()
    target = record if level == "top" else record["files"][0]  # type: ignore[index]
    assert isinstance(target, dict)
    if change == "missing":
        target.pop(next(iter(target)))
    else:
        target["synthetic_unknown_field"] = "value"

    with pytest.raises(ResearchManifestValidationError):
        ResearchDatasetManifest.from_record(record)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("symbols", ("BTC/USDT",)),
        ("expected_raw_files", ("BTC-USDT-1m.jsonl",)),
        ("files", ()),
        ("records_written", "1"),
        ("schema_version", True),
    ],
)
def test_from_record_does_not_coerce_wrong_runtime_types(field: str, value: object) -> None:
    record = build_manifest().to_record()
    record[field] = value

    with pytest.raises(ResearchManifestValidationError):
        ResearchDatasetManifest.from_record(record)


def test_json_is_compact_sorted_and_byte_deterministic_across_round_trip() -> None:
    manifest = build_manifest()
    record = manifest.to_record()
    text = manifest.to_json()
    restored = ResearchDatasetManifest.from_json(text)

    assert list(record) == sorted(record)
    assert all(list(item) == sorted(item) for item in record["files"])  # type: ignore[union-attr]
    assert text == json.dumps(record, separators=(",", ":"), sort_keys=True)
    assert not text.endswith("\n")
    assert restored == manifest
    assert restored.to_json() == text


def test_serialized_record_contains_no_float_anywhere() -> None:
    assert_no_float(build_manifest().to_record())


def test_from_json_rejects_duplicate_top_level_key() -> None:
    canonical = build_manifest().to_json()
    duplicated = f'{{"dataset_id":"duplicate",{canonical[1:]}'

    with pytest.raises(ResearchManifestValidationError) as raised:
        ResearchDatasetManifest.from_json(duplicated)

    assert raised.value.field == "text"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_from_json_rejects_duplicate_nested_artifact_key() -> None:
    canonical = build_manifest().to_json()
    duplicated = canonical.replace(
        '"files":[{',
        '"files":[{"raw_name":"duplicate.jsonl",',
        1,
    )

    with pytest.raises(ResearchManifestValidationError) as raised:
        ResearchDatasetManifest.from_json(duplicated)

    assert raised.value.field == "text"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None


def test_from_json_rejects_oversized_integer_with_sanitized_error() -> None:
    oversized_integer = "9" * 5_000
    payload = f'{{"records_written":{oversized_integer}}}'

    with pytest.raises(ResearchManifestValidationError) as raised:
        ResearchDatasetManifest.from_json(payload)

    error = raised.value
    assert error.field == "text"
    assert oversized_integer not in str(error)
    assert oversized_integer not in repr(error)
    assert oversized_integer not in repr(vars(error))
    assert error.__cause__ is None
    assert error.__context__ is None


def test_malformed_sensitive_json_produces_detached_sanitized_error() -> None:
    marker = "SYNTHETIC_SECRET_MARKER_7391"
    with pytest.raises(ResearchManifestValidationError) as raised:
        ResearchDatasetManifest.from_json(f'{{"payload":"{marker}"')

    error = raised.value
    assert marker not in str(error)
    assert marker not in repr(error)
    assert marker not in repr(vars(error))
    assert error.__cause__ is None
    assert error.__context__ is None
    with pytest.raises(ResearchManifestValidationError, match="JSON object"):
        ResearchDatasetManifest.from_json("[]")

    duplicate_secret = f'{{"{marker}":1,"{marker}":2}}'
    with pytest.raises(ResearchManifestValidationError) as duplicate_raised:
        ResearchDatasetManifest.from_json(duplicate_secret)
    duplicate_error = duplicate_raised.value
    assert marker not in str(duplicate_error)
    assert marker not in repr(duplicate_error)
    assert marker not in repr(vars(duplicate_error))
    assert duplicate_error.__cause__ is None
    assert duplicate_error.__context__ is None


def test_validation_remains_active_under_python_optimized_mode() -> None:
    script = (
        "from packages.market_data.datasets.conversion_manifest import "
        "ResearchDatasetManifest; ResearchDatasetManifest.from_json('{}')"
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode != 0
    assert "ResearchManifestValidationError" in result.stderr


def test_builder_does_not_mutate_dependency_objects_or_input_sequence() -> None:
    raw = make_raw_manifest()
    report = make_report()
    artifact = make_artifact(report=report)
    artifacts = [artifact]
    raw_before = raw.to_json()
    report_before = asdict(report)
    artifact_before = asdict(artifact)

    build_manifest(raw_manifest=raw, artifacts=artifacts)

    assert raw.to_json() == raw_before
    assert asdict(report) == report_before
    assert asdict(artifact) == artifact_before
    assert artifacts == [artifact]


def test_eight_lazy_exports_import_successfully() -> None:
    import packages.market_data.datasets as dataset_exports

    assert dataset_exports.PublicationStatus is not None
    assert dataset_exports.RESEARCH_CONVERTER_VERSION == RESEARCH_CONVERTER_VERSION
    assert dataset_exports.RESEARCH_DATASET_VERSION == RESEARCH_DATASET_VERSION
    assert dataset_exports.RESEARCH_MANIFEST_FILE == RESEARCH_MANIFEST_FILE
    assert dataset_exports.ResearchDatasetManifest is ResearchDatasetManifest
    assert dataset_exports.ResearchFileArtifact is ResearchFileArtifact
    assert dataset_exports.ResearchManifestValidationError is ResearchManifestValidationError
    assert dataset_exports.build_research_manifest is build_research_manifest


def test_production_module_has_no_filesystem_network_or_dynamic_execution() -> None:
    tree = ast.parse(inspect.getsource(manifest_module))
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
        "eval",
        "exec",
        "mkdir",
        "open",
        "read_bytes",
        "read_text",
        "remove",
        "rename",
        "unlink",
        "write_bytes",
        "write_text",
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


# =============================================================================
# ResearchFilePlan tests — DATA-005 C3B-1
# =============================================================================


class TestResearchFilePlanValidConstruction:
    """Valid direct construction and classmethod."""

    def test_direct_construction_valid(self) -> None:
        plan = ResearchFilePlan(
            raw_name="BTC-USDT-1m.jsonl",
            research_name="BTC-USDT-1m.jsonl",
            failure_name="BTC-USDT-1m.failures.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        assert plan.raw_name == "BTC-USDT-1m.jsonl"
        assert plan.research_name == "BTC-USDT-1m.jsonl"
        assert plan.failure_name == "BTC-USDT-1m.failures.jsonl"
        assert plan.symbol == "BTC/USDT"
        assert plan.interval == "1m"

    def test_from_raw_identity_valid(self) -> None:
        plan = ResearchFilePlan.from_raw_identity(
            raw_name="ETH-USDT-1h.jsonl",
            symbol="ETH/USDT",
            interval="1h",
        )
        assert plan.raw_name == "ETH-USDT-1h.jsonl"
        assert plan.research_name == "ETH-USDT-1h.jsonl"
        assert plan.failure_name == "ETH-USDT-1h.failures.jsonl"
        assert plan.symbol == "ETH/USDT"
        assert plan.interval == "1h"

    @pytest.mark.parametrize(
        ("symbol", "interval"),
        [
            ("BTC/USDT", "1m"),
            ("BTC/USDT", "1h"),
            ("BTC/USDT", "1d"),
            ("ETH/USDT", "1m"),
            ("SOL/USDT", "5m"),
            ("BTC/USDT", "1M"),
        ],
    )
    def test_canonical_names_match_expected_formula(self, symbol: str, interval: str) -> None:
        prefix = f"{symbol.replace('/', '-')}-{interval}"
        expected_raw = f"{prefix}.jsonl"
        expected_failure = f"{prefix}.failures.jsonl"
        plan = ResearchFilePlan.from_raw_identity(
            raw_name=expected_raw, symbol=symbol, interval=interval
        )
        assert plan.raw_name == expected_raw
        assert plan.research_name == expected_raw
        assert plan.failure_name == expected_failure


class TestResearchFilePlan1mVs1MDistinction:
    """1m and 1M must produce distinct names."""

    def test_1m_and_1M_names_are_distinct(self) -> None:
        plan_1m = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1m.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        plan_1M = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1M.jsonl",
            symbol="BTC/USDT",
            interval="1M",
        )
        assert plan_1m.raw_name != plan_1M.raw_name
        assert plan_1m.failure_name != plan_1M.failure_name
        assert plan_1m.raw_name == "BTC-USDT-1m.jsonl"
        assert plan_1M.raw_name == "BTC-USDT-1M.jsonl"


class TestResearchFilePlanInvalidSymbol:
    """Invalid or malformed symbol is rejected."""

    @pytest.mark.parametrize(
        "symbol",
        [
            "",  # empty
            "btc/usdt",  # lowercase
            "BTCUSDT",  # no slash
            "BTC/",  # missing quote
            "/USDT",  # missing base
            "BTC-USDT",  # dash instead of slash
        ],
    )
    def test_invalid_symbol_rejected(self, symbol: str) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name="BTC-USDT-1m.jsonl",
                symbol=symbol,
                interval="1m",
            )
        assert exc_info.value.field == "symbol"

    def test_non_string_symbol_rejected(self) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name="BTC-USDT-1m.jsonl",
                symbol=123,  # type: ignore[arg-type]
                interval="1m",
            )
        assert exc_info.value.field == "symbol"


class TestResearchFilePlanInvalidInterval:
    """Unsupported or wrong-case interval is rejected."""

    @pytest.mark.parametrize(
        "interval",
        [
            "7m",  # unsupported
        ],
    )
    def test_unsupported_interval_rejected(self, interval: str) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name=f"BTC-USDT-{interval}.jsonl",
                symbol="BTC/USDT",
                interval=interval,
            )
        assert exc_info.value.field == "interval"

    def test_wrong_case_interval_rejected(self) -> None:
        # "1m" is minute, "1M" is month. Pass "1m" in raw_name but "1M"
        # as interval — the naming formula would produce "1M" so mismatch.
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name="BTC-USDT-1m.jsonl",
                symbol="BTC/USDT",
                interval="1M",
            )
        assert exc_info.value.field == "raw_name"


class TestResearchFilePlanNameMismatch:
    """raw_name mismatch with symbol/interval is rejected."""

    def test_raw_name_mismatch_rejected(self) -> None:
        # raw_name says 1m but interval says 1h
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name="BTC-USDT-1m.jsonl",
                symbol="BTC/USDT",
                interval="1h",
            )
        assert exc_info.value.field == "raw_name"

    def test_raw_name_symbol_mismatch_rejected(self) -> None:
        # raw_name says ETH but symbol says BTC
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name="ETH-USDT-1m.jsonl",
                symbol="BTC/USDT",
                interval="1m",
            )
        assert exc_info.value.field == "raw_name"


class TestResearchFilePlanDirectNameMismatch:
    """Direct construction validates all three names."""

    @pytest.mark.parametrize(
        ("field", "bad_name"),
        [
            # raw_name mismatch (wrong symbol in name): formula cross-check fires
            # first and raises raw_name error.
            ("raw_name", "ETH-USDT-1m.jsonl"),
            # research_name mismatch: safe_basename passes, raw==research fails.
            ("research_name", "BTC-USDT-5m.jsonl"),
            # failure_name mismatch (wrong suffix): safe_basename passes,
            # raw==research passes, then failure_name fails formula.
            ("failure_name", "BTC-USDT-1m.failed.jsonl"),
        ],
    )
    def test_direct_construction_rejects_name_mismatch(self, field: str, bad_name: str) -> None:
        values = {
            "raw_name": "BTC-USDT-1m.jsonl",
            "research_name": "BTC-USDT-1m.jsonl",
            "failure_name": "BTC-USDT-1m.failures.jsonl",
            "symbol": "BTC/USDT",
            "interval": "1m",
        }
        values[field] = bad_name
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan(**values)
        # All three cases raise errors on the field they corrupt.
        assert exc_info.value.field == field

    def test_research_name_must_equal_raw_name(self) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan(
                raw_name="BTC-USDT-1m.jsonl",
                research_name="BTC-USDT-5m.jsonl",  # different
                failure_name="BTC-USDT-1m.failures.jsonl",
                symbol="BTC/USDT",
                interval="1m",
            )
        assert exc_info.value.field == "research_name"


class TestResearchFilePlanUnsafeBasename:
    """Unsafe basenames are rejected."""

    @pytest.mark.parametrize(
        ("field", "unsafe_name"),
        [
            ("raw_name", "dir/file.jsonl"),
            ("raw_name", "dir\\file.jsonl"),
            ("raw_name", "/absolute.jsonl"),
            ("raw_name", ""),
            ("raw_name", "."),
            ("raw_name", ".."),
            ("raw_name", "bad\0x"),
            ("raw_name", "bad\rx"),
            ("raw_name", "bad\nx"),
            ("research_name", "dir/file.jsonl"),
            ("failure_name", "dir/failures.jsonl"),
        ],
    )
    def test_unsafe_basename_rejected(self, field: str, unsafe_name: str) -> None:
        values = {
            "raw_name": "BTC-USDT-1m.jsonl",
            "research_name": "BTC-USDT-1m.jsonl",
            "failure_name": "BTC-USDT-1m.failures.jsonl",
            "symbol": "BTC/USDT",
            "interval": "1m",
        }
        values[field] = unsafe_name
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan(**values)
        assert (
            "safe basename" in str(exc_info.value).lower()
            or "basename" in str(exc_info.value).lower()
        )


class TestResearchFilePlanStrictTypes:
    """Exact runtime types are enforced."""

    @pytest.mark.parametrize(
        ("field", "bad_value"),
        [
            ("raw_name", None),
            ("research_name", None),
            ("failure_name", None),
            ("symbol", None),
            ("interval", None),
            ("symbol", 123),
            ("interval", True),
            ("raw_name", object()),
            ("research_name", 42),
            ("failure_name", []),
        ],
    )
    def test_non_string_rejected(self, field: str, bad_value: object) -> None:
        values = {
            "raw_name": "BTC-USDT-1m.jsonl",
            "research_name": "BTC-USDT-1m.jsonl",
            "failure_name": "BTC-USDT-1m.failures.jsonl",
            "symbol": "BTC/USDT",
            "interval": "1m",
        }
        values[field] = bad_value
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan(**values)
        assert exc_info.value.field == field


class TestResearchFilePlanFrozen:
    """Frozen dataclass enforcement."""

    def test_frozen_prevents_attribute_mutation(self) -> None:
        plan = ResearchFilePlan.from_raw_identity(
            raw_name="BTC-USDT-1m.jsonl",
            symbol="BTC/USDT",
            interval="1m",
        )
        with pytest.raises(FrozenInstanceError):
            plan.symbol = "ETH/USDT"  # type: ignore[misc]


class TestResearchFilePlanPythonOptimized:
    """Validation works under python -O (no assert stripping)."""

    def test_validation_uses_raise_not_assert(self) -> None:
        source = inspect.getsource(ResearchFilePlan)
        tree = ast.parse(source)
        has_assert = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Assert):
                has_assert = True
        assert not has_assert, "__post_init__ must not use assert"

    def test_invalid_construction_rejected_under_optimized_mode(self) -> None:
        script = (
            "from packages.market_data.datasets.conversion_manifest import "
            "ResearchFilePlan, ResearchManifestValidationError\n"
            "try:\n"
            "    ResearchFilePlan.from_raw_identity("
            "raw_name='BTC-USDT-1m.jsonl', symbol='btc/usdt', interval='1m')\n"
            "    raise SystemExit(1)\n"
            "except ResearchManifestValidationError:\n"
            "    raise SystemExit(0)\n"
        )
        result = subprocess.run(
            [sys.executable, "-O", "-c", script],
            capture_output=True,
            check=False,
            text=True,
        )
        assert result.returncode == 0, result.stderr


class TestResearchFilePlanSanitizedErrors:
    """Errors are sanitized: no raw values or paths in messages."""

    def test_error_contains_no_raw_value(self) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name="BTC-USDT-1m.jsonl",
                symbol="btc/usdt",  # invalid lowercase
                interval="1m",
            )
        error = exc_info.value
        assert error.field == "symbol"
        assert "btc" not in str(error).lower()
        assert "usdt" not in str(error).lower()
        assert error.__cause__ is None
        assert error.__context__ is None

    def test_error_fields_are_detached(self) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            ResearchFilePlan.from_raw_identity(
                raw_name="BTC-USDT-1m.jsonl",
                symbol="BTC/USDT",
                interval="7m",  # unsupported
            )
        error = exc_info.value
        assert isinstance(error.field, str)
        assert isinstance(error.reason, str)
        assert "7m" not in str(error)


class TestResearchFileArtifactExistingBehaviorUnchanged:
    """Existing ResearchFileArtifact behavior is preserved."""

    def test_artifact_naming_uses_same_authority(self) -> None:
        # Given a canonical symbol/interval, both ResearchFileArtifact
        # and ResearchFilePlan produce identical naming.
        symbol, interval = "BTC/USDT", "1m"
        plan = ResearchFilePlan.from_raw_identity(
            raw_name=f"{symbol.replace('/', '-')}-{interval}.jsonl",
            symbol=symbol,
            interval=interval,
        )
        artifact = make_artifact(symbol=symbol, interval=interval)
        assert plan.raw_name == artifact.raw_name
        assert plan.research_name == artifact.research_name
        assert plan.failure_name == artifact.failure_name

    def test_artifact_direct_construction_and_plan_produce_same_names(self) -> None:
        # Both paths (direct construction and from_stream_report) must
        # still work and produce the same canonical names.
        artifact1 = make_artifact()
        artifact2 = ResearchFileArtifact.from_stream_report(
            raw_name=artifact1.raw_name,
            research_name=artifact1.research_name,
            failure_name=artifact1.failure_name,
            symbol=artifact1.symbol,
            interval=artifact1.interval,
            raw_sha256=artifact1.raw_sha256,
            raw_bytes=artifact1.raw_bytes,
            report=make_report(file=artifact1.raw_name),
        )
        assert artifact1.raw_name == artifact2.raw_name
        assert artifact1.research_name == artifact2.research_name
        assert artifact1.failure_name == artifact2.failure_name

    def test_artifact_unsafe_basename_still_rejected(self) -> None:
        # Ensure existing validation still works.
        with pytest.raises(ResearchManifestValidationError, match="safe basename"):
            make_artifact(raw_name="dir/file.jsonl")

    def test_artifact_invalid_interval_still_rejected(self) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            make_artifact(interval="7m")
        assert exc_info.value.field == "interval"

    def test_artifact_invalid_symbol_still_rejected(self) -> None:
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            make_artifact(symbol="btc/usdt")
        assert exc_info.value.field == "symbol"

    def test_artifact_name_mismatch_still_rejected(self) -> None:
        # make_artifact uses from_stream_report, which checks report.file == raw_name.
        # A mismatched raw_name triggers the report.field error, not raw_name.
        with pytest.raises(ResearchManifestValidationError) as exc_info:
            make_artifact(raw_name="BTC-USDT-5m.jsonl")
        assert exc_info.value.field == "report"


# =============================================================================
# _validate_raw_manifest — strict DownloadManifest regression tests
# =============================================================================


class _StrSubclass(str):
    pass


class _IntSubclass(int):
    pass


class _DatetimeSubclass(datetime):
    pass


def _make_output_file(
    name: str = "BTC-USDT-1m.jsonl",
    *,
    records: object = 1,
    range_start: object = START,
    range_end: object = START + timedelta(minutes=1),
) -> OutputFileInfo:
    return OutputFileInfo(
        name=name,
        records=records,
        range_start=range_start,
        range_end=range_end,
    )


def _make_raw_manifest(**overrides: object) -> DownloadManifest:
    files = overrides.pop("files", (_make_output_file(),))
    record_count = overrides.pop(
        "record_count",
        sum(file_info.records for file_info in files),
    )
    # actual_start/actual_end must match file range boundaries for non-empty datasets.
    if files:
        first_range_start = files[0].range_start
        last_range_end = files[-1].range_end
    else:
        first_range_start = None
        last_range_end = None
    values: dict[str, object] = {
        "dataset_id": "raw-request-identity",
        "dataset_version": DATASET_DOWNLOAD_VERSION,
        "downloader_version": DOWNLOADER_VERSION,
        "schema_version": 1,
        "source": RESEARCH_SOURCE,
        "exchange": "binance",
        "market_type": "spot",
        "symbols": ("BTC/USDT",),
        "intervals": ("1m",),
        "requested_start": START,
        "requested_end": END,
        "actual_start": first_range_start,
        "actual_end": last_range_end,
        "record_count": record_count,
        "files": files,
        "completion_status": "complete",
        "failure": None,
        "page_limit": 1000,
        "resume": False,
        "server_time": END,
    }
    values.update(overrides)
    return DownloadManifest(**values)  # type: ignore[arg-type]


# =============================================================================
# actual_start / actual_end / server_time type/wrong-type rejection
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [object(), "2024-01-01T00:00:00Z", 123, True, False, [], {}],
)
def test_validator_rejects_wrong_type_actual_start(value: object) -> None:
    raw = _make_raw_manifest(actual_start=value)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.parametrize(
    "value",
    [object(), "2024-01-01T00:00:00Z", 123, True, False, [], {}],
)
def test_validator_rejects_wrong_type_actual_end(value: object) -> None:
    raw = _make_raw_manifest(actual_end=value)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


@pytest.mark.parametrize(
    "value",
    [object(), "2024-01-01T00:00:00Z", 123, True, False, [], {}],
)
def test_validator_rejects_wrong_type_server_time(value: object) -> None:
    raw = _make_raw_manifest(server_time=value)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_validator_rejects_naive_datetime_actual_start() -> None:
    raw = _make_raw_manifest(actual_start=datetime(2024, 1, 1))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_validator_rejects_datetime_subclass_actual_start() -> None:
    raw = _make_raw_manifest(
        actual_start=_DatetimeSubclass(2024, 1, 1, tzinfo=UTC),
    )

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


# =============================================================================
# page_limit strict validation
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [True, False, 0, -1, 1001, 1.0, "1000", None],
)
def test_validator_rejects_page_limit_non_int(value: object) -> None:
    raw = _make_raw_manifest(page_limit=value)  # type: ignore[arg-type]

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


def test_validator_rejects_page_limit_int_subclass() -> None:
    raw = _make_raw_manifest(page_limit=_IntSubclass(1000))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


# =============================================================================
# resume strict validation
# =============================================================================


@pytest.mark.parametrize("value", [0, 1, "false", None, object()])
def test_validator_rejects_resume_non_bool(value: object) -> None:
    raw = _make_raw_manifest(resume=value)  # type: ignore[arg-type]

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


def test_validator_rejects_resume_bool_subclass() -> None:
    raw = _make_raw_manifest(resume=0)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


# =============================================================================
# string field subclasses
# =============================================================================


@pytest.mark.parametrize(
    "field",
    ["dataset_id", "dataset_version", "downloader_version", "source", "exchange", "market_type"],
)
def test_validator_rejects_str_subclass_on_string_fields(field: str) -> None:
    raw = _make_raw_manifest(**{field: _StrSubclass("binance")})

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


# =============================================================================
# symbol / interval subclasses
# =============================================================================


def test_validator_rejects_symbol_str_subclass() -> None:
    raw = _make_raw_manifest(symbols=(_StrSubclass("BTC/USDT"),))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "symbols"


def test_validator_rejects_interval_str_subclass() -> None:
    raw = _make_raw_manifest(intervals=(_StrSubclass("1m"),))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "intervals"


# =============================================================================
# int field subclasses
# =============================================================================


def test_validator_rejects_schema_version_int_subclass() -> None:
    raw = _make_raw_manifest(schema_version=_IntSubclass(1))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


def test_validator_rejects_record_count_int_subclass() -> None:
    raw = _make_raw_manifest(record_count=_IntSubclass(1))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


# =============================================================================
# OutputFileInfo subclass and nested field subclasses
# =============================================================================


class _OutputFileInfoSubclass(OutputFileInfo):
    pass


def test_validator_rejects_output_file_info_subclass() -> None:
    raw_files = (
        _OutputFileInfoSubclass(
            name="BTC-USDT-1m.jsonl",
            records=1,
            range_start=START,
            range_end=START + timedelta(minutes=1),
        ),
    )
    raw = _make_raw_manifest(files=raw_files)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


def test_validator_rejects_output_file_records_int_subclass() -> None:
    raw_files = (replace(_make_output_file(), records=_IntSubclass(1)),)
    raw = _make_raw_manifest(files=raw_files)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


def test_validator_rejects_output_file_name_str_subclass() -> None:
    raw_files = (replace(_make_output_file(), name=_StrSubclass("BTC-USDT-1m.jsonl")),)
    raw = _make_raw_manifest(files=raw_files)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    assert exc_info.value.field == "raw_manifest"


# =============================================================================
# valid canonical cases remain valid
# =============================================================================


def test_validator_accepts_empty_dataset_with_none_actual_start_end() -> None:
    raw = _make_raw_manifest(files=(), record_count=0)
    assert raw.files == ()

    manifest = build_research_manifest(
        raw,
        raw_manifest_sha256="b" * 64,
        files=[],
        research_checksum=EMPTY_SHA256,
        failure_checksum=EMPTY_SHA256,
        max_line_bytes=1_048_576,
        completion_status="complete",
    )

    assert manifest.files == ()


def test_validator_accepts_non_empty_manifest_with_aware_datetimes() -> None:
    raw = _make_raw_manifest(
        actual_start=START,
        actual_end=START + timedelta(minutes=1),
        server_time=END,
    )

    manifest = build_research_manifest(
        raw,
        raw_manifest_sha256="b" * 64,
        files=[make_artifact()],
        research_checksum=AGGREGATE_RESEARCH_SHA256,
        failure_checksum=EMPTY_SHA256,
        max_line_bytes=1_048_576,
        completion_status="complete",
    )

    assert len(manifest.files) == 1


def test_validator_accepts_non_utc_offset_timezone() -> None:
    plus_seven = timezone(timedelta(hours=7))
    # Use UTC-equivalent boundaries so cross-field invariants pass.
    utc_equivalent_start = datetime(2024, 1, 1, 7, 0, 0, tzinfo=plus_seven)  # = START in UTC
    utc_equivalent_end = datetime(2024, 1, 1, 7, 1, 0, 0, tzinfo=plus_seven)  # = START+1min in UTC
    raw = _make_raw_manifest(
        actual_start=utc_equivalent_start,
        actual_end=utc_equivalent_end,
        server_time=datetime(2024, 1, 2, tzinfo=plus_seven),
        files=(
            _make_output_file(
                range_start=utc_equivalent_start,
                range_end=utc_equivalent_end,
            ),
        ),
    )

    # Artifact coverage must match the UTC epoch milliseconds of the UTC-equivalent range.
    utc_start_ms = int(utc_equivalent_start.timestamp() * 1000)
    utc_end_ms = int(utc_equivalent_end.timestamp() * 1000)
    artifact_report = make_report(
        coverage_start_ms=utc_start_ms,
        coverage_end_ms=utc_end_ms,
    )
    artifact = make_artifact(report=artifact_report)

    manifest = build_research_manifest(
        raw,
        raw_manifest_sha256="b" * 64,
        files=[artifact],
        research_checksum=AGGREGATE_RESEARCH_SHA256,
        failure_checksum=EMPTY_SHA256,
        max_line_bytes=1_048_576,
        completion_status="complete",
    )

    assert len(manifest.files) == 1
    assert manifest.requested_start.tzinfo is UTC


# =============================================================================
# error sanitization and detachment
# =============================================================================


@pytest.mark.parametrize(
    "value",
    [
        object(),
        _StrSubclass("BTC/USDT"),
        _DatetimeSubclass(2024, 1, 1, tzinfo=UTC),
        _IntSubclass(1),
        True,
        False,
    ],
)
def test_validator_errors_are_sanitized_and_detached(value: object) -> None:
    raw = _make_raw_manifest(
        actual_start=value,
        actual_end=value,
        server_time=value,
    )

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    # No raw values or markers leak into error surface.
    assert "object()" not in rendered
    assert "StrSubclass" not in rendered
    assert "DatetimeSubclass" not in rendered
    assert "IntSubclass" not in rendered


def test_validator_preserves_return_shape() -> None:
    raw = _make_raw_manifest(files=(), record_count=0)

    from packages.market_data.datasets.conversion_manifest import _validate_raw_manifest

    result = _validate_raw_manifest(raw)

    assert isinstance(result, tuple)
    assert len(result) == 3
    assert isinstance(result[0], datetime)
    assert isinstance(result[1], datetime)
    assert isinstance(result[2], tuple)


def test_validator_does_not_mutate_manifest_objects() -> None:
    raw = _make_raw_manifest()
    before = asdict(raw)
    files_before = asdict(raw.files[0])

    try:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )
    except ResearchManifestValidationError:
        pass

    assert asdict(raw) == before
    assert asdict(raw.files[0]) == files_before


def test_validator_works_under_python_optimized_mode() -> None:
    script = (
        "from packages.market_data.datasets.conversion_manifest import "
        "ResearchManifestValidationError, build_research_manifest\n"
        "from packages.market_data.datasets.downloader import (\n"
        "    DATASET_DOWNLOAD_VERSION, DOWNLOADER_VERSION, DownloadManifest\n"
        ")\n"
        "from packages.market_data.datasets.metadata import DATASET_SCHEMA_VERSION\n"
        "from packages.market_data.datasets.research_format import RESEARCH_SOURCE\n"
        "from datetime import UTC, datetime, timedelta\n"
        "start = datetime(2024, 1, 1, tzinfo=UTC)\n"
        "end = start + timedelta(days=1)\n"
        "raw = DownloadManifest(\n"
        "    dataset_id='id', dataset_version=DATASET_DOWNLOAD_VERSION,\n"
        "    downloader_version=DOWNLOADER_VERSION,\n"
        "    schema_version=DATASET_SCHEMA_VERSION,\n"
        "    source=RESEARCH_SOURCE, exchange='binance', market_type='spot',\n"
        "    symbols=('BTC/USDT',), intervals=('1m',),\n"
        "    requested_start=start, requested_end=end,\n"
        "    actual_start=object(), actual_end=object(),\n"
        "    record_count=0, files=(), completion_status='complete',\n"
        "    failure=None, page_limit=True, resume='false', server_time=object(),\n"
        ")\n"
        "try:\n"
        "    build_research_manifest(\n"
        "        raw,\n"
        "        raw_manifest_sha256='b' * 64,\n"
        "        files=[],\n"
        "        research_checksum='" + EMPTY_SHA256 + "',\n"
        "        failure_checksum='" + EMPTY_SHA256 + "',\n"
        "        max_line_bytes=1048576,\n"
        "        completion_status='complete',\n"
        "    )\n"
        "    raise SystemExit(1)\n"
        "except ResearchManifestValidationError as e:\n"
        "    if e.__cause__ is None and e.__context__ is None:\n"
        "        raise SystemExit(0)\n"
        "    raise SystemExit(2)\n"
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr


class TestResearchFilePlanLazyExport:
    """ResearchFilePlan is exported from datasets.__init__."""

    def test_plan_exported(self) -> None:
        from packages.market_data.datasets import ResearchFilePlan

        assert ResearchFilePlan is not None


# =============================================================================
# C4A H1 second-repair mandatory regressions
# Finding 1: required datetime normalization catches OverflowError/TypeError/ValueError
# Finding 2: exact types precede all semantic operations
# Finding 3: required datetimes reject datetime subclasses
# =============================================================================


# ---------- Finding 1: datetime subclasses bypass type() guards ----------
# A datetime with a stateful or malformed tzinfo IS constructible — the tzinfo
# raises only when its methods (utcoffset, astimezone) are called. A stateful tzinfo
# can return timedelta(0) on the first utcoffset() call but raise ValueError on
# astimezone(UTC). The exact-type preflight fires first; the single protected try
# block catches any ValueError/OverflowError/TypeError from utcoffset or astimezone.


# ---------- Finding 2: malicious str subclass overrides comparison ----------


class _MaliciousStr(str):
    """str subclass that raises on equality/inequality."""

    def __eq__(self, other: object) -> bool:
        raise ValueError("malicious eq")

    def __ne__(self, other: object) -> bool:
        raise ValueError("malicious ne")


class _MaliciousStrMembership(str):
    """str subclass that raises on __contains__."""

    def __contains__(self, item: object) -> bool:
        raise ValueError("malicious contains")


def test_malicious_str_subclass_on_scalar_field_raises_detached() -> None:
    raw = _make_raw_manifest(dataset_id=_MaliciousStr("binance"))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "malicious" not in rendered.lower()
    assert "MaliciousStr" not in rendered


def test_malicious_str_subclass_on_symbol_raises_detached() -> None:
    raw = _make_raw_manifest(symbols=(_MaliciousStr("BTC/USDT"),))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None


def test_malicious_str_subclass_on_filename_raises_detached() -> None:
    raw_files = (
        OutputFileInfo(
            name=_MaliciousStr("BTC-USDT-1m.jsonl"),
            records=1,
            range_start=START,
            range_end=START + timedelta(minutes=1),
        ),
    )
    raw = _make_raw_manifest(files=raw_files)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "malicious" not in rendered.lower()


# ---------- Finding 3: datetime subclass on required fields ----------


def test_requested_start_datetime_subclass_rejected() -> None:
    raw = _make_raw_manifest(
        requested_start=_DatetimeSubclass(2024, 1, 1, tzinfo=UTC),
    )

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "DatetimeSubclass" not in rendered


def test_requested_end_datetime_subclass_rejected() -> None:
    raw = _make_raw_manifest(
        requested_end=_DatetimeSubclass(2024, 1, 2, tzinfo=UTC),
    )

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "DatetimeSubclass" not in rendered


# ---------- Stateful tzinfo normalization lifecycle ----------
# datetime.astimezone() observes the source tzinfo after the explicit awareness
# check.  Every counter below belongs to one tzinfo instance so tests cannot pass
# because another field or test consumed a shared observation.


_TZINFO_PRIVATE_MARKER = "C4A_H1_TIMEZONE_PRIVATE_MARKER_DO_NOT_LEAK"


class _RaiseOnSecondObservationTzinfo(tzinfo_base):
    """Return one valid offset, then fail during the same normalization lifecycle."""

    __slots__ = ("observations",)

    def __init__(self) -> None:
        self.observations = 0

    def utcoffset(self, dt: datetime | None) -> timedelta:
        self.observations += 1
        if self.observations == 1:
            return timedelta(0)
        raise ValueError(_TZINFO_PRIVATE_MARKER)

    def tzname(self, dt: datetime | None) -> str:
        return "raise-on-second-observation"

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)


@pytest.mark.parametrize("field", ["requested_start", "actual_start", "server_time"])
def test_second_timezone_observation_failure_is_detached_and_sanitized(
    field: str,
) -> None:
    observed_timezone = _RaiseOnSecondObservationTzinfo()
    raw = _make_raw_manifest(**{field: datetime(2024, 1, 1, tzinfo=observed_timezone)})

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_manifest(raw_manifest=raw)

    error = exc_info.value
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert observed_timezone.observations == 2
    assert error.__cause__ is None
    assert error.__context__ is None
    assert _TZINFO_PRIVATE_MARKER not in rendered


class _OffsetSequenceTzinfo(tzinfo_base):
    """Return a private per-instance offset sequence for exact call accounting."""

    __slots__ = ("_offsets", "observations")

    def __init__(self, offsets: tuple[timedelta, ...]) -> None:
        self._offsets = offsets
        self.observations = 0

    def utcoffset(self, dt: datetime | None) -> timedelta:
        observation = self.observations
        self.observations += 1
        if observation >= len(self._offsets):
            raise ValueError(_TZINFO_PRIVATE_MARKER)
        return self._offsets[observation]

    def tzname(self, dt: datetime | None) -> str:
        return "offset-sequence"

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)


def test_per_instance_offset_sequences_reject_inconsistent_normalized_starts() -> None:
    actual_timezone = _OffsetSequenceTzinfo((timedelta(0), timedelta(hours=1)))
    file_timezone = _OffsetSequenceTzinfo((timedelta(0), timedelta(0)))
    actual_start = datetime(2024, 1, 1, 1, tzinfo=actual_timezone)
    file_start = datetime(2024, 1, 1, 1, tzinfo=file_timezone)
    file_end = START + timedelta(hours=1, minutes=1)
    raw = _make_raw_manifest(
        actual_start=actual_start,
        actual_end=file_end,
        files=(
            OutputFileInfo(
                name="BTC-USDT-1m.jsonl",
                records=1,
                range_start=file_start,
                range_end=file_end,
            ),
        ),
        record_count=1,
    )

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_manifest(raw_manifest=raw)

    error = exc_info.value
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert actual_timezone.observations == 2
    assert file_timezone.observations == 2
    assert error.__cause__ is None
    assert error.__context__ is None
    assert _TZINFO_PRIVATE_MARKER not in rendered


class _StableFixedOffsetTzinfo(tzinfo_base):
    """Real custom fixed-offset timezone accepted by the normalization contract."""

    __slots__ = ("_offset",)

    def __init__(self, offset: timedelta) -> None:
        self._offset = offset

    def utcoffset(self, dt: datetime | None) -> timedelta:
        return self._offset

    def tzname(self, dt: datetime | None) -> str:
        return "stable-fixed-offset"

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)


def test_stable_custom_fixed_offset_is_accepted_and_normalized_to_utc() -> None:
    stable_timezone = _StableFixedOffsetTzinfo(timedelta(hours=5, minutes=30))
    local_requested_start = datetime(2024, 1, 1, 5, 30, tzinfo=stable_timezone)
    local_requested_end = datetime(2024, 1, 2, 5, 30, tzinfo=stable_timezone)
    local_file_end = datetime(2024, 1, 1, 5, 31, tzinfo=stable_timezone)
    raw = _make_raw_manifest(
        requested_start=local_requested_start,
        requested_end=local_requested_end,
        actual_start=local_requested_start,
        actual_end=local_file_end,
        server_time=local_requested_end,
        files=(
            OutputFileInfo(
                name="BTC-USDT-1m.jsonl",
                records=1,
                range_start=local_requested_start,
                range_end=local_file_end,
            ),
        ),
        record_count=1,
    )

    manifest = build_manifest(raw_manifest=raw)

    assert type(stable_timezone) is _StableFixedOffsetTzinfo
    assert isinstance(stable_timezone, tzinfo_base)
    assert not isinstance(stable_timezone, timezone)
    assert manifest.requested_start == START
    assert manifest.requested_end == END
    assert manifest.requested_start.tzinfo is UTC
    assert manifest.requested_end.tzinfo is UTC


class _FailAfterSingleNormalizationTzinfo(tzinfo_base):
    """Allow two offset observations and expose any later re-observation."""

    __slots__ = ("_offset", "observations")

    def __init__(self, offset: timedelta) -> None:
        self._offset = offset
        self.observations = 0

    def utcoffset(self, dt: datetime | None) -> timedelta:
        self.observations += 1
        if self.observations > 2:
            raise ValueError(_TZINFO_PRIVATE_MARKER)
        return self._offset

    def tzname(self, dt: datetime | None) -> str:
        return "fail-after-single-normalization"

    def dst(self, dt: datetime | None) -> timedelta:
        return timedelta(0)


def test_cross_field_validation_performs_no_third_timezone_observation() -> None:
    observed_timezones: list[_FailAfterSingleNormalizationTzinfo] = []

    def guarded_datetime(*args: int) -> datetime:
        observed_timezone = _FailAfterSingleNormalizationTzinfo(timedelta(hours=2))
        observed_timezones.append(observed_timezone)
        return datetime(*args, tzinfo=observed_timezone)

    requested_start = guarded_datetime(2024, 1, 1, 2, 0)
    requested_end = guarded_datetime(2024, 1, 2, 2, 0)
    actual_start = guarded_datetime(2024, 1, 1, 2, 0)
    actual_end = guarded_datetime(2024, 1, 1, 2, 1)
    server_time = guarded_datetime(2024, 1, 2, 2, 0)
    file_start = guarded_datetime(2024, 1, 1, 2, 0)
    file_end = guarded_datetime(2024, 1, 1, 2, 1)
    raw = _make_raw_manifest(
        requested_start=requested_start,
        requested_end=requested_end,
        actual_start=actual_start,
        actual_end=actual_end,
        server_time=server_time,
        files=(
            OutputFileInfo(
                name="BTC-USDT-1m.jsonl",
                records=1,
                range_start=file_start,
                range_end=file_end,
            ),
        ),
        record_count=1,
    )

    manifest = build_manifest(raw_manifest=raw)

    assert manifest.requested_start == START
    assert manifest.requested_end == END
    assert len(observed_timezones) == 7
    assert tuple(zone.observations for zone in observed_timezones) == (2,) * 7


def test_file_range_start_datetime_subclass_rejected() -> None:
    raw_files = (
        OutputFileInfo(
            name="BTC-USDT-1m.jsonl",
            records=1,
            range_start=_DatetimeSubclass(2024, 1, 1, tzinfo=UTC),
            range_end=START + timedelta(minutes=1),
        ),
    )
    raw = _make_raw_manifest(files=raw_files)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "DatetimeSubclass" not in rendered


def test_file_range_end_datetime_subclass_rejected() -> None:
    raw_files = (
        OutputFileInfo(
            name="BTC-USDT-1m.jsonl",
            records=1,
            range_start=START,
            range_end=_DatetimeSubclass(2024, 1, 1, 0, 1, tzinfo=UTC),
        ),
    )
    raw = _make_raw_manifest(files=raw_files)

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "DatetimeSubclass" not in rendered


# ---------- Malicious int subclass ----------


class _MaliciousInt(int):
    """int subclass that raises on comparison."""

    def __lt__(self, other: object) -> bool:
        raise ValueError("malicious lt")

    def __le__(self, other: object) -> bool:
        raise ValueError("malicious le")

    def __gt__(self, other: object) -> bool:
        raise ValueError("malicious gt")

    def __ge__(self, other: object) -> bool:
        raise ValueError("malicious ge")


def test_malicious_int_subclass_on_records_raises_detached() -> None:
    raw_files = (
        OutputFileInfo(
            name="BTC-USDT-1m.jsonl",
            records=_MaliciousInt(1),
            range_start=START,
            range_end=START + timedelta(minutes=1),
        ),
    )
    raw = _make_raw_manifest(files=raw_files, record_count=_MaliciousInt(1))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "malicious" not in rendered.lower()
    assert "MaliciousInt" not in rendered


# ---------- C4A mapping: validate_manifest operation ----------


def test_c4a_validate_manifest_error_operation() -> None:
    """C4A maps validator failure to operation=validate_manifest."""
    raw = _make_raw_manifest(requested_start=_DatetimeSubclass(2024, 1, 1, tzinfo=UTC))

    with pytest.raises(ResearchManifestValidationError) as exc_info:
        build_research_manifest(
            raw,
            raw_manifest_sha256="b" * 64,
            files=[],
            research_checksum=EMPTY_SHA256,
            failure_checksum=EMPTY_SHA256,
            max_line_bytes=1_048_576,
            completion_status="complete",
        )

    error = exc_info.value
    assert error.field == "raw_manifest"
    assert error.__cause__ is None
    assert error.__context__ is None


# ---------- Canonical valid manifests remain accepted ----------


def test_canonical_valid_manifest_accepted() -> None:
    raw = _make_raw_manifest(
        actual_start=START,
        actual_end=START + timedelta(minutes=1),
        server_time=END,
    )

    manifest = build_research_manifest(
        raw,
        raw_manifest_sha256="b" * 64,
        files=[make_artifact()],
        research_checksum="e" * 64,
        failure_checksum=EMPTY_SHA256,
        max_line_bytes=1_048_576,
        completion_status="complete",
    )

    assert manifest is not None


def test_non_utc_offset_valid_manifest_accepted() -> None:
    plus_seven = timezone(timedelta(hours=7))
    utc_equiv_start = datetime(2024, 1, 1, 7, 0, 0, tzinfo=plus_seven)
    utc_equiv_end = datetime(2024, 1, 1, 7, 1, 0, tzinfo=plus_seven)
    raw = _make_raw_manifest(
        requested_start=utc_equiv_start,
        requested_end=utc_equiv_end,
        actual_start=utc_equiv_start,
        actual_end=utc_equiv_end,
        server_time=datetime(2024, 1, 2, tzinfo=plus_seven),
        files=(
            _make_output_file(
                name="BTC-USDT-1m.jsonl",
                range_start=utc_equiv_start,
                range_end=utc_equiv_end,
            ),
        ),
    )

    manifest = build_research_manifest(
        raw,
        raw_manifest_sha256="b" * 64,
        files=[make_artifact()],
        research_checksum="e" * 64,
        failure_checksum=EMPTY_SHA256,
        max_line_bytes=1_048_576,
        completion_status="complete",
    )

    assert manifest is not None
    assert manifest.requested_start.tzinfo is UTC


# ---------- python -O ----------


def test_h1_fixes_work_under_optimized_mode() -> None:
    script = (
        "from packages.market_data.datasets.conversion_manifest import "
        "ResearchManifestValidationError, build_research_manifest\n"
        "from packages.market_data.datasets.downloader import (\n"
        "    DATASET_DOWNLOAD_VERSION, DOWNLOADER_VERSION, DownloadManifest\n"
        ")\n"
        "from packages.market_data.datasets.metadata import DATASET_SCHEMA_VERSION\n"
        "from packages.market_data.datasets.research_format import RESEARCH_SOURCE\n"
        "from datetime import UTC, datetime, timedelta\n"
        "start = datetime(2024, 1, 1, tzinfo=UTC)\n"
        "end = start + timedelta(days=1)\n"
        # datetime subclass on required field
        "class _DT(datetime): pass\n"
        "raw = DownloadManifest(\n"
        "    dataset_id='id', dataset_version=DATASET_DOWNLOAD_VERSION,\n"
        "    downloader_version=DOWNLOADER_VERSION,\n"
        "    schema_version=DATASET_SCHEMA_VERSION,\n"
        "    source=RESEARCH_SOURCE, exchange='binance', market_type='spot',\n"
        "    symbols=('BTC/USDT',), intervals=('1m',),\n"
        "    requested_start=_DT(2024, 1, 1, tzinfo=UTC),\n"
        "    requested_end=end,\n"
        "    actual_start=None, actual_end=None, record_count=0,\n"
        "    files=(), completion_status='complete',\n"
        "    failure=None, page_limit=1000, resume=False, server_time=end,\n"
        ")\n"
        "try:\n"
        "    build_research_manifest(\n"
        "        raw,\n"
        "        raw_manifest_sha256='b' * 64,\n"
        "        files=[],\n"
        "        research_checksum='" + EMPTY_SHA256 + "',\n"
        "        failure_checksum='" + EMPTY_SHA256 + "',\n"
        "        max_line_bytes=1048576,\n"
        "        completion_status='complete',\n"
        "    )\n"
        "    raise SystemExit(1)\n"
        "except ResearchManifestValidationError as e:\n"
        "    if e.__cause__ is None and e.__context__ is None:\n"
        "        raise SystemExit(0)\n"
        "    raise SystemExit(2)\n"
    )
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
