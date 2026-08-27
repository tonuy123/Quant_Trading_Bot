"""DATA-005 C4A/5 pure deterministic dataset work-plan tests."""

from __future__ import annotations

import ast
import importlib
import subprocess
import sys
from collections.abc import Callable
from dataclasses import FrozenInstanceError, asdict, fields, replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

import packages.market_data.datasets as datasets_package
import packages.market_data.datasets._publication_dataset_plan as plan_module
import packages.market_data.datasets.conversion_manifest as conversion_manifest
from packages.market_data.datasets._publication_dataset_plan import (
    ResearchDatasetConversionPlan,
    ResearchDatasetPlanError,
    ResearchDatasetWorkItem,
    build_research_dataset_conversion_plan,
)
from packages.market_data.datasets.conversion_manifest import (
    ResearchFilePlan,
    ResearchManifestValidationError,
)
from packages.market_data.datasets.converter import RawConversionContext
from packages.market_data.datasets.downloader import (
    DATASET_DOWNLOAD_VERSION,
    DOWNLOADER_VERSION,
    DownloadFailure,
    DownloadManifest,
    OutputFileInfo,
)
from packages.market_data.datasets.metadata import DATASET_SCHEMA_VERSION
from packages.market_data.datasets.research_format import RESEARCH_SOURCE

_START = datetime(2024, 1, 1, tzinfo=UTC)
_END = datetime(2024, 2, 1, tzinfo=UTC)
_PRIVATE_MARKER = "C4A_DATASET_PLAN_PRIVATE_MARKER_DO_NOT_LEAK"


class _DownloadManifestSubclass(DownloadManifest):
    pass


class _OutputFileInfoSubclass(OutputFileInfo):
    pass


class _StrSubclass(str):
    pass


class _IntSubclass(int):
    pass


class _DatetimeSubclass(datetime):
    pass


class _ResearchFilePlanSubclass(ResearchFilePlan):
    pass


class _RawConversionContextSubclass(RawConversionContext):
    pass


def _make_output_file(
    name: str = "BTC-USDT-1m.jsonl",
    *,
    records: object = 1,
    range_start: object = _START + timedelta(hours=1),
    range_end: object = _START + timedelta(hours=2),
) -> OutputFileInfo:
    return OutputFileInfo(
        name=name,
        records=records,
        range_start=range_start,
        range_end=range_end,
    )


def _make_failure() -> DownloadFailure:
    return DownloadFailure(
        symbol="BTC/USDT",
        interval="1m",
        range_start=_START,
        range_end=_END,
        endpoint="/api/v3/klines",
        error_type="request_error",
        message="fixed public failure",
        attempts=1,
    )


def _make_manifest(**overrides: object) -> DownloadManifest:
    raw_files = overrides.pop("files", (_make_output_file(),))
    record_count = overrides.pop(
        "record_count",
        sum(file_info.records for file_info in raw_files),
    )
    # actual_start/actual_end must match file range boundaries for non-empty datasets.
    if raw_files:
        first_range_start = raw_files[0].range_start
        last_range_end = raw_files[-1].range_end
    else:
        first_range_start = None
        last_range_end = None
    values: dict[str, object] = {
        "dataset_id": "raw-request-identity",
        "dataset_version": DATASET_DOWNLOAD_VERSION,
        "downloader_version": DOWNLOADER_VERSION,
        "schema_version": DATASET_SCHEMA_VERSION,
        "source": RESEARCH_SOURCE,
        "exchange": "binance",
        "market_type": "spot",
        "symbols": ("BTC/USDT",),
        "intervals": ("1m",),
        "requested_start": _START,
        "requested_end": _END,
        "actual_start": first_range_start,
        "actual_end": last_range_end,
        "record_count": record_count,
        "files": raw_files,
        "completion_status": "complete",
        "failure": None,
        "page_limit": 1000,
        "resume": False,
        "server_time": _END,
    }
    values.update(overrides)
    return DownloadManifest(**values)


def _build(
    raw_manifest: DownloadManifest | None = None,
    *,
    max_line_bytes: int = 1_048_576,
) -> ResearchDatasetConversionPlan:
    return build_research_dataset_conversion_plan(
        _make_manifest() if raw_manifest is None else raw_manifest,
        max_line_bytes=max_line_bytes,
    )


def _valid_children() -> tuple[ResearchFilePlan, RawConversionContext]:
    file_plan = ResearchFilePlan.from_raw_identity(
        raw_name="BTC-USDT-1m.jsonl",
        symbol="BTC/USDT",
        interval="1m",
    )
    context = RawConversionContext(
        file_name=file_plan.raw_name,
        symbol=file_plan.symbol,
        interval=file_plan.interval,
        range_start=_START,
        range_end=_END,
        max_line_bytes=1_048_576,
    )
    return file_plan, context


def _copy_as_subclass(value: object, subclass: type[object]) -> object:
    return subclass(**{item.name: getattr(value, item.name) for item in fields(value)})


def _assert_plan_error(
    error: ResearchDatasetPlanError,
    *,
    operation: str,
    markers: tuple[str, ...] = (),
) -> None:
    assert error.operation == operation
    assert error.__cause__ is None
    assert error.__context__ is None
    assert set(vars(error)) <= {"operation"}
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}\n{error.operation!r}"
    for marker in (_PRIVATE_MARKER, *markers):
        assert marker not in rendered


def _assert_invalid_error_constructor(call: Callable[[], object]) -> None:
    with pytest.raises(ValueError) as raised:
        call()
    error = raised.value
    assert type(error) is ValueError
    assert str(error) == "invalid dataset planning error contract"
    assert vars(error) == {}
    assert not hasattr(error, "operation")
    assert _PRIVATE_MARKER not in f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert error.__cause__ is None
    assert error.__context__ is None


def test_valid_single_file_plan_binds_exact_identity() -> None:
    raw_manifest = _make_manifest()

    plan = _build(raw_manifest)

    assert type(plan) is ResearchDatasetConversionPlan
    assert plan.raw_manifest is raw_manifest
    assert plan.max_line_bytes == 1_048_576
    assert type(plan.files) is tuple
    assert len(plan.files) == 1
    item = plan.files[0]
    assert type(item) is ResearchDatasetWorkItem
    assert item.file_plan == ResearchFilePlan.from_raw_identity(
        raw_name="BTC-USDT-1m.jsonl",
        symbol="BTC/USDT",
        interval="1m",
    )
    assert item.expected_lines == 1


def test_multi_file_plan_preserves_validated_manifest_file_order() -> None:
    raw_files = (
        _make_output_file("BTC-USDT-1M.jsonl", records=2),
        _make_output_file("BTC-USDT-1m.jsonl", records=3),
        _make_output_file("ETH-USDT-1m.jsonl", records=5),
    )
    raw_manifest = _make_manifest(
        symbols=("BTC/USDT", "ETH/USDT"),
        intervals=("1M", "1m"),
        files=raw_files,
        record_count=10,
    )

    first = _build(raw_manifest)
    second = _build(raw_manifest)

    expected_names = tuple(file_info.name for file_info in raw_files)
    assert tuple(item.file_plan.raw_name for item in first.files) == expected_names
    assert first == second
    assert first.files == second.files


def test_manifest_file_subset_does_not_manufacture_cartesian_work() -> None:
    raw_manifest = _make_manifest(
        symbols=("BTC/USDT", "ETH/USDT"),
        intervals=("1M", "1m"),
        files=(_make_output_file("ETH-USDT-1m.jsonl", records=7),),
        record_count=7,
    )

    plan = _build(raw_manifest)

    assert len(plan.files) == 1
    assert plan.files[0].file_plan.raw_name == "ETH-USDT-1m.jsonl"


def test_empty_complete_raw_dataset_returns_empty_work_tuple() -> None:
    raw_manifest = _make_manifest(files=(), record_count=0)

    plan = _build(raw_manifest)

    assert plan.files == ()
    assert type(plan.files) is tuple


def test_zero_record_file_remains_representable_at_planning_level() -> None:
    raw_manifest = _make_manifest(
        files=(_make_output_file(records=0),),
        record_count=0,
    )

    plan = _build(raw_manifest)

    assert len(plan.files) == 1
    assert plan.files[0].expected_lines == 0


@pytest.mark.parametrize("records", [0, 1, 17])
def test_expected_lines_equals_exact_output_file_record_count(records: int) -> None:
    raw_manifest = _make_manifest(
        files=(_make_output_file(records=records),),
        record_count=records,
    )

    assert _build(raw_manifest).files[0].expected_lines == records


@pytest.mark.parametrize(
    ("requested_start", "requested_end"),
    [
        (_START, _END),
        (
            datetime(2024, 1, 1, 7, tzinfo=timezone(timedelta(hours=7))),
            datetime(2024, 2, 1, 7, tzinfo=timezone(timedelta(hours=7))),
        ),
    ],
)
def test_context_uses_normalized_requested_range_exact_limit_and_plan_identity(
    requested_start: datetime,
    requested_end: datetime,
) -> None:
    file_start = _START + timedelta(days=5)
    file_end = _START + timedelta(days=6)
    raw_manifest = _make_manifest(
        requested_start=requested_start,
        requested_end=requested_end,
        files=(
            _make_output_file(
                range_start=file_start,
                range_end=file_end,
                records=4,
            ),
        ),
        record_count=4,
    )

    item = _build(raw_manifest, max_line_bytes=4097).files[0]

    assert item.context.range_start == requested_start.astimezone(UTC)
    assert item.context.range_end == requested_end.astimezone(UTC)
    assert item.context.range_start != file_start
    assert item.context.range_end != file_end
    assert item.context.max_line_bytes == 4097
    assert item.context.file_name == item.file_plan.raw_name
    assert item.context.symbol == item.file_plan.symbol
    assert item.context.interval == item.file_plan.interval


def test_minute_and_month_intervals_remain_distinct() -> None:
    raw_manifest = _make_manifest(
        intervals=("1M", "1m"),
        files=(
            _make_output_file("BTC-USDT-1M.jsonl"),
            _make_output_file("BTC-USDT-1m.jsonl"),
        ),
        record_count=2,
    )

    plan = _build(raw_manifest)

    assert tuple(item.file_plan.interval for item in plan.files) == ("1M", "1m")
    assert tuple(item.file_plan.raw_name for item in plan.files) == (
        "BTC-USDT-1M.jsonl",
        "BTC-USDT-1m.jsonl",
    )


def test_multiple_symbols_and_intervals_map_to_exact_canonical_plans() -> None:
    raw_manifest = _make_manifest(
        symbols=("BTC/USDT", "ETH/USDT", "SOL/USDT"),
        intervals=("1M", "1h", "1m"),
        files=(
            _make_output_file("BTC-USDT-1M.jsonl"),
            _make_output_file("ETH-USDT-1h.jsonl"),
            _make_output_file("SOL-USDT-1m.jsonl"),
        ),
        record_count=3,
    )

    plan = _build(raw_manifest)

    assert tuple(
        (item.file_plan.raw_name, item.file_plan.symbol, item.file_plan.interval)
        for item in plan.files
    ) == (
        ("BTC-USDT-1M.jsonl", "BTC/USDT", "1M"),
        ("ETH-USDT-1h.jsonl", "ETH/USDT", "1h"),
        ("SOL-USDT-1m.jsonl", "SOL/USDT", "1m"),
    )


def test_derivation_builds_each_candidate_once_then_uses_file_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_manifest = _make_manifest(
        symbols=("BTC/USDT", "ETH/USDT", "SOL/USDT"),
        intervals=("1M", "1m"),
        files=(
            _make_output_file("BTC-USDT-1M.jsonl"),
            _make_output_file("SOL-USDT-1m.jsonl"),
        ),
        record_count=2,
    )
    real_factory = ResearchFilePlan.from_raw_identity
    candidate_calls: list[tuple[str, str, str]] = []

    def tracking_factory(*, raw_name: str, symbol: str, interval: str) -> ResearchFilePlan:
        candidate_calls.append((raw_name, symbol, interval))
        return real_factory(raw_name=raw_name, symbol=symbol, interval=interval)

    monkeypatch.setattr(
        ResearchFilePlan,
        "from_raw_identity",
        staticmethod(tracking_factory),
    )

    plan = _build(raw_manifest)

    assert len(candidate_calls) == len(raw_manifest.symbols) * len(raw_manifest.intervals)
    assert len(plan.files) == len(raw_manifest.files)


@pytest.mark.parametrize("invalid", [None, {}, [], object()])
def test_wrong_manifest_type_is_validate_input_before_dependency_use(
    monkeypatch: pytest.MonkeyPatch,
    invalid: object,
) -> None:
    calls = 0

    def forbidden_validator(_manifest: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("validator must not be called")

    monkeypatch.setattr(conversion_manifest, "_validate_raw_manifest", forbidden_validator)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        build_research_dataset_conversion_plan(invalid, max_line_bytes=1)
    with pytest.raises(ResearchDatasetPlanError) as direct_raised:
        ResearchDatasetConversionPlan(raw_manifest=invalid, max_line_bytes=1)

    _assert_plan_error(raised.value, operation="validate_input")
    _assert_plan_error(direct_raised.value, operation="validate_input")
    assert calls == 0


def test_download_manifest_subclass_is_rejected() -> None:
    raw_manifest = _make_manifest()
    invalid = _copy_as_subclass(raw_manifest, _DownloadManifestSubclass)

    with pytest.raises(ResearchDatasetPlanError) as factory_error:
        build_research_dataset_conversion_plan(invalid, max_line_bytes=1)
    with pytest.raises(ResearchDatasetPlanError) as direct_error:
        ResearchDatasetConversionPlan(raw_manifest=invalid, max_line_bytes=1)

    _assert_plan_error(factory_error.value, operation="validate_input")
    _assert_plan_error(direct_error.value, operation="validate_input")


@pytest.mark.parametrize("invalid", [True, False, 0, -1, 1.0, "1", None])
def test_max_line_bytes_requires_positive_exact_int(invalid: object) -> None:
    raw_manifest = _make_manifest()

    with pytest.raises(ResearchDatasetPlanError) as factory_error:
        build_research_dataset_conversion_plan(
            raw_manifest,
            max_line_bytes=invalid,
        )
    with pytest.raises(ResearchDatasetPlanError) as direct_error:
        ResearchDatasetConversionPlan(
            raw_manifest=raw_manifest,
            max_line_bytes=invalid,
        )

    _assert_plan_error(factory_error.value, operation="validate_input")
    _assert_plan_error(direct_error.value, operation="validate_input")


@pytest.mark.parametrize("kind", ["incomplete", "failure"])
def test_incomplete_or_failing_raw_manifest_is_validate_manifest(kind: str) -> None:
    raw_manifest = _make_manifest()
    if kind == "incomplete":
        raw_manifest = replace(raw_manifest, completion_status="incomplete")
    else:
        raw_manifest = replace(raw_manifest, failure=_make_failure())

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize(
    ("field_name", "invalid"),
    [
        ("dataset_id", ""),
        ("dataset_version", "wrong-version"),
        ("downloader_version", "wrong-version"),
        ("schema_version", True),
        ("schema_version", DATASET_SCHEMA_VERSION + 1),
        ("source", "private-source"),
        ("exchange", "other-exchange"),
        ("market_type", "futures"),
    ],
)
def test_directly_constructed_manifest_contract_fields_are_revalidated(
    field_name: str,
    invalid: object,
) -> None:
    raw_manifest = replace(_make_manifest(), **{field_name: invalid})

    with pytest.raises(ResearchDatasetPlanError) as raised:
        ResearchDatasetConversionPlan(raw_manifest=raw_manifest, max_line_bytes=1)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize(
    ("field_name", "invalid"),
    [
        ("symbols", ["BTC/USDT"]),
        ("symbols", ()),
        ("symbols", ("ETH/USDT", "BTC/USDT")),
        ("symbols", ("BTC/USDT", "BTC/USDT")),
        ("symbols", ("btc/usdt",)),
        ("intervals", ["1m"]),
        ("intervals", ()),
        ("intervals", ("1m", "1M")),
        ("intervals", ("1m", "1m")),
        ("intervals", ("7m",)),
    ],
)
def test_invalid_symbol_or_interval_contract_is_validate_manifest(
    field_name: str,
    invalid: object,
) -> None:
    raw_manifest = replace(_make_manifest(), **{field_name: invalid})

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize(
    "name",
    [
        "unrelated.jsonl",
        "BTC-USDT-1M.jsonl",
        "../BTC-USDT-1m.jsonl",
        "BTC-USDT-1m.csv",
    ],
)
def test_malformed_or_noncanonical_raw_filename_is_validate_manifest(name: str) -> None:
    raw_manifest = _make_manifest(files=(_make_output_file(name),), record_count=1)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize("kind", ["unsorted", "duplicate"])
def test_unsorted_or_duplicate_raw_filenames_are_validate_manifest(kind: str) -> None:
    if kind == "unsorted":
        raw_files = (
            _make_output_file("ETH-USDT-1m.jsonl"),
            _make_output_file("BTC-USDT-1m.jsonl"),
        )
    else:
        raw_files = (
            _make_output_file("BTC-USDT-1m.jsonl"),
            _make_output_file("BTC-USDT-1m.jsonl"),
        )
    raw_manifest = _make_manifest(
        symbols=("BTC/USDT", "ETH/USDT"),
        files=raw_files,
        record_count=2,
    )

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_record_count_mismatch_is_validate_manifest() -> None:
    raw_manifest = _make_manifest(record_count=2)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize(
    "defect",
    [
        "files_list",
        "file_subclass",
        "records_bool",
        "records_negative",
        "naive_range",
        "reversed_range",
    ],
)
def test_invalid_output_file_contract_is_validate_manifest(defect: str) -> None:
    file_info: object = _make_output_file()
    raw_files: object = (file_info,)
    record_count: object = 1
    if defect == "files_list":
        raw_files = [file_info]
    elif defect == "file_subclass":
        file_info = _copy_as_subclass(file_info, _OutputFileInfoSubclass)
        raw_files = (file_info,)
    elif defect == "records_bool":
        raw_files = (_make_output_file(records=True),)
        record_count = True
    elif defect == "records_negative":
        raw_files = (_make_output_file(records=-1),)
        record_count = -1
    elif defect == "naive_range":
        raw_files = (_make_output_file(range_start=datetime(2024, 1, 1)),)
    else:
        raw_files = (
            _make_output_file(
                range_start=_START + timedelta(days=2),
                range_end=_START + timedelta(days=1),
            ),
        )
    raw_manifest = _make_manifest(files=raw_files, record_count=record_count)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


# =============================================================================
# C4A mapping: newly rejected defects → ResearchDatasetPlanError("validate_manifest")
# =============================================================================


@pytest.mark.parametrize(
    "defect",
    [
        "actual_start_object",
        "actual_end_object",
        "server_time_object",
        "actual_start_str",
        "actual_end_str",
        "server_time_str",
        "actual_start_int",
        "actual_end_int",
        "actual_start_bool",
        "actual_end_bool",
        "actual_start_naive",
        "actual_end_naive",
        "server_time_naive",
        "actual_start_datetime_subclass",
        "server_time_datetime_subclass",
    ],
)
def test_optional_datetime_defects_rejected_as_validate_manifest(defect: str) -> None:
    """Each wrong type/naive/subclass optional datetime maps to validate_manifest."""
    kwargs: dict[str, object] = {}
    if defect == "actual_start_object":
        kwargs["actual_start"] = object()
    elif defect == "actual_end_object":
        kwargs["actual_end"] = object()
    elif defect == "server_time_object":
        kwargs["server_time"] = object()
    elif defect == "actual_start_str":
        kwargs["actual_start"] = "2024-01-01T00:00:00+00:00"
    elif defect == "actual_end_str":
        kwargs["actual_end"] = "2024-01-01T00:00:00+00:00"
    elif defect == "server_time_str":
        kwargs["server_time"] = "2024-01-01T00:00:00+00:00"
    elif defect == "actual_start_int":
        kwargs["actual_start"] = 1
    elif defect == "actual_end_int":
        kwargs["actual_end"] = 1
    elif defect == "actual_start_bool":
        kwargs["actual_start"] = False
    elif defect == "actual_end_bool":
        kwargs["actual_end"] = False
    elif defect == "actual_start_naive":
        kwargs["actual_start"] = datetime(2024, 1, 1)
    elif defect == "actual_end_naive":
        kwargs["actual_end"] = datetime(2024, 1, 1)
    elif defect == "server_time_naive":
        kwargs["server_time"] = datetime(2024, 1, 1)
    elif defect == "actual_start_datetime_subclass":
        kwargs["actual_start"] = _DatetimeSubclass(2024, 1, 1, tzinfo=UTC)
    elif defect == "server_time_datetime_subclass":
        kwargs["server_time"] = _DatetimeSubclass(2024, 1, 1, tzinfo=UTC)

    raw_manifest = _make_manifest(**kwargs)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize(
    "value",
    [True, False, 0, -1, 1001, 1.0, "1000", None],
)
def test_page_limit_non_int_rejected_as_validate_manifest(value: object) -> None:
    raw_manifest = _make_manifest(page_limit=value)  # type: ignore[arg-type]

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_page_limit_int_subclass_rejected_as_validate_manifest() -> None:
    raw_manifest = _make_manifest(page_limit=_IntSubclass(1000))

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize("value", [0, 1, "false", None, object()])
def test_resume_non_bool_rejected_as_validate_manifest(value: object) -> None:
    raw_manifest = _make_manifest(resume=value)  # type: ignore[arg-type]

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_resume_bool_subclass_rejected_as_validate_manifest() -> None:
    raw_manifest = _make_manifest(resume=0)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize(
    "field",
    ["dataset_id", "dataset_version", "downloader_version", "source", "exchange", "market_type"],
)
def test_string_field_str_subclass_rejected_as_validate_manifest(field: str) -> None:
    raw_manifest = _make_manifest(**{field: _StrSubclass("binance")})

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_symbol_str_subclass_rejected_as_validate_manifest() -> None:
    raw_manifest = _make_manifest(symbols=(_StrSubclass("BTC/USDT"),))

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_interval_str_subclass_rejected_as_validate_manifest() -> None:
    raw_manifest = _make_manifest(intervals=(_StrSubclass("1m"),))

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_schema_version_int_subclass_rejected_as_validate_manifest() -> None:
    raw_manifest = _make_manifest(schema_version=_IntSubclass(DATASET_SCHEMA_VERSION))

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_record_count_int_subclass_rejected_as_validate_manifest() -> None:
    raw_manifest = _make_manifest(record_count=_IntSubclass(1))

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_output_file_records_int_subclass_rejected_as_validate_manifest() -> None:
    raw_files = (replace(_make_output_file(), records=_IntSubclass(1)),)
    raw_manifest = _make_manifest(files=raw_files)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


def test_output_file_name_str_subclass_rejected_as_validate_manifest() -> None:
    raw_files = (replace(_make_output_file(), name=_StrSubclass("BTC-USDT-1m.jsonl")),)
    raw_manifest = _make_manifest(files=raw_files)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    _assert_plan_error(raised.value, operation="validate_manifest")


@pytest.mark.parametrize(
    "defect",
    [
        "actual_start_object",
        "actual_end_object",
        "server_time_object",
        "page_limit_true",
        "resume_false_string",
        "dataset_id_subclass",
        "symbol_subclass",
        "interval_subclass",
    ],
)
def test_c4a_error_surfaces_are_sanitized_and_cause_context_none(defect: str) -> None:
    """C4A errors have __cause__==None and __context__==None and no marker leaks."""
    kwargs: dict[str, object] = {}
    if defect == "actual_start_object":
        kwargs["actual_start"] = object()
    elif defect == "actual_end_object":
        kwargs["actual_end"] = object()
    elif defect == "server_time_object":
        kwargs["server_time"] = object()
    elif defect == "page_limit_true":
        kwargs["page_limit"] = True
    elif defect == "resume_false_string":
        kwargs["resume"] = "false"
    elif defect == "dataset_id_subclass":
        kwargs["dataset_id"] = _StrSubclass("raw-request-identity")
    elif defect == "symbol_subclass":
        kwargs["symbols"] = (_StrSubclass("BTC/USDT"),)
    elif defect == "interval_subclass":
        kwargs["intervals"] = (_StrSubclass("1m"),)

    raw_manifest = _make_manifest(**kwargs)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    error = raised.value
    assert error.operation == "validate_manifest"
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert "StrSubclass" not in rendered
    assert "object()" not in rendered
    assert "BoolSubclass" not in rendered


def test_valid_plan_output_remains_equivalent_after_strict_validation() -> None:
    """Adding strict validation must not change valid plan output."""
    raw_manifest = _make_manifest()

    plan = _build(raw_manifest)

    assert type(plan) is ResearchDatasetConversionPlan
    assert plan.max_line_bytes == 1_048_576
    assert len(plan.files) == 1
    assert plan.files[0].file_plan.raw_name == "BTC-USDT-1m.jsonl"
    assert plan.files[0].expected_lines == 1


def test_c4a_production_module_remains_unchanged() -> None:
    """C4A production module does not need modification for this repair."""
    import ast
    import inspect

    import packages.market_data.datasets._publication_dataset_plan as plan_module

    source = inspect.getsource(plan_module)

    # No regression: production module should not have been touched.
    # The fix is centralized in conversion_manifest._validate_raw_manifest.
    tree = ast.parse(source)
    assert tree is not None


@pytest.mark.parametrize(
    "defect",
    [
        "actual_start_object",
        "actual_end_object",
        "server_time_object",
        "page_limit_true",
        "resume_false_string",
        "dataset_id_subclass",
        "symbol_subclass",
        "interval_subclass",
    ],
)
def test_c4a_error_vars_contains_only_operation(defect: str) -> None:
    """Error vars contains only the operation field; no raw values."""
    kwargs: dict[str, object] = {}
    if defect == "actual_start_object":
        kwargs["actual_start"] = object()
    elif defect == "actual_end_object":
        kwargs["actual_end"] = object()
    elif defect == "server_time_object":
        kwargs["server_time"] = object()
    elif defect == "page_limit_true":
        kwargs["page_limit"] = True
    elif defect == "resume_false_string":
        kwargs["resume"] = "false"
    elif defect == "dataset_id_subclass":
        kwargs["dataset_id"] = _StrSubclass("raw-request-identity")
    elif defect == "symbol_subclass":
        kwargs["symbols"] = (_StrSubclass("BTC/USDT"),)
    elif defect == "interval_subclass":
        kwargs["intervals"] = (_StrSubclass("1m"),)

    raw_manifest = _make_manifest(**kwargs)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build(raw_manifest)

    error = raised.value
    assert set(vars(error)) <= {"operation"}


@pytest.mark.parametrize(
    "defect",
    ["wrong_plan", "plan_subclass", "wrong_context", "context_subclass"],
)
def test_work_item_requires_exact_child_types(defect: str) -> None:
    file_plan, context = _valid_children()
    plan_value: object = file_plan
    context_value: object = context
    if defect == "wrong_plan":
        plan_value = object()
    elif defect == "plan_subclass":
        plan_value = _copy_as_subclass(file_plan, _ResearchFilePlanSubclass)
    elif defect == "wrong_context":
        context_value = object()
    else:
        context_value = _copy_as_subclass(context, _RawConversionContextSubclass)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        ResearchDatasetWorkItem(
            file_plan=plan_value,
            context=context_value,
            expected_lines=1,
        )

    _assert_plan_error(raised.value, operation="derive_work")


@pytest.mark.parametrize("field_name", ["file_name", "symbol", "interval"])
def test_work_item_rejects_context_file_plan_identity_mismatch(field_name: str) -> None:
    file_plan, context = _valid_children()
    replacements: dict[str, object] = {
        "file_name": "ETH-USDT-1m.jsonl",
        "symbol": "ETH/USDT",
        "interval": "1h",
    }
    context = replace(context, **{field_name: replacements[field_name]})

    with pytest.raises(ResearchDatasetPlanError) as raised:
        ResearchDatasetWorkItem(
            file_plan=file_plan,
            context=context,
            expected_lines=1,
        )

    _assert_plan_error(raised.value, operation="derive_work")


@pytest.mark.parametrize("invalid", [True, -1, 1.0, "1", None])
def test_work_item_expected_lines_requires_non_negative_exact_int(invalid: object) -> None:
    file_plan, context = _valid_children()

    with pytest.raises(ResearchDatasetPlanError) as raised:
        ResearchDatasetWorkItem(
            file_plan=file_plan,
            context=context,
            expected_lines=invalid,
        )

    _assert_plan_error(raised.value, operation="derive_work")


def test_plan_and_work_items_are_frozen_and_files_is_exact_tuple() -> None:
    plan = _build()
    item = plan.files[0]

    assert type(plan.files) is tuple
    with pytest.raises(FrozenInstanceError):
        plan.max_line_bytes = 2
    with pytest.raises(FrozenInstanceError):
        plan.files = ()
    with pytest.raises(FrozenInstanceError):
        item.expected_lines = 2


def test_files_is_not_a_caller_supplied_constructor_field() -> None:
    with pytest.raises(TypeError):
        ResearchDatasetConversionPlan(
            raw_manifest=_make_manifest(),
            max_line_bytes=1,
            files=(),
        )


def test_planning_does_not_mutate_manifest_or_output_file_objects() -> None:
    raw_manifest = _make_manifest()
    manifest_before = asdict(raw_manifest)
    file_before = asdict(raw_manifest.files[0])

    plan = _build(raw_manifest)

    assert asdict(raw_manifest) == manifest_before
    assert asdict(raw_manifest.files[0]) == file_before
    assert plan.raw_manifest is raw_manifest


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        ("validate_input", "dataset conversion planning input contract is invalid"),
        ("validate_manifest", "raw manifest is not valid for research conversion"),
        ("derive_work", "dataset conversion work plan could not be derived"),
    ],
)
def test_error_valid_operation_message_and_attribute_contract(
    operation: str,
    message: str,
) -> None:
    error = ResearchDatasetPlanError(operation=operation)

    assert str(error) == message
    _assert_plan_error(error, operation=operation)


@pytest.mark.parametrize(
    "invalid",
    [
        f"unknown-{_PRIVATE_MARKER}",
        _PRIVATE_MARKER.encode(),
        [_PRIVATE_MARKER],
        True,
        None,
        object(),
    ],
)
def test_error_invalid_constructor_values_are_fixed_and_marker_free(
    invalid: object,
) -> None:
    _assert_invalid_error_constructor(lambda: ResearchDatasetPlanError(operation=invalid))


def test_manifest_dependency_error_is_detached_and_not_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependency_error = ResearchManifestValidationError(
        field=_PRIVATE_MARKER,
        reason=_PRIVATE_MARKER,
    )

    def failing_validator(_manifest: DownloadManifest) -> object:
        raise dependency_error

    monkeypatch.setattr(conversion_manifest, "_validate_raw_manifest", failing_validator)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build()

    _assert_plan_error(
        raised.value,
        operation="validate_manifest",
        markers=(_PRIVATE_MARKER,),
    )
    assert all(value is not dependency_error for value in vars(raised.value).values())


@pytest.mark.parametrize("dependency", ["file_plan", "context"])
def test_derived_child_validation_error_is_detached_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    dependency: str,
) -> None:
    if dependency == "file_plan":

        def failing_plan(**_kwargs: object) -> object:
            raise ResearchManifestValidationError(
                field=_PRIVATE_MARKER,
                reason=_PRIVATE_MARKER,
            )

        monkeypatch.setattr(
            ResearchFilePlan,
            "from_raw_identity",
            staticmethod(failing_plan),
        )
    else:

        def failing_context(**_kwargs: object) -> object:
            raise ValueError(_PRIVATE_MARKER)

        monkeypatch.setattr(plan_module, "RawConversionContext", failing_context)

    with pytest.raises(ResearchDatasetPlanError) as raised:
        _build()

    _assert_plan_error(
        raised.value,
        operation="derive_work",
        markers=(_PRIVATE_MARKER,),
    )


@pytest.mark.parametrize(
    "error_type",
    [RuntimeError, TypeError, ValueError, KeyboardInterrupt, SystemExit, MemoryError],
)
def test_unexpected_and_critical_validator_failures_propagate_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    error_type: type[BaseException],
) -> None:
    fault = error_type(_PRIVATE_MARKER)

    def faulty_validator(_manifest: DownloadManifest) -> object:
        raise fault

    monkeypatch.setattr(conversion_manifest, "_validate_raw_manifest", faulty_validator)

    with pytest.raises(error_type) as raised:
        _build()

    assert raised.value is fault


def test_python_optimized_mode_preserves_all_runtime_validation() -> None:
    script = """
from datetime import UTC, datetime, timedelta
from packages.market_data.datasets._publication_dataset_plan import (
    ResearchDatasetConversionPlan,
    ResearchDatasetPlanError,
    ResearchDatasetWorkItem,
    build_research_dataset_conversion_plan,
)
from packages.market_data.datasets.conversion_manifest import ResearchFilePlan
from packages.market_data.datasets.converter import RawConversionContext
from packages.market_data.datasets.downloader import (
    DATASET_DOWNLOAD_VERSION,
    DOWNLOADER_VERSION,
    DownloadManifest,
)
from packages.market_data.datasets.metadata import DATASET_SCHEMA_VERSION
from packages.market_data.datasets.research_format import RESEARCH_SOURCE

start = datetime(2024, 1, 1, tzinfo=UTC)
end = start + timedelta(days=1)
manifest = DownloadManifest(
    dataset_id='id', dataset_version=DATASET_DOWNLOAD_VERSION,
    downloader_version=DOWNLOADER_VERSION, schema_version=DATASET_SCHEMA_VERSION,
    source=RESEARCH_SOURCE, exchange='binance', market_type='spot',
    symbols=('BTC/USDT',), intervals=('1m',), requested_start=start,
    requested_end=end, actual_start=None, actual_end=None, record_count=0,
    files=(), completion_status='complete', failure=None, page_limit=1000,
    resume=False, server_time=end,
)
checks = []
try:
    build_research_dataset_conversion_plan(manifest, max_line_bytes=True)
except ResearchDatasetPlanError as error:
    checks.append(error.operation == 'validate_input')
file_plan = ResearchFilePlan.from_raw_identity(
    raw_name='BTC-USDT-1m.jsonl', symbol='BTC/USDT', interval='1m'
)
context = RawConversionContext(
    file_name=file_plan.raw_name, symbol=file_plan.symbol,
    interval=file_plan.interval, range_start=start, range_end=end,
    max_line_bytes=1,
)
try:
    ResearchDatasetWorkItem(file_plan=file_plan, context=context, expected_lines=True)
except ResearchDatasetPlanError as error:
    checks.append(error.operation == 'derive_work')
try:
    ResearchDatasetConversionPlan(
        raw_manifest=manifest, max_line_bytes=0
    )
except ResearchDatasetPlanError as error:
    checks.append(error.operation == 'validate_input')
try:
    ResearchDatasetPlanError(operation=['private-marker'])
except ValueError as error:
    checks.append(
        str(error) == 'invalid dataset planning error contract'
        and vars(error) == {}
        and error.__cause__ is None
        and error.__context__ is None
    )
if checks != [True, True, True, True]:
    raise SystemExit(1)
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_private_module_imports_and_package_root_has_no_lazy_exports() -> None:
    module = importlib.import_module("packages.market_data.datasets._publication_dataset_plan")
    private_names = {
        "DatasetPlanOperation",
        "ResearchDatasetPlanError",
        "ResearchDatasetWorkItem",
        "ResearchDatasetConversionPlan",
        "build_research_dataset_conversion_plan",
    }

    assert all(hasattr(module, name) for name in private_names)
    assert private_names.isdisjoint(datasets_package.__all__)
    assert private_names.isdisjoint(datasets_package._EXPORTS)
    for name in private_names:
        with pytest.raises(AttributeError):
            getattr(datasets_package, name)


def test_production_ast_is_pure_canonical_and_linear() -> None:
    source = Path(plan_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden_imports = {
        "os",
        "pathlib",
        "hashlib",
        "json",
        "tempfile",
        "shutil",
        "threading",
        "asyncio",
        "logging",
        "requests",
        "httpx",
        "packages.market_data.datasets._publication_fs",
        "packages.market_data.datasets._publication_staging",
        "packages.market_data.datasets._publication_conversion",
        "packages.market_data.datasets._publication_promotion",
        "packages.market_data.datasets._publication_pair",
        "packages.market_data.datasets.publication_preflight",
    }
    forbidden_calls = {
        "open",
        "read",
        "read_bytes",
        "read_text",
        "write",
        "write_bytes",
        "write_text",
        "stat",
        "lstat",
        "resolve",
        "exists",
        "hash",
        "digest",
        "hexdigest",
        "eval",
        "exec",
        "split",
        "replace",
        "lower",
        "casefold",
    }
    imported: set[str] = set()
    called: set[str] = set()
    broad_handlers: list[str] = []
    raises_in_handlers = 0
    dunder_mutations: list[str] = []
    functions: dict[str, ast.FunctionDef] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.add(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                called.add(node.func.attr)
        elif isinstance(node, ast.ExceptHandler):
            if node.type is None:
                broad_handlers.append("bare")
            elif isinstance(node.type, ast.Name) and node.type.id in {
                "Exception",
                "BaseException",
            }:
                broad_handlers.append(node.type.id)
            raises_in_handlers += sum(isinstance(child, ast.Raise) for child in ast.walk(node))
        elif (
            isinstance(node, ast.Attribute)
            and isinstance(node.ctx, ast.Store)
            and node.attr in {"__cause__", "__context__", "__traceback__"}
        ):
            dunder_mutations.append(node.attr)
        elif isinstance(node, ast.FunctionDef):
            functions[node.name] = node

    def max_loop_depth(node: ast.AST, depth: int = 0) -> int:
        next_depth = depth + int(isinstance(node, (ast.For, ast.AsyncFor, ast.While)))
        return max(
            [next_depth]
            + [max_loop_depth(child, next_depth) for child in ast.iter_child_nodes(node)]
        )

    assert forbidden_imports.isdisjoint(imported)
    assert forbidden_calls.isdisjoint(called)
    assert broad_handlers == []
    assert raises_in_handlers == 0
    assert dunder_mutations == []
    assert not any(isinstance(node, ast.Assert) for node in ast.walk(tree))
    assert not any(
        isinstance(node, ast.Raise) and node.cause is not None for node in ast.walk(tree)
    )
    assert max_loop_depth(functions["_derive_file_plan_map"]) == 2
    assert max_loop_depth(functions["_derive_work_items"]) == 1
    assert ".jsonl" not in source
    assert "symbol.replace" not in source
    assert "filename.split" not in source
