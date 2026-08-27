"""Pure deterministic planning for one raw-to-research dataset conversion.

This package-private slice derives immutable per-file work from one exact raw
manifest.  It performs no filesystem, parsing, hashing, conversion, promotion,
or manifest-publication work.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Literal, NoReturn

import packages.market_data.datasets.conversion_manifest as conversion_manifest
from packages.market_data.datasets.conversion_manifest import (
    ResearchFilePlan,
    ResearchManifestValidationError,
)
from packages.market_data.datasets.converter import RawConversionContext
from packages.market_data.datasets.downloader import DownloadManifest, OutputFileInfo

DatasetPlanOperation = Literal[
    "validate_input",
    "validate_manifest",
    "derive_work",
]

_ERROR_MESSAGES: MappingProxyType[DatasetPlanOperation, str] = MappingProxyType(
    {
        "validate_input": "dataset conversion planning input contract is invalid",
        "validate_manifest": "raw manifest is not valid for research conversion",
        "derive_work": "dataset conversion work plan could not be derived",
    }
)
_INVALID_ERROR_CONTRACT = "invalid dataset planning error contract"


class ResearchDatasetPlanError(ValueError):
    """Sanitized package-private dataset-planning failure."""

    __slots__ = ("operation",)
    operation: DatasetPlanOperation

    def __init__(self, *, operation: DatasetPlanOperation) -> None:
        message: str | None = None
        if type(operation) is str:
            message = _ERROR_MESSAGES.get(operation)
        if message is None:
            raise ValueError(_INVALID_ERROR_CONTRACT)
        self.operation = operation
        super().__init__(message)


def _raise_plan_error(operation: DatasetPlanOperation) -> NoReturn:
    raise ResearchDatasetPlanError(operation=operation)


@dataclass(frozen=True, kw_only=True, slots=True)
class ResearchDatasetWorkItem:
    """One immutable raw-file identity, conversion context, and line contract."""

    file_plan: ResearchFilePlan
    context: RawConversionContext
    expected_lines: int

    def __post_init__(self) -> None:
        if (
            type(self.file_plan) is not ResearchFilePlan
            or type(self.context) is not RawConversionContext
            or type(self.expected_lines) is not int
            or self.expected_lines < 0
        ):
            _raise_plan_error("derive_work")
        if (
            self.context.file_name != self.file_plan.raw_name
            or self.context.symbol != self.file_plan.symbol
            or self.context.interval != self.file_plan.interval
        ):
            _raise_plan_error("derive_work")


_ValidatedRawManifest = tuple[datetime, datetime, tuple[OutputFileInfo, ...]]


def _valid_plan_inputs(raw_manifest: object, max_line_bytes: object) -> bool:
    return (
        type(raw_manifest) is DownloadManifest
        and type(max_line_bytes) is int
        and max_line_bytes > 0
    )


def _revalidate_raw_manifest(raw_manifest: DownloadManifest) -> _ValidatedRawManifest:
    validated: _ValidatedRawManifest | None = None
    validation_failed = False
    try:
        validated = conversion_manifest._validate_raw_manifest(raw_manifest)
    except ResearchManifestValidationError:
        validation_failed = True
    if validation_failed or validated is None:
        _raise_plan_error("validate_manifest")
    return validated


def _derive_file_plan_map(
    raw_manifest: DownloadManifest,
) -> dict[str, ResearchFilePlan]:
    file_plans: dict[str, ResearchFilePlan] = {}
    for symbol in raw_manifest.symbols:
        for interval in raw_manifest.intervals:
            raw_name = conversion_manifest._raw_output_name(symbol, interval)
            file_plan: ResearchFilePlan | None = None
            plan_failed = False
            try:
                file_plan = ResearchFilePlan.from_raw_identity(
                    raw_name=raw_name,
                    symbol=symbol,
                    interval=interval,
                )
            except ResearchManifestValidationError:
                plan_failed = True
            if plan_failed or file_plan is None or raw_name in file_plans:
                _raise_plan_error("derive_work")
            file_plans[raw_name] = file_plan
    return file_plans


def _derive_work_items(
    raw_manifest: DownloadManifest,
    *,
    max_line_bytes: int,
    requested_start: datetime,
    requested_end: datetime,
    raw_files: tuple[OutputFileInfo, ...],
) -> tuple[ResearchDatasetWorkItem, ...]:
    file_plans = _derive_file_plan_map(raw_manifest)
    work_items: list[ResearchDatasetWorkItem] = []
    for file_info in raw_files:
        file_plan = file_plans.get(file_info.name)
        if file_plan is None:
            _raise_plan_error("derive_work")

        context: RawConversionContext | None = None
        context_failed = False
        try:
            context = RawConversionContext(
                file_name=file_plan.raw_name,
                symbol=file_plan.symbol,
                interval=file_plan.interval,
                range_start=requested_start,
                range_end=requested_end,
                max_line_bytes=max_line_bytes,
            )
        except ValueError:
            context_failed = True
        if context_failed or context is None:
            _raise_plan_error("derive_work")

        work_items.append(
            ResearchDatasetWorkItem(
                file_plan=file_plan,
                context=context,
                expected_lines=file_info.records,
            )
        )
    return tuple(work_items)


@dataclass(frozen=True, kw_only=True, slots=True)
class ResearchDatasetConversionPlan:
    """Immutable ordered work derived from one deeply revalidated raw manifest."""

    raw_manifest: DownloadManifest
    max_line_bytes: int
    files: tuple[ResearchDatasetWorkItem, ...] = field(init=False)

    def __post_init__(self) -> None:
        if not _valid_plan_inputs(self.raw_manifest, self.max_line_bytes):
            _raise_plan_error("validate_input")
        requested_start, requested_end, raw_files = _revalidate_raw_manifest(self.raw_manifest)
        derived_files = _derive_work_items(
            self.raw_manifest,
            max_line_bytes=self.max_line_bytes,
            requested_start=requested_start,
            requested_end=requested_end,
            raw_files=raw_files,
        )
        object.__setattr__(self, "files", derived_files)


def build_research_dataset_conversion_plan(
    raw_manifest: DownloadManifest,
    *,
    max_line_bytes: int,
) -> ResearchDatasetConversionPlan:
    """Validate exact inputs and derive one deterministic immutable work plan."""
    if not _valid_plan_inputs(raw_manifest, max_line_bytes):
        _raise_plan_error("validate_input")
    return ResearchDatasetConversionPlan(
        raw_manifest=raw_manifest,
        max_line_bytes=max_line_bytes,
    )
