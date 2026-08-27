"""Package-internal preparation of exclusive research staging streams.

This module performs only the mutation boundary required to create a safe
directory tree and two canonical, zero-byte staging files.  It does not read
raw or manifest contents, write payload bytes, flush, fsync, promote, replace,
or clean up filesystem entries.

All filesystem observations are point-in-time.  The JIT checks and immediate
descriptor/path binding checks reduce observable TOCTOU races, but path-based
``mkdir`` and ``open(..., "xb")`` cannot eliminate ancestor-swap races.  Later
publication slices must revalidate again before writing and promotion.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Literal, NoReturn

import packages.market_data.datasets._publication_fs as _fs
import packages.market_data.datasets.publication_preflight as _preflight
from packages.market_data.datasets.conversion_manifest import ResearchFilePlan
from packages.market_data.datasets.publication_layout import (
    PublicationLayoutValidationError,
    ResearchArtifactPaths,
    ResearchPublicationLayout,
)

StagingOperation = Literal[
    "validate_input",
    "preflight",
    "create_directory",
    "create_staging",
    "verify_staging",
    "close",
]

StagingCategory = Literal[
    "invalid_contract",
    "unsafe_filesystem",
    "entry_exists",
    "io_failure",
    "concurrent_change",
]

_INSPECTION_FAILURE_REASONS: frozenset[str] = frozenset(
    {
        "filesystem entry could not be inspected",
        "filesystem containment could not be verified",
    }
)

_ERROR_MESSAGES: dict[tuple[StagingOperation, StagingCategory], str] = {
    ("validate_input", "invalid_contract"): "staging input contract is invalid",
    ("preflight", "unsafe_filesystem"): "publication filesystem is unsafe",
    ("preflight", "entry_exists"): "publication destination already exists",
    ("preflight", "io_failure"): "publication filesystem inspection failed",
    ("preflight", "concurrent_change"): "publication filesystem changed concurrently",
    ("create_directory", "unsafe_filesystem"): "directory destination is unsafe",
    ("create_directory", "entry_exists"): "directory destination already exists",
    ("create_directory", "io_failure"): "directory creation failed",
    ("create_directory", "concurrent_change"): "directory path changed concurrently",
    ("create_staging", "unsafe_filesystem"): "staging destination is unsafe",
    ("create_staging", "entry_exists"): "staging destination already exists",
    ("create_staging", "io_failure"): "exclusive staging creation failed",
    ("create_staging", "concurrent_change"): "staging path changed concurrently",
    ("verify_staging", "unsafe_filesystem"): "staging entry is unsafe",
    ("verify_staging", "entry_exists"): "publication destination already exists",
    ("verify_staging", "io_failure"): "staging entry verification failed",
    ("verify_staging", "concurrent_change"): "staging entry changed concurrently",
    ("close", "invalid_contract"): "owned staging pair contract is invalid",
    ("close", "io_failure"): "owned staging stream close failed",
}


class StagingPreparationError(RuntimeError):
    """Sanitized package-internal staging preparation failure."""

    __slots__ = ("category", "operation")
    operation: StagingOperation
    category: StagingCategory

    def __init__(
        self,
        *,
        operation: StagingOperation,
        category: StagingCategory,
    ) -> None:
        self.operation = operation
        self.category = category
        message = _ERROR_MESSAGES.get((operation, category), "staging preparation failed")
        super().__init__(message)


def _error(
    operation: StagingOperation,
    category: StagingCategory,
) -> StagingPreparationError:
    return StagingPreparationError(operation=operation, category=category)


@dataclass(slots=True, kw_only=True)
class OwnedStagingPair:
    """Two open zero-byte staging streams owned by one preparation call."""

    paths: ResearchArtifactPaths
    research_stream: BinaryIO
    failure_stream: BinaryIO
    research_identity: tuple[int, int] | None
    failure_identity: tuple[int, int] | None
    closed: bool = False


@dataclass(frozen=True, slots=True)
class _CoreInspection:
    raw_root: _fs._InspectedEntry
    raw_manifest: _fs._InspectedEntry
    raw_artifact: _fs._InspectedEntry
    output_root: Path
    research_dir: Path
    failure_dir: Path
    staging_dir: Path
    staging_research_dir: Path
    staging_failure_dir: Path


@dataclass(frozen=True, slots=True)
class _OwnedStageGuard:
    stream: BinaryIO
    path: Path
    stage_parent_path: Path
    stage_parent_expected: Path
    final_parent_path: Path
    final_parent_expected: Path


def _inspection_category(
    error: _fs.PhysicalInspectionError | _preflight.PublicationPreflightError,
    *,
    changed: bool,
) -> StagingCategory:
    reason = error.reason
    if isinstance(error, _fs.PhysicalResolutionError):
        return "io_failure"
    if reason == "filesystem entry could not be inspected":
        return "io_failure"
    if changed:
        return "concurrent_change"
    if reason in _INSPECTION_FAILURE_REASONS:
        return "io_failure"
    return "unsafe_filesystem"


def _require_inputs(
    layout: ResearchPublicationLayout,
    plan: ResearchFilePlan,
) -> tuple[ResearchPublicationLayout, ResearchFilePlan]:
    if type(layout) is not ResearchPublicationLayout:
        raise _error("validate_input", "invalid_contract")
    if type(plan) is not ResearchFilePlan:
        raise _error("validate_input", "invalid_contract")
    return layout, plan


def _derive_paths(
    layout: ResearchPublicationLayout,
    plan: ResearchFilePlan,
) -> ResearchArtifactPaths:
    paths: ResearchArtifactPaths | None = None
    derivation_failed = False
    try:
        paths = layout.artifact_paths_for(plan)
    except PublicationLayoutValidationError:
        derivation_failed = True

    if derivation_failed or paths is None:
        raise _error("validate_input", "invalid_contract")
    return paths


def _run_snapshot_preflight(layout: ResearchPublicationLayout) -> None:
    preflight_failure: StagingCategory | None = None
    try:
        _preflight.preflight_research_publication(layout, ())
    except _preflight.PublicationPreflightError as error:
        preflight_failure = _inspection_category(error, changed=False)

    if preflight_failure is not None:
        raise _error("preflight", preflight_failure)


def _inspect_core_physical(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
) -> _CoreInspection:
    _fs._require_local_root(layout.raw_dir, field="raw_dir")
    _fs._require_local_root(layout.output_dir, field="output_dir")

    raw_root = _fs._inspect_required_entry(
        layout.raw_dir,
        field="raw_dir",
        kind="directory",
    )
    raw_manifest = _fs._inspect_required_entry(
        layout.raw_manifest_path,
        field="raw_manifest",
        kind="file",
    )
    _fs._require_direct_parent(
        raw_manifest.resolved,
        expected_parent=raw_root.resolved,
    )

    output_root = _fs._inspect_output_root(layout.output_dir)
    _fs._reject_related_physical_roots(raw_root.resolved, output_root.projected)

    raw_artifact = _fs._inspect_required_entry(
        paths.raw_path,
        field="raw_artifact",
        kind="file",
    )
    _fs._require_direct_parent(
        raw_artifact.resolved,
        expected_parent=raw_root.resolved,
    )
    research_dir = _fs._inspect_output_child(
        layout.research_dir,
        expected_parent=output_root.projected,
        kind="directory",
    )
    failure_dir = _fs._inspect_output_child(
        layout.failure_dir,
        expected_parent=output_root.projected,
        kind="directory",
    )
    staging_dir = _fs._inspect_output_child(
        layout.staging_dir,
        expected_parent=output_root.projected,
        kind="directory",
    )
    staging_research_dir = _fs._inspect_output_child(
        layout.staging_research_dir,
        expected_parent=staging_dir.projected,
        kind="directory",
    )
    staging_failure_dir = _fs._inspect_output_child(
        layout.staging_failure_dir,
        expected_parent=staging_dir.projected,
        kind="directory",
    )
    return _CoreInspection(
        raw_root=raw_root,
        raw_manifest=raw_manifest,
        raw_artifact=raw_artifact,
        output_root=output_root.projected,
        research_dir=research_dir.projected,
        failure_dir=failure_dir.projected,
        staging_dir=staging_dir.projected,
        staging_research_dir=staging_research_dir.projected,
        staging_failure_dir=staging_failure_dir.projected,
    )


def _same_core_paths(first: _CoreInspection, second: _CoreInspection) -> bool:
    return (
        first.raw_root.resolved == second.raw_root.resolved
        and first.raw_manifest.resolved == second.raw_manifest.resolved
        and first.raw_artifact.resolved == second.raw_artifact.resolved
        and first.output_root == second.output_root
        and first.research_dir == second.research_dir
        and first.failure_dir == second.failure_dir
        and first.staging_dir == second.staging_dir
        and first.staging_research_dir == second.staging_research_dir
        and first.staging_failure_dir == second.staging_failure_dir
    )


def _inspect_core(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    *,
    operation: StagingOperation,
    baseline: _CoreInspection | None,
) -> _CoreInspection:
    inspected: _CoreInspection | None = None
    inspection_failure: StagingCategory | None = None
    try:
        inspected = _inspect_core_physical(layout, paths)
    except _fs.PhysicalInspectionError as error:
        inspection_failure = _inspection_category(error, changed=baseline is not None)

    if inspection_failure is not None or inspected is None:
        category = inspection_failure or "unsafe_filesystem"
        raise _error(operation, category)
    manifest_identity = _fs._physical_file_identity(inspected.raw_manifest.metadata)
    raw_identity = _fs._physical_file_identity(inspected.raw_artifact.metadata)
    if inspected.raw_artifact.resolved == inspected.raw_manifest.resolved or (
        manifest_identity is not None and manifest_identity == raw_identity
    ):
        alias_category: StagingCategory = (
            "concurrent_change" if baseline is not None else "unsafe_filesystem"
        )
        raise _error(operation, alias_category)
    if baseline is not None and not _same_core_paths(baseline, inspected):
        raise _error(operation, "concurrent_change")
    return inspected


def _target_parent_pairs(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    inspected: _CoreInspection,
) -> tuple[tuple[Path, Path], ...]:
    return (
        (layout.research_manifest_path, inspected.output_root),
        (layout.staging_manifest_path, inspected.staging_dir),
        (paths.research_path, inspected.research_dir),
        (paths.failure_path, inspected.failure_dir),
        (paths.staging_research_path, inspected.staging_research_dir),
        (paths.staging_failure_path, inspected.staging_failure_dir),
    )


def _require_target_absent(
    path: Path,
    *,
    expected_parent: Path,
    operation: StagingOperation,
    changed: bool,
) -> None:
    inspection_failure: StagingCategory | None = None
    target_exists = False
    try:
        presence = _fs._inspect_entry_presence(
            path,
            expected_parent=expected_parent,
            field="target",
        )
        target_exists = presence.state == "existing"
    except _fs.PhysicalInspectionError as error:
        inspection_failure = _inspection_category(error, changed=changed)

    if inspection_failure is not None:
        raise _error(operation, inspection_failure)
    if target_exists:
        raise _error(operation, "entry_exists")


def _require_initial_targets_absent(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    inspected: _CoreInspection,
) -> None:
    _require_all_targets_absent(
        layout,
        paths,
        inspected,
        operation="preflight",
        changed=False,
    )


def _require_all_targets_absent(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    inspected: _CoreInspection,
    *,
    operation: StagingOperation,
    changed: bool,
) -> None:
    for target, expected_parent in _target_parent_pairs(layout, paths, inspected):
        _require_target_absent(
            target,
            expected_parent=expected_parent,
            operation=operation,
            changed=changed,
        )


def _inspect_directory_plan(
    path: Path,
    *,
    changed: bool,
) -> _fs._DirectoryCreationPlan:
    plan: _fs._DirectoryCreationPlan | None = None
    inspection_failure: StagingCategory | None = None
    try:
        plan = _fs._inspect_directory_creation_plan(path, field="output_dir")
    except _fs.PhysicalInspectionError as error:
        inspection_failure = _inspection_category(error, changed=changed)

    if inspection_failure is not None or plan is None:
        category = inspection_failure or "unsafe_filesystem"
        raise _error("create_directory", category)
    return plan


def _inspect_created_directory(
    path: Path,
    *,
    expected: Path,
) -> _fs._InspectedEntry:
    inspected: _fs._InspectedEntry | None = None
    inspection_failure: StagingCategory | None = None
    try:
        inspected = _fs._inspect_required_entry(
            path,
            field="output_dir",
            kind="directory",
        )
    except _fs.PhysicalInspectionError as error:
        inspection_failure = _inspection_category(error, changed=True)

    if inspection_failure is not None or inspected is None:
        category = inspection_failure or "concurrent_change"
        raise _error("create_directory", category)
    if inspected.resolved != expected:
        raise _error("create_directory", "concurrent_change")
    return inspected


def _mkdir_component(path: Path) -> None:
    entry_exists = False
    io_failure = False
    try:
        path.mkdir(parents=False, exist_ok=False)
    except FileExistsError:
        entry_exists = True
    except OSError:
        io_failure = True

    if entry_exists:
        raise _error("create_directory", "entry_exists")
    if io_failure:
        raise _error("create_directory", "io_failure")


def _ensure_directory(
    path: Path,
    *,
    expected_parent: Path | None,
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    baseline: _CoreInspection,
) -> _fs._InspectedEntry:
    initial_plan = _inspect_directory_plan(path, changed=True)
    if not initial_plan.missing_components:
        if expected_parent is not None and initial_plan.projected.parent != expected_parent:
            raise _error("create_directory", "concurrent_change")
        return initial_plan.existing_ancestor

    for component in initial_plan.missing_components:
        _inspect_core(
            layout,
            paths,
            operation="create_directory",
            baseline=baseline,
        )
        current_plan = _inspect_directory_plan(component, changed=True)
        if current_plan.missing_components != (component,):
            raise _error("create_directory", "concurrent_change")
        if current_plan.projected.parent != current_plan.existing_ancestor.resolved:
            raise _error("create_directory", "concurrent_change")
        if not (
            baseline.output_root.is_relative_to(current_plan.projected)
            or current_plan.projected.is_relative_to(baseline.output_root)
        ):
            raise _error("create_directory", "concurrent_change")
        _mkdir_component(component)
        _inspect_created_directory(component, expected=current_plan.projected)

    completed = _inspect_created_directory(path, expected=initial_plan.projected)
    if expected_parent is not None and completed.resolved.parent != expected_parent:
        raise _error("create_directory", "concurrent_change")
    return completed


def _inspect_required_directory(
    path: Path,
    *,
    expected: Path,
    operation: StagingOperation,
) -> _fs._InspectedEntry:
    inspected: _fs._InspectedEntry | None = None
    inspection_failure: StagingCategory | None = None
    try:
        inspected = _fs._inspect_required_entry(
            path,
            field="output_dir",
            kind="directory",
        )
    except _fs.PhysicalInspectionError as error:
        inspection_failure = _inspection_category(error, changed=True)

    if inspection_failure is not None or inspected is None:
        category = inspection_failure or "concurrent_change"
        raise _error(operation, category)
    if inspected.resolved != expected:
        raise _error(operation, "concurrent_change")
    return inspected


def _require_same_device(
    staging_parent: _fs._InspectedEntry,
    final_parent: _fs._InspectedEntry,
) -> None:
    staging_device = _fs._physical_device(staging_parent.metadata)
    final_device = _fs._physical_device(final_parent.metadata)
    if staging_device is not None and final_device is not None and staging_device != final_device:
        raise _error("create_staging", "unsafe_filesystem")


def _open_exclusive(path: Path) -> BinaryIO:
    return open(path, "xb")


def _stream_descriptor(stream: BinaryIO) -> int:
    descriptor: int | None = None
    contract_failed = False
    io_failure = False
    try:
        mode = getattr(stream, "mode", None)
        if type(mode) is not str or "b" not in mode or "x" not in mode:
            contract_failed = True
        elif stream.closed or not stream.writable() or stream.tell() != 0:
            contract_failed = True
        else:
            candidate = stream.fileno()
            if type(candidate) is int:
                descriptor = candidate
            else:
                contract_failed = True
    except OSError:
        io_failure = True
    except (AttributeError, TypeError, ValueError):
        contract_failed = True

    if io_failure:
        raise _error("verify_staging", "io_failure")
    if contract_failed or descriptor is None:
        raise _error("verify_staging", "concurrent_change")
    return descriptor


def _descriptor_metadata(descriptor: int) -> os.stat_result:
    metadata: os.stat_result | None = None
    io_failure = False
    try:
        metadata = os.fstat(descriptor)
    except OSError:
        io_failure = True

    if io_failure or metadata is None:
        raise _error("verify_staging", "io_failure")
    return metadata


def _require_non_inheritable(descriptor: int) -> None:
    get_inheritable = getattr(os, "get_inheritable", None)
    if get_inheritable is None:
        return

    inheritable: bool | None = None
    io_failure = False
    try:
        inheritable = get_inheritable(descriptor)
    except OSError:
        io_failure = True

    if io_failure:
        raise _error("verify_staging", "io_failure")
    if type(inheritable) is not bool or inheritable:
        raise _error("verify_staging", "concurrent_change")


def _verify_opened_staging(
    stream: BinaryIO,
    *,
    path: Path,
    stage_parent_path: Path,
    stage_parent_expected: Path,
    final_parent_path: Path,
    final_parent_expected: Path,
) -> tuple[int, int] | None:
    descriptor = _stream_descriptor(stream)
    descriptor_metadata = _descriptor_metadata(descriptor)
    _require_non_inheritable(descriptor)

    inspected_path: _fs._InspectedEntry | None = None
    inspection_failure: StagingCategory | None = None
    try:
        _fs._require_regular_zero_byte_file(
            descriptor_metadata,
            field="staging",
        )
        inspected_path = _fs._inspect_required_entry(
            path,
            field="staging",
            kind="file",
        )
        _fs._require_regular_zero_byte_file(
            inspected_path.metadata,
            field="staging",
        )
        _fs._require_direct_parent(
            inspected_path.resolved,
            expected_parent=stage_parent_expected,
        )
        identity = _fs._require_matching_physical_identity(
            descriptor_metadata,
            inspected_path.metadata,
            field="staging",
        )
    except _fs.PhysicalInspectionError as error:
        inspection_failure = _inspection_category(error, changed=True)

    if inspection_failure is not None or inspected_path is None:
        category = inspection_failure or "concurrent_change"
        raise _error("verify_staging", category)
    staging_parent = _inspect_required_directory(
        stage_parent_path,
        expected=stage_parent_expected,
        operation="verify_staging",
    )
    final_parent = _inspect_required_directory(
        final_parent_path,
        expected=final_parent_expected,
        operation="verify_staging",
    )
    usable_devices = tuple(
        device
        for device in (
            _fs._physical_device(descriptor_metadata),
            _fs._physical_device(inspected_path.metadata),
            _fs._physical_device(staging_parent.metadata),
            _fs._physical_device(final_parent.metadata),
        )
        if device is not None
    )
    if len(set(usable_devices)) > 1:
        raise _error("verify_staging", "concurrent_change")
    return identity


def _revalidate_owned_staging_pair(
    layout: ResearchPublicationLayout,
    plan: ResearchFilePlan,
    pair: OwnedStagingPair,
) -> _CoreInspection:
    """JIT-revalidate an owned pair without changing C3B-2B state."""
    if (
        type(layout) is not ResearchPublicationLayout
        or type(plan) is not ResearchFilePlan
        or type(pair) is not OwnedStagingPair
        or pair.closed
    ):
        raise _error("validate_input", "invalid_contract")

    paths = _derive_paths(layout, plan)
    if pair.paths != paths:
        raise _error("validate_input", "invalid_contract")

    inspected = _inspect_core(
        layout,
        paths,
        operation="verify_staging",
        baseline=None,
    )
    research_identity = _verify_opened_staging(
        pair.research_stream,
        path=paths.staging_research_path,
        stage_parent_path=layout.staging_research_dir,
        stage_parent_expected=inspected.staging_research_dir,
        final_parent_path=layout.research_dir,
        final_parent_expected=inspected.research_dir,
    )
    failure_identity = _verify_opened_staging(
        pair.failure_stream,
        path=paths.staging_failure_path,
        stage_parent_path=layout.staging_failure_dir,
        stage_parent_expected=inspected.staging_failure_dir,
        final_parent_path=layout.failure_dir,
        final_parent_expected=inspected.failure_dir,
    )
    if (
        pair.research_identity is not None
        and research_identity is not None
        and pair.research_identity != research_identity
    ) or (
        pair.failure_identity is not None
        and failure_identity is not None
        and pair.failure_identity != failure_identity
    ):
        raise _error("verify_staging", "concurrent_change")

    for target, parent in (
        (layout.research_manifest_path, inspected.output_root),
        (layout.staging_manifest_path, inspected.staging_dir),
        (paths.research_path, inspected.research_dir),
        (paths.failure_path, inspected.failure_dir),
    ):
        _require_target_absent(
            target,
            expected_parent=parent,
            operation="verify_staging",
            changed=True,
        )
    return inspected


def _attempt_close_streams(streams: tuple[BinaryIO, ...]) -> bool:
    close_failed = False
    for stream in streams:
        try:
            stream.close()
        except (OSError, RuntimeError, ValueError):
            close_failed = True
    return close_failed


def _stream_is_closed(stream: BinaryIO) -> bool:
    closed: bool | None = None
    inspection_failed = False
    try:
        closed = stream.closed
    except (AttributeError, OSError, RuntimeError, ValueError):
        inspection_failed = True
    return not inspection_failed and type(closed) is bool and closed


def _close_after_failure(
    streams: tuple[BinaryIO, ...],
    *,
    original: tuple[StagingOperation, StagingCategory],
) -> NoReturn:
    close_failed = _attempt_close_streams(streams)
    any_open = any(not _stream_is_closed(stream) for stream in streams)
    if close_failed or any_open:
        raise _error("close", "io_failure")
    raise _error(*original)


def _recheck_stage_absence(
    layout: ResearchPublicationLayout,
    *,
    stage_path: Path,
    stage_parent: Path,
    final_path: Path,
    final_parent: Path,
    inspected: _CoreInspection,
    operation: StagingOperation,
    additional_targets: tuple[tuple[Path, Path], ...],
) -> None:
    for target, parent in (
        (layout.research_manifest_path, inspected.output_root),
        (layout.staging_manifest_path, inspected.staging_dir),
        (final_path, final_parent),
        (stage_path, stage_parent),
        *additional_targets,
    ):
        _require_target_absent(
            target,
            expected_parent=parent,
            operation=operation,
            changed=True,
        )


def _recheck_after_stage_creation(
    layout: ResearchPublicationLayout,
    *,
    final_path: Path,
    final_parent: Path,
    inspected: _CoreInspection,
) -> None:
    for target, parent in (
        (layout.research_manifest_path, inspected.output_root),
        (layout.staging_manifest_path, inspected.staging_dir),
        (final_path, final_parent),
    ):
        _require_target_absent(
            target,
            expected_parent=parent,
            operation="verify_staging",
            changed=True,
        )


def _create_one_staging(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    baseline: _CoreInspection,
    *,
    stage_path: Path,
    stage_parent_path: Path,
    stage_parent_expected: Path,
    final_path: Path,
    final_parent_path: Path,
    final_parent_expected: Path,
    additional_absence: tuple[tuple[Path, Path], ...],
    owned_guard: _OwnedStageGuard | None,
    ownership: list[BinaryIO],
) -> tuple[BinaryIO, tuple[int, int] | None]:
    inspected = _inspect_core(
        layout,
        paths,
        operation="create_staging",
        baseline=baseline,
    )
    staging_parent = _inspect_required_directory(
        stage_parent_path,
        expected=stage_parent_expected,
        operation="create_staging",
    )
    final_parent = _inspect_required_directory(
        final_parent_path,
        expected=final_parent_expected,
        operation="create_staging",
    )
    _require_same_device(staging_parent, final_parent)
    if owned_guard is not None:
        _verify_opened_staging(
            owned_guard.stream,
            path=owned_guard.path,
            stage_parent_path=owned_guard.stage_parent_path,
            stage_parent_expected=owned_guard.stage_parent_expected,
            final_parent_path=owned_guard.final_parent_path,
            final_parent_expected=owned_guard.final_parent_expected,
        )
    _recheck_stage_absence(
        layout,
        stage_path=stage_path,
        stage_parent=stage_parent_expected,
        final_path=final_path,
        final_parent=final_parent_expected,
        inspected=inspected,
        operation="create_staging",
        additional_targets=additional_absence,
    )

    stream: BinaryIO | None = None
    transferred = False
    try:
        create_failure: StagingCategory | None = None
        try:
            stream = _open_exclusive(stage_path)
        except FileExistsError:
            create_failure = "entry_exists"
        except OSError:
            create_failure = "io_failure"

        if create_failure is not None or stream is None:
            category = create_failure or "io_failure"
            raise _error("create_staging", category)
        ownership.append(stream)

        verification_failure: tuple[StagingOperation, StagingCategory] | None = None
        identity: tuple[int, int] | None = None
        try:
            identity = _verify_opened_staging(
                stream,
                path=stage_path,
                stage_parent_path=stage_parent_path,
                stage_parent_expected=stage_parent_expected,
                final_parent_path=final_parent_path,
                final_parent_expected=final_parent_expected,
            )
            _recheck_after_stage_creation(
                layout,
                final_path=final_path,
                final_parent=final_parent_expected,
                inspected=inspected,
            )
        except StagingPreparationError as error:
            verification_failure = error.operation, error.category

        if verification_failure is not None:
            _close_after_failure((stream,), original=verification_failure)
        transferred = True
        return stream, identity
    finally:
        if stream is not None and not transferred:
            _attempt_close_streams((stream,))


def _create_directory_tree(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    baseline: _CoreInspection,
) -> _CoreInspection:
    output_root = _ensure_directory(
        layout.output_dir,
        expected_parent=None,
        layout=layout,
        paths=paths,
        baseline=baseline,
    )
    _ensure_directory(
        layout.research_dir,
        expected_parent=output_root.resolved,
        layout=layout,
        paths=paths,
        baseline=baseline,
    )
    _ensure_directory(
        layout.failure_dir,
        expected_parent=output_root.resolved,
        layout=layout,
        paths=paths,
        baseline=baseline,
    )
    staging_dir = _ensure_directory(
        layout.staging_dir,
        expected_parent=output_root.resolved,
        layout=layout,
        paths=paths,
        baseline=baseline,
    )
    _ensure_directory(
        layout.staging_research_dir,
        expected_parent=staging_dir.resolved,
        layout=layout,
        paths=paths,
        baseline=baseline,
    )
    _ensure_directory(
        layout.staging_failure_dir,
        expected_parent=staging_dir.resolved,
        layout=layout,
        paths=paths,
        baseline=baseline,
    )
    return _inspect_core(
        layout,
        paths,
        operation="create_directory",
        baseline=baseline,
    )


def prepare_exclusive_staging_pair(
    layout: ResearchPublicationLayout,
    *,
    plan: ResearchFilePlan,
) -> OwnedStagingPair:
    """Create and return two exclusively owned canonical staging streams."""
    exact_layout, exact_plan = _require_inputs(layout, plan)
    paths = _derive_paths(exact_layout, exact_plan)
    _run_snapshot_preflight(exact_layout)
    baseline = _inspect_core(
        exact_layout,
        paths,
        operation="preflight",
        baseline=None,
    )
    _require_initial_targets_absent(exact_layout, paths, baseline)
    current = _create_directory_tree(exact_layout, paths, baseline)
    _require_all_targets_absent(
        exact_layout,
        paths,
        current,
        operation="create_staging",
        changed=True,
    )

    research_stream: BinaryIO | None = None
    failure_stream: BinaryIO | None = None
    research_identity: tuple[int, int] | None = None
    failure_identity: tuple[int, int] | None = None
    preparation_failure: tuple[StagingOperation, StagingCategory] | None = None
    owned_streams: list[BinaryIO] = []
    ownership_transferred = False
    try:
        try:
            research_stream, research_identity = _create_one_staging(
                exact_layout,
                paths,
                current,
                stage_path=paths.staging_research_path,
                stage_parent_path=exact_layout.staging_research_dir,
                stage_parent_expected=current.staging_research_dir,
                final_path=paths.research_path,
                final_parent_path=exact_layout.research_dir,
                final_parent_expected=current.research_dir,
                additional_absence=(
                    (paths.failure_path, current.failure_dir),
                    (paths.staging_failure_path, current.staging_failure_dir),
                ),
                owned_guard=None,
                ownership=owned_streams,
            )
            failure_stream, failure_identity = _create_one_staging(
                exact_layout,
                paths,
                current,
                stage_path=paths.staging_failure_path,
                stage_parent_path=exact_layout.staging_failure_dir,
                stage_parent_expected=current.staging_failure_dir,
                final_path=paths.failure_path,
                final_parent_path=exact_layout.failure_dir,
                final_parent_expected=current.failure_dir,
                additional_absence=((paths.research_path, current.research_dir),),
                owned_guard=_OwnedStageGuard(
                    stream=research_stream,
                    path=paths.staging_research_path,
                    stage_parent_path=exact_layout.staging_research_dir,
                    stage_parent_expected=current.staging_research_dir,
                    final_parent_path=exact_layout.research_dir,
                    final_parent_expected=current.research_dir,
                ),
                ownership=owned_streams,
            )
            _verify_opened_staging(
                research_stream,
                path=paths.staging_research_path,
                stage_parent_path=exact_layout.staging_research_dir,
                stage_parent_expected=current.staging_research_dir,
                final_parent_path=exact_layout.research_dir,
                final_parent_expected=current.research_dir,
            )
            _verify_opened_staging(
                failure_stream,
                path=paths.staging_failure_path,
                stage_parent_path=exact_layout.staging_failure_dir,
                stage_parent_expected=current.staging_failure_dir,
                final_parent_path=exact_layout.failure_dir,
                final_parent_expected=current.failure_dir,
            )
            final_core = _inspect_core(
                exact_layout,
                paths,
                operation="verify_staging",
                baseline=current,
            )
            for target, parent in (
                (exact_layout.research_manifest_path, final_core.output_root),
                (exact_layout.staging_manifest_path, final_core.staging_dir),
                (paths.research_path, final_core.research_dir),
                (paths.failure_path, final_core.failure_dir),
            ):
                _require_target_absent(
                    target,
                    expected_parent=parent,
                    operation="verify_staging",
                    changed=True,
                )
        except StagingPreparationError as error:
            preparation_failure = error.operation, error.category

        if preparation_failure is not None:
            _close_after_failure(tuple(owned_streams), original=preparation_failure)
        if research_stream is None or failure_stream is None:
            _close_after_failure(
                tuple(owned_streams),
                original=("create_staging", "io_failure"),
            )
        pair = OwnedStagingPair(
            paths=paths,
            research_stream=research_stream,
            failure_stream=failure_stream,
            research_identity=research_identity,
            failure_identity=failure_identity,
        )
        ownership_transferred = True
        return pair
    finally:
        if not ownership_transferred:
            _attempt_close_streams(tuple(owned_streams))


def close_owned_staging_pair(pair: OwnedStagingPair) -> None:
    """Close both streams without explicit flush, sync, or entry removal."""
    if type(pair) is not OwnedStagingPair:
        raise _error("validate_input", "invalid_contract")
    if (
        pair.closed is True
        and _stream_is_closed(pair.research_stream)
        and _stream_is_closed(pair.failure_stream)
    ):
        return

    close_failed = _attempt_close_streams((pair.research_stream, pair.failure_stream))
    both_closed = _stream_is_closed(pair.research_stream) and _stream_is_closed(pair.failure_stream)
    pair.closed = both_closed
    if close_failed or not both_closed:
        raise _error("close", "io_failure")
