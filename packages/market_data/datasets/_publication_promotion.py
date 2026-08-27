"""Windows-local no-clobber promotion of one durable staging entry.

This package-private micro-slice performs metadata-only inspection followed by
one same-volume ``os.rename``.  It never reads or hashes staged content, writes
manifests, orchestrates a pair, or rolls back a successful rename.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Literal, NoReturn

import packages.market_data.datasets._publication_fs as _fs
import packages.market_data.datasets._publication_staging as _staging
import packages.market_data.datasets.publication_preflight as _preflight
from packages.market_data.datasets.conversion_manifest import (
    ResearchFileArtifact,
    ResearchFilePlan,
    ResearchManifestValidationError,
)
from packages.market_data.datasets.publication_layout import (
    PublicationLayoutValidationError,
    ResearchArtifactPaths,
    ResearchPublicationLayout,
)

PromotionEntryKind = Literal["failure", "research"]
PromotionOperation = Literal[
    "validate_input",
    "preflight",
    "inspect_source",
    "inspect_destination",
    "promote",
    "verify_destination",
]
PromotionCategory = Literal[
    "invalid_contract",
    "unsupported_platform",
    "unsafe_filesystem",
    "entry_exists",
    "io_failure",
    "verification_mismatch",
    "concurrent_change",
]

_Failure = tuple[PromotionOperation, PromotionCategory]
_ERROR_MESSAGES: MappingProxyType[_Failure, str] = MappingProxyType(
    {
        ("validate_input", "invalid_contract"): "promotion input contract is invalid",
        ("validate_input", "unsupported_platform"): "no-clobber promotion requires Windows",
        (
            "preflight",
            "unsafe_filesystem",
        ): "publication preflight rejected the physical layout",
        ("inspect_source", "unsafe_filesystem"): "staging source is not safe",
        ("inspect_source", "io_failure"): "staging source could not be inspected",
        (
            "inspect_source",
            "verification_mismatch",
        ): "staging source does not match the artifact contract",
        (
            "inspect_source",
            "concurrent_change",
        ): "staging source changed during promotion",
        ("inspect_destination", "unsafe_filesystem"): "promotion destination is not safe",
        (
            "inspect_destination",
            "entry_exists",
        ): "promotion destination already exists",
        (
            "inspect_destination",
            "io_failure",
        ): "promotion destination could not be inspected",
        (
            "inspect_destination",
            "concurrent_change",
        ): "promotion destination changed during promotion",
        ("promote", "entry_exists"): "promotion destination appeared concurrently",
        ("promote", "io_failure"): "staging entry could not be promoted",
        ("verify_destination", "unsafe_filesystem"): "promoted entry is not safe",
        (
            "verify_destination",
            "io_failure",
        ): "promoted entry could not be inspected",
        (
            "verify_destination",
            "verification_mismatch",
        ): "promoted entry does not match the artifact contract",
        (
            "verify_destination",
            "concurrent_change",
        ): "promoted entry changed during verification",
    },
)

_INVALID_ERROR_CONTRACT = "invalid promotion entry error contract"
_INVALID_STATE_CONTRACT = "invalid promoted entry state contract"
_INSPECTION_IO_REASONS: frozenset[str] = frozenset(
    {
        "filesystem entry could not be inspected",
    }
)


class PromotionEntryError(RuntimeError):
    """Sanitized package-private failure from one-entry promotion."""

    __slots__ = ("category", "operation")
    operation: PromotionOperation
    category: PromotionCategory

    def __init__(
        self,
        *,
        operation: PromotionOperation,
        category: PromotionCategory,
    ) -> None:
        if type(operation) is not str or type(category) is not str:
            raise ValueError(_INVALID_ERROR_CONTRACT)
        message = _ERROR_MESSAGES.get((operation, category))
        if message is None:
            raise ValueError(_INVALID_ERROR_CONTRACT)
        self.operation = operation
        self.category = category
        super().__init__(message)


@dataclass(frozen=True, kw_only=True, slots=True)
class PromotedEntryState:
    """Observational verified physical state of one promoted final entry.

    This state is evidence only. It is not a winner, lease, ownership,
    retry, idempotency, or commit token. Do not use it to infer which
    invocation performed the physical rename.
    """

    kind: PromotionEntryKind
    byte_count: int
    physical_identity: tuple[int, int] | None

    def __post_init__(self) -> None:
        identity = self.physical_identity
        invalid_identity = identity is not None and (
            type(identity) is not tuple
            or len(identity) != 2
            or type(identity[0]) is not int
            or type(identity[1]) is not int
            or identity[1] == 0
        )
        if (
            type(self.kind) is not str
            or self.kind not in {"failure", "research"}
            or type(self.byte_count) is not int
            or self.byte_count < 0
            or invalid_identity
        ):
            raise ValueError(_INVALID_STATE_CONTRACT)


@dataclass(frozen=True, slots=True)
class _PhysicalSnapshot:
    resolved: Path
    identity: tuple[int, int] | None
    device: int | None
    size: int | None


@dataclass(frozen=True, slots=True)
class _SelectedEntry:
    kind: PromotionEntryKind
    source_path: Path
    destination_path: Path
    staging_parent_path: Path
    final_parent_path: Path
    expected_bytes: int


@dataclass(frozen=True, slots=True)
class _PromotionSnapshot:
    core: _staging._CoreInspection
    raw_root: _PhysicalSnapshot
    raw_manifest: _PhysicalSnapshot
    raw_artifact: _PhysicalSnapshot
    output_root: _PhysicalSnapshot
    staging_root: _PhysicalSnapshot
    staging_parent: _PhysicalSnapshot
    final_parent: _PhysicalSnapshot
    source: _PhysicalSnapshot
    destination_projected: Path
    research_manifest_projected: Path
    staging_manifest_projected: Path


def _raise_failure(failure: _Failure) -> NoReturn:
    raise PromotionEntryError(operation=failure[0], category=failure[1])


def _supports_windows_no_clobber_rename() -> bool:
    return os.name == "nt"


def _require_inputs(
    layout: object,
    artifact: object,
    kind: object,
) -> tuple[
    ResearchPublicationLayout,
    ResearchFileArtifact,
    PromotionEntryKind,
    ResearchFilePlan,
    ResearchArtifactPaths,
]:
    if (
        type(layout) is not ResearchPublicationLayout
        or type(artifact) is not ResearchFileArtifact
        or type(kind) is not str
        or kind not in {"failure", "research"}
    ):
        _raise_failure(("validate_input", "invalid_contract"))

    exact_layout = layout
    exact_artifact = artifact
    if kind == "failure":
        exact_kind: PromotionEntryKind = "failure"
    else:
        exact_kind = "research"
    plan: ResearchFilePlan | None = None
    paths: ResearchArtifactPaths | None = None
    contract_failed = False
    try:
        plan = ResearchFilePlan(
            raw_name=exact_artifact.raw_name,
            research_name=exact_artifact.research_name,
            failure_name=exact_artifact.failure_name,
            symbol=exact_artifact.symbol,
            interval=exact_artifact.interval,
        )
        paths = exact_layout.artifact_paths_for(plan)
    except (ResearchManifestValidationError, PublicationLayoutValidationError):
        contract_failed = True

    if contract_failed or plan is None or paths is None:
        _raise_failure(("validate_input", "invalid_contract"))
    return exact_layout, exact_artifact, exact_kind, plan, paths


def _select_entry(
    layout: ResearchPublicationLayout,
    artifact: ResearchFileArtifact,
    paths: ResearchArtifactPaths,
    kind: PromotionEntryKind,
) -> _SelectedEntry:
    if kind == "failure":
        return _SelectedEntry(
            kind=kind,
            source_path=paths.staging_failure_path,
            destination_path=paths.failure_path,
            staging_parent_path=layout.staging_failure_dir,
            final_parent_path=layout.failure_dir,
            expected_bytes=artifact.failure_bytes,
        )
    return _SelectedEntry(
        kind=kind,
        source_path=paths.staging_research_path,
        destination_path=paths.research_path,
        staging_parent_path=layout.staging_research_dir,
        final_parent_path=layout.research_dir,
        expected_bytes=artifact.research_bytes,
    )


def _run_preflight(
    layout: ResearchPublicationLayout,
    artifact: ResearchFileArtifact,
) -> None:
    failed = False
    try:
        _preflight.preflight_research_publication(layout, (artifact,))
    except _preflight.PublicationPreflightError:
        failed = True
    if failed:
        _raise_failure(("preflight", "unsafe_filesystem"))


def _inspection_is_io_failure(error: _fs.PhysicalInspectionError) -> bool:
    return isinstance(error, _fs.PhysicalResolutionError) or error.reason in (
        _INSPECTION_IO_REASONS
    )


def _inspection_category(
    error: _fs.PhysicalInspectionError,
    *,
    operation: PromotionOperation,
    changed: bool,
    missing_is_mismatch: bool,
) -> PromotionCategory:
    if _inspection_is_io_failure(error):
        return "io_failure"
    if changed:
        return "concurrent_change"
    if missing_is_mismatch and error.reason == "filesystem entry does not exist":
        return "verification_mismatch"
    if operation in {"inspect_source", "verify_destination"} and (
        error.reason == "filesystem entry has an invalid size"
    ):
        return "verification_mismatch"
    return "unsafe_filesystem"


def _inspect_core(
    layout: ResearchPublicationLayout,
    paths: ResearchArtifactPaths,
    *,
    baseline: _staging._CoreInspection | None,
) -> _staging._CoreInspection:
    inspected: _staging._CoreInspection | None = None
    failure: _Failure | None = None
    try:
        inspected = _staging._inspect_core(
            layout,
            paths,
            operation="verify_staging",
            baseline=baseline,
        )
    except _staging.StagingPreparationError as error:
        if error.category == "io_failure":
            category: PromotionCategory = "io_failure"
        elif baseline is not None or error.category == "concurrent_change":
            category = "concurrent_change"
        else:
            category = "unsafe_filesystem"
        failure = "inspect_source", category

    if failure is not None:
        _raise_failure(failure)
    if inspected is None:
        _raise_failure(("inspect_source", "io_failure"))
    return inspected


def _inspect_required_entry(
    path: Path,
    *,
    field: str,
    kind: _fs._EntryKind,
    operation: PromotionOperation,
    changed: bool,
    missing_is_mismatch: bool,
    expected_parent: Path | None = None,
    expected_resolved: Path | None = None,
) -> _fs._InspectedEntry:
    inspected: _fs._InspectedEntry | None = None
    failure: _Failure | None = None
    try:
        inspected = _fs._inspect_required_entry(path, field=field, kind=kind)
        if expected_parent is not None:
            _fs._require_direct_parent(
                inspected.resolved,
                expected_parent=expected_parent,
            )
    except _fs.PhysicalInspectionError as error:
        failure = (
            operation,
            _inspection_category(
                error,
                operation=operation,
                changed=changed,
                missing_is_mismatch=missing_is_mismatch,
            ),
        )

    if failure is not None:
        _raise_failure(failure)
    if inspected is None:
        _raise_failure((operation, "io_failure"))
    if expected_resolved is not None and inspected.resolved != expected_resolved:
        category: PromotionCategory = "concurrent_change" if changed else "unsafe_filesystem"
        _raise_failure((operation, category))
    return inspected


def _snapshot_entry(
    inspected: _fs._InspectedEntry,
    *,
    include_size: bool,
    operation: PromotionOperation,
    changed: bool,
    size_mismatch: bool,
) -> _PhysicalSnapshot:
    size: int | None = None
    failure: _Failure | None = None
    if include_size:
        try:
            size = _fs._require_file_size(inspected.metadata)
        except _fs.PhysicalInspectionError as error:
            if _inspection_is_io_failure(error):
                category: PromotionCategory = "io_failure"
            elif changed:
                category = "concurrent_change"
            elif size_mismatch:
                category = "verification_mismatch"
            else:
                category = "unsafe_filesystem"
            failure = operation, category

    if failure is not None:
        _raise_failure(failure)
    return _PhysicalSnapshot(
        resolved=inspected.resolved,
        identity=_fs._physical_file_identity(inspected.metadata),
        device=_fs._physical_device(inspected.metadata),
        size=size,
    )


def _require_absent(
    path: Path,
    *,
    expected_parent: Path,
    operation: PromotionOperation,
    changed: bool,
    existing_category: PromotionCategory,
) -> Path:
    presence: _fs._OutputRoot | None = None
    failure: _Failure | None = None
    try:
        presence = _fs._inspect_entry_presence(
            path,
            expected_parent=expected_parent,
            field="target",
        )
    except _fs.PhysicalInspectionError as error:
        failure = (
            operation,
            _inspection_category(
                error,
                operation=operation,
                changed=changed,
                missing_is_mismatch=False,
            ),
        )

    if failure is not None:
        _raise_failure(failure)
    if presence is None:
        _raise_failure((operation, "io_failure"))
    if presence.state == "existing":
        existing_is_regular = False
        try:
            _fs._inspect_required_entry(path, field="target", kind="file")
            existing_is_regular = True
        except _fs.PhysicalInspectionError as error:
            failure = (
                operation,
                _inspection_category(
                    error,
                    operation=operation,
                    changed=changed,
                    missing_is_mismatch=False,
                ),
            )
        if failure is not None:
            _raise_failure(failure)
        if existing_is_regular:
            _raise_failure((operation, existing_category))
        _raise_failure((operation, "unsafe_filesystem"))
    return presence.projected


def _snapshot_changed(
    baseline: _PhysicalSnapshot,
    current: _PhysicalSnapshot,
) -> bool:
    return (
        baseline.resolved != current.resolved
        or baseline.identity != current.identity
        or baseline.device != current.device
        or baseline.size != current.size
    )


def _require_same_volume(snapshot: _PromotionSnapshot) -> None:
    source_device = snapshot.source.device
    staging_device = snapshot.staging_parent.device
    final_device = snapshot.final_parent.device
    if source_device is not None and staging_device is not None and source_device != staging_device:
        _raise_failure(("inspect_source", "unsafe_filesystem"))
    usable = {
        device for device in (source_device, staging_device, final_device) if device is not None
    }
    if len(usable) > 1:
        _raise_failure(("inspect_destination", "unsafe_filesystem"))


def _inspect_promotion_state(
    layout: ResearchPublicationLayout,
    artifact: ResearchFileArtifact,
    paths: ResearchArtifactPaths,
    selected: _SelectedEntry,
    *,
    baseline: _PromotionSnapshot | None,
) -> _PromotionSnapshot:
    changed = baseline is not None
    core = _inspect_core(
        layout,
        paths,
        baseline=None if baseline is None else baseline.core,
    )

    raw_root = _snapshot_entry(
        core.raw_root,
        include_size=False,
        operation="inspect_source",
        changed=changed,
        size_mismatch=False,
    )
    raw_manifest = _snapshot_entry(
        core.raw_manifest,
        include_size=True,
        operation="inspect_source",
        changed=changed,
        size_mismatch=False,
    )
    raw_artifact = _snapshot_entry(
        core.raw_artifact,
        include_size=True,
        operation="inspect_source",
        changed=changed,
        size_mismatch=True,
    )
    if raw_artifact.size != artifact.raw_bytes:
        _raise_failure(("inspect_source", "concurrent_change"))

    output_entry = _inspect_required_entry(
        layout.output_dir,
        field="output_dir",
        kind="directory",
        operation="inspect_destination",
        changed=changed,
        missing_is_mismatch=False,
        expected_resolved=core.output_root,
    )
    staging_root_entry = _inspect_required_entry(
        layout.staging_dir,
        field="staging_dir",
        kind="directory",
        operation="inspect_source",
        changed=changed,
        missing_is_mismatch=False,
        expected_resolved=core.staging_dir,
    )
    staging_parent_entry = _inspect_required_entry(
        selected.staging_parent_path,
        field="staging_parent",
        kind="directory",
        operation="inspect_source",
        changed=changed,
        missing_is_mismatch=False,
        expected_resolved=(
            core.staging_failure_dir if selected.kind == "failure" else core.staging_research_dir
        ),
    )
    final_parent_entry = _inspect_required_entry(
        selected.final_parent_path,
        field="final_parent",
        kind="directory",
        operation="inspect_destination",
        changed=changed,
        missing_is_mismatch=False,
        expected_resolved=(core.failure_dir if selected.kind == "failure" else core.research_dir),
    )
    source_entry = _inspect_required_entry(
        selected.source_path,
        field="staging_source",
        kind="file",
        operation="inspect_source",
        changed=changed,
        missing_is_mismatch=True,
        expected_parent=staging_parent_entry.resolved,
    )

    output_root = _snapshot_entry(
        output_entry,
        include_size=False,
        operation="inspect_destination",
        changed=changed,
        size_mismatch=False,
    )
    staging_root = _snapshot_entry(
        staging_root_entry,
        include_size=False,
        operation="inspect_source",
        changed=changed,
        size_mismatch=False,
    )
    staging_parent = _snapshot_entry(
        staging_parent_entry,
        include_size=False,
        operation="inspect_source",
        changed=changed,
        size_mismatch=False,
    )
    final_parent = _snapshot_entry(
        final_parent_entry,
        include_size=False,
        operation="inspect_destination",
        changed=changed,
        size_mismatch=False,
    )
    source = _snapshot_entry(
        source_entry,
        include_size=True,
        operation="inspect_source",
        changed=changed,
        size_mismatch=True,
    )
    if source.size != selected.expected_bytes:
        category: PromotionCategory = "concurrent_change" if changed else "verification_mismatch"
        _raise_failure(("inspect_source", category))

    destination_projected = _require_absent(
        selected.destination_path,
        expected_parent=final_parent.resolved,
        operation="inspect_destination",
        changed=changed,
        existing_category=("concurrent_change" if changed else "entry_exists"),
    )
    research_manifest_projected = _require_absent(
        layout.research_manifest_path,
        expected_parent=output_root.resolved,
        operation="inspect_destination",
        changed=changed,
        existing_category=("concurrent_change" if changed else "entry_exists"),
    )
    staging_manifest_projected = _require_absent(
        layout.staging_manifest_path,
        expected_parent=staging_root.resolved,
        operation="inspect_destination",
        changed=changed,
        existing_category=("concurrent_change" if changed else "entry_exists"),
    )

    current = _PromotionSnapshot(
        core=core,
        raw_root=raw_root,
        raw_manifest=raw_manifest,
        raw_artifact=raw_artifact,
        output_root=output_root,
        staging_root=staging_root,
        staging_parent=staging_parent,
        final_parent=final_parent,
        source=source,
        destination_projected=destination_projected,
        research_manifest_projected=research_manifest_projected,
        staging_manifest_projected=staging_manifest_projected,
    )

    if baseline is not None:
        if any(
            _snapshot_changed(first, second)
            for first, second in (
                (baseline.raw_root, current.raw_root),
                (baseline.raw_manifest, current.raw_manifest),
                (baseline.raw_artifact, current.raw_artifact),
                (baseline.staging_root, current.staging_root),
                (baseline.staging_parent, current.staging_parent),
                (baseline.source, current.source),
            )
        ):
            _raise_failure(("inspect_source", "concurrent_change"))
        if any(
            _snapshot_changed(first, second)
            for first, second in (
                (baseline.output_root, current.output_root),
                (baseline.final_parent, current.final_parent),
            )
        ) or (
            baseline.destination_projected != current.destination_projected
            or baseline.research_manifest_projected != current.research_manifest_projected
            or baseline.staging_manifest_projected != current.staging_manifest_projected
        ):
            _raise_failure(("inspect_destination", "concurrent_change"))

    _require_same_volume(current)
    return current


def _perform_rename(source: Path, destination: Path) -> None:
    failure: _Failure | None = None
    try:
        os.rename(source, destination)
    except FileExistsError:
        failure = "promote", "entry_exists"
    except OSError:
        failure = "promote", "io_failure"
    if failure is not None:
        _raise_failure(failure)


def _verify_parent_snapshot(
    path: Path,
    *,
    field: str,
    expected: _PhysicalSnapshot,
) -> _PhysicalSnapshot:
    entry = _inspect_required_entry(
        path,
        field=field,
        kind="directory",
        operation="verify_destination",
        changed=True,
        missing_is_mismatch=False,
        expected_resolved=expected.resolved,
    )
    current = _snapshot_entry(
        entry,
        include_size=False,
        operation="verify_destination",
        changed=True,
        size_mismatch=False,
    )
    if _snapshot_changed(expected, current):
        _raise_failure(("verify_destination", "concurrent_change"))
    return current


def _inspect_promoted_destination(
    selected: _SelectedEntry,
    snapshot: _PromotionSnapshot,
    *,
    previous: _PhysicalSnapshot | None,
) -> _PhysicalSnapshot:
    _verify_parent_snapshot(
        snapshot.core.output_root,
        field="output_dir",
        expected=snapshot.output_root,
    )
    _verify_parent_snapshot(
        snapshot.core.staging_dir,
        field="staging_dir",
        expected=snapshot.staging_root,
    )
    _verify_parent_snapshot(
        selected.staging_parent_path,
        field="staging_parent",
        expected=snapshot.staging_parent,
    )
    final_parent = _verify_parent_snapshot(
        selected.final_parent_path,
        field="final_parent",
        expected=snapshot.final_parent,
    )
    destination_entry = _inspect_required_entry(
        selected.destination_path,
        field="promoted_entry",
        kind="file",
        operation="verify_destination",
        changed=True,
        missing_is_mismatch=True,
        expected_parent=final_parent.resolved,
        expected_resolved=snapshot.destination_projected,
    )
    destination = _snapshot_entry(
        destination_entry,
        include_size=True,
        operation="verify_destination",
        changed=previous is not None,
        size_mismatch=True,
    )
    if previous is not None and _snapshot_changed(previous, destination):
        _raise_failure(("verify_destination", "concurrent_change"))
    if destination.size != selected.expected_bytes:
        _raise_failure(("verify_destination", "verification_mismatch"))
    if (
        snapshot.source.identity != destination.identity
        or snapshot.source.device != destination.device
    ):
        _raise_failure(("verify_destination", "verification_mismatch"))
    return destination


def _verify_post_rename(
    layout: ResearchPublicationLayout,
    selected: _SelectedEntry,
    snapshot: _PromotionSnapshot,
) -> _PhysicalSnapshot:
    first_destination = _inspect_promoted_destination(
        selected,
        snapshot,
        previous=None,
    )
    _require_absent(
        selected.source_path,
        expected_parent=snapshot.staging_parent.resolved,
        operation="verify_destination",
        changed=True,
        existing_category="concurrent_change",
    )
    _require_absent(
        layout.research_manifest_path,
        expected_parent=snapshot.output_root.resolved,
        operation="verify_destination",
        changed=True,
        existing_category="concurrent_change",
    )
    _require_absent(
        layout.staging_manifest_path,
        expected_parent=snapshot.staging_root.resolved,
        operation="verify_destination",
        changed=True,
        existing_category="concurrent_change",
    )
    return _inspect_promoted_destination(
        selected,
        snapshot,
        previous=first_destination,
    )


def promote_staged_entry_no_clobber(
    layout: ResearchPublicationLayout,
    *,
    artifact: ResearchFileArtifact,
    kind: PromotionEntryKind,
) -> PromotedEntryState:
    """Promote one closed durable stage and return observational final state.

    The returned state verifies the canonical final entry. It is not proof that
    this invocation performed the physical rename and is not an ownership,
    lease, retry, idempotency, or dataset-commit token.
    """
    exact_layout, exact_artifact, exact_kind, _plan, paths = _require_inputs(
        layout,
        artifact,
        kind,
    )
    if not _supports_windows_no_clobber_rename():
        _raise_failure(("validate_input", "unsupported_platform"))

    selected = _select_entry(exact_layout, exact_artifact, paths, exact_kind)
    _run_preflight(exact_layout, exact_artifact)
    initial = _inspect_promotion_state(
        exact_layout,
        exact_artifact,
        paths,
        selected,
        baseline=None,
    )
    jit = _inspect_promotion_state(
        exact_layout,
        exact_artifact,
        paths,
        selected,
        baseline=initial,
    )

    _perform_rename(jit.source.resolved, jit.destination_projected)
    destination = _verify_post_rename(exact_layout, selected, jit)
    return PromotedEntryState(
        kind=exact_kind,
        byte_count=selected.expected_bytes,
        physical_identity=destination.identity,
    )
