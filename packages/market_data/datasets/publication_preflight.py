"""DATA-005 B-2B C2/5 read-only physical publication preflight.

The checks in this module are a point-in-time filesystem snapshot. They do not
lock the filesystem and do not authorize publication. C3 must revalidate every
critical path immediately before directory creation, staging writes, and atomic
replacement to reduce, but not claim to eliminate, TOCTOU risk.

This module performs metadata inspection only. It never opens or reads file
contents and never creates, modifies, renames, or removes filesystem entries.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import packages.market_data.datasets._publication_fs as _fs
from packages.market_data.datasets.conversion_manifest import ResearchFileArtifact
from packages.market_data.datasets.downloader import MANIFEST_FILE
from packages.market_data.datasets.publication_layout import ResearchPublicationLayout

OutputDirectoryState = Literal["existing", "missing"]

_OUTPUT_DIRECTORY_STATES: frozenset[str] = frozenset({"existing", "missing"})


class PublicationPreflightError(ValueError):
    """Sanitized physical-publication preflight failure."""

    __slots__ = ("field", "reason")

    def __init__(self, *, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


def _error(field: str, reason: str) -> PublicationPreflightError:
    return PublicationPreflightError(field=field, reason=reason)


def _require_non_negative_integer(
    value: object,
    *,
    field: str,
    reason: str,
) -> int:
    if type(value) is not int or value < 0:
        raise _error(field, reason)
    return value


@dataclass(frozen=True, kw_only=True)
class PublicationPreflightResult:
    """Immutable point-in-time summary of a successful read-only preflight."""

    raw_file_count: int
    raw_bytes: int
    output_directory_state: OutputDirectoryState

    def __post_init__(self) -> None:
        _require_non_negative_integer(
            self.raw_file_count,
            field="raw_file_count",
            reason="raw_file_count must be a non-negative integer",
        )
        _require_non_negative_integer(
            self.raw_bytes,
            field="raw_bytes",
            reason="raw_bytes must be a non-negative integer",
        )
        if (
            type(self.output_directory_state) is not str
            or self.output_directory_state not in _OUTPUT_DIRECTORY_STATES
        ):
            raise _error("output_dir", "state must be existing or missing")


def _require_arguments(
    layout: ResearchPublicationLayout,
    artifacts: tuple[ResearchFileArtifact, ...],
) -> tuple[ResearchPublicationLayout, tuple[ResearchFileArtifact, ...]]:
    if type(layout) is not ResearchPublicationLayout:
        raise _error("layout", "must be an exact ResearchPublicationLayout")
    if type(artifacts) is not tuple:
        raise _error("artifacts", "must be an exact tuple")

    raw_names: set[str] = set()
    for artifact in artifacts:
        if type(artifact) is not ResearchFileArtifact:
            raise _error("artifacts", "must contain exact ResearchFileArtifact values")
        if artifact.raw_name in raw_names:
            raise _error("artifacts", "must not contain duplicate raw names")
        raw_names.add(artifact.raw_name)
    return layout, artifacts


def _inspect_physical_publication(
    layout: ResearchPublicationLayout,
    artifacts: tuple[ResearchFileArtifact, ...],
) -> tuple[int, int, OutputDirectoryState]:
    _fs._require_local_root(layout.raw_dir, field="raw_dir")
    _fs._require_local_root(layout.output_dir, field="output_dir")

    raw_root = _fs._inspect_required_entry(
        layout.raw_dir,
        field="raw_dir",
        kind="directory",
    )

    raw_manifest_path = layout.raw_manifest_path
    if raw_manifest_path.name != MANIFEST_FILE:
        raise _error("raw_manifest", "must use the downloader manifest filename")
    raw_manifest = _fs._inspect_required_entry(
        raw_manifest_path,
        field="raw_manifest",
        kind="file",
    )
    _fs._require_direct_parent(
        raw_manifest.resolved,
        expected_parent=raw_root.resolved,
    )

    output_root = _fs._inspect_output_root(layout.output_dir)
    _fs._reject_related_physical_roots(raw_root.resolved, output_root.projected)

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
    _fs._inspect_output_child(
        layout.research_manifest_path,
        expected_parent=output_root.projected,
        kind="file",
    )
    _fs._inspect_output_child(
        layout.staging_manifest_path,
        expected_parent=staging_dir.projected,
        kind="file",
    )

    resolved_raw_paths = {raw_manifest.resolved}
    physical_file_identities: set[tuple[int, int]] = set()
    raw_manifest_identity = _fs._physical_file_identity(raw_manifest.metadata)
    if raw_manifest_identity is not None:
        physical_file_identities.add(raw_manifest_identity)
    raw_sizes: list[int] = []
    for artifact in artifacts:
        inspected = _fs._inspect_required_entry(
            layout.raw_path(artifact),
            field="raw_artifact",
            kind="file",
        )
        _fs._require_direct_parent(
            inspected.resolved,
            expected_parent=raw_root.resolved,
        )
        physical_identity = _fs._physical_file_identity(inspected.metadata)
        if inspected.resolved in resolved_raw_paths or (
            physical_identity is not None and physical_identity in physical_file_identities
        ):
            raise _error("artifacts", "must resolve to distinct physical raw paths")
        resolved_raw_paths.add(inspected.resolved)
        if physical_identity is not None:
            physical_file_identities.add(physical_identity)
        physical_size = _fs._require_file_size(inspected.metadata)
        if physical_size != artifact.raw_bytes:
            raise _error("raw_artifact", "physical size must match artifact raw_bytes")
        raw_sizes.append(physical_size)

        _fs._inspect_output_child(
            layout.research_path(artifact),
            expected_parent=research_dir.projected,
            kind="file",
        )
        _fs._inspect_output_child(
            layout.failure_path(artifact),
            expected_parent=failure_dir.projected,
            kind="file",
        )
        _fs._inspect_output_child(
            layout.staging_research_path(artifact),
            expected_parent=staging_research_dir.projected,
            kind="file",
        )
        _fs._inspect_output_child(
            layout.staging_failure_path(artifact),
            expected_parent=staging_failure_dir.projected,
            kind="file",
        )

    return len(artifacts), sum(raw_sizes), output_root.state


def preflight_research_publication(
    layout: ResearchPublicationLayout,
    artifacts: tuple[ResearchFileArtifact, ...],
) -> PublicationPreflightResult:
    """Validate physical publication paths without reading or mutating contents.

    The returned value describes only the instant at which these checks ran.
    It is not a capability or security guarantee for later filesystem writes.
    """
    exact_layout, exact_artifacts = _require_arguments(layout, artifacts)
    raw_file_count = 0
    raw_bytes = 0
    output_directory_state: OutputDirectoryState = "missing"
    inspection_failure: tuple[str, str] | None = None
    try:
        raw_file_count, raw_bytes, output_directory_state = _inspect_physical_publication(
            exact_layout,
            exact_artifacts,
        )
    except _fs.PhysicalInspectionError as error:
        inspection_failure = error.field, error.reason

    if inspection_failure is not None:
        raise _error(*inspection_failure)
    return PublicationPreflightResult(
        raw_file_count=raw_file_count,
        raw_bytes=raw_bytes,
        output_directory_state=output_directory_state,
    )
