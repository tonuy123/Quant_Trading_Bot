"""DATA-005 B-2B C1/5: immutable, deterministic physical path layout for research publication.

This module defines the canonical physical path layout contract for research
publication. It performs ONLY lexical Path operations; it does NOT read, write,
stat, resolve, or otherwise interact with the filesystem.

Scope (C1 slice):
- Pure lexical path derivation from validated inputs.
- Immutable, frozen dataclass contract.
- Strict root validation using Path API only (no I/O).

Out of scope (future slices):
- Symlink / containment against real filesystem (C2)
- Directory creation (C2/C3)
- Atomic write/publish (C3)
- Manifest read/write/checksum (C4)
- Resume/cached rerun (C5)
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePath

from packages.market_data.datasets.conversion_manifest import (
    RESEARCH_MANIFEST_FILE,
    ResearchFileArtifact,
    ResearchFilePlan,
)
from packages.market_data.datasets.downloader import MANIFEST_FILE

# =============================================================================
# Public constants
# =============================================================================

RESEARCH_OUTPUT_DIRECTORY: str = "research"
"""Directory name for published research output files."""

FAILURE_OUTPUT_DIRECTORY: str = "failures"
"""Directory name for published failure sidecar files."""

STAGING_DIRECTORY: str = ".staging"
"""Directory name for atomic-staging area."""


# =============================================================================
# Validation helpers (lexical only, no I/O)
# =============================================================================

_DANGEROUS_COMPONENTS: frozenset[str] = frozenset({".."})


def _is_dangerous_component(part: str) -> bool:
    return part in _DANGEROUS_COMPONENTS or "\0" in part or "\r" in part or "\n" in part


def _validate_path_root(
    value: Path,
    *,
    field: str,
) -> Path:
    """Validate a path is a safe absolute root (lexical only)."""
    # Must be a Path subclass instance (no str coercion).
    # Use PurePath as the base check so PureWindowsPath / PurePosixPath also pass.
    if not isinstance(value, PurePath):
        raise PublicationLayoutValidationError(field=field, reason="must be a Path")

    # Must be absolute
    if not value.is_absolute():
        raise PublicationLayoutValidationError(field=field, reason="must be absolute")

    # Must not be a filesystem anchor/root.
    if value.parent == value:
        raise PublicationLayoutValidationError(
            field=field, reason="must not be a filesystem root/anchor"
        )

    # Check path components for dangerous values
    for part in value.parts:
        if _is_dangerous_component(part):
            raise PublicationLayoutValidationError(
                field=field, reason="must not contain '..', NUL, CR, or LF"
            )

    return value


def _require_path_root(value: object, *, field: str) -> Path:
    """Require a safe absolute Path root."""
    if not isinstance(value, Path):
        raise PublicationLayoutValidationError(field=field, reason="must be a Path")
    return _validate_path_root(value, field=field)


def _require_artifact(value: object) -> ResearchFileArtifact:
    if type(value) is not ResearchFileArtifact:
        raise PublicationLayoutValidationError(
            field="artifact",
            reason="must be an exact ResearchFileArtifact",
        )
    return value


def _require_plan(value: object) -> ResearchFilePlan:
    if type(value) is not ResearchFilePlan:
        raise PublicationLayoutValidationError(
            field="plan",
            reason="must be an exact ResearchFilePlan",
        )
    return value


# =============================================================================
# Artifact paths contract
# =============================================================================


@dataclass(frozen=True, kw_only=True)
class ResearchArtifactPaths:
    """Immutable lexical path contract derived from a layout + plan.

    Performs ONLY lexical Path operations. Does NOT read, write, stat, resolve,
    or otherwise interact with the filesystem.
    """

    raw_path: Path
    """Absolute path to the raw input file."""

    research_path: Path
    """Absolute path to the published research output file."""

    failure_path: Path
    """Absolute path to the published failure sidecar file."""

    staging_research_path: Path
    """Absolute path to the staging research file (tmp-suffixed)."""

    staging_failure_path: Path
    """Absolute path to the staging failure file (tmp-suffixed)."""

    def __post_init__(self) -> None:
        # Strict lexical validation: all fields must be absolute Path instances.
        for field in (
            "raw_path",
            "research_path",
            "failure_path",
            "staging_research_path",
            "staging_failure_path",
        ):
            value = getattr(self, field)
            if not isinstance(value, Path):
                raise PublicationLayoutValidationError(field=field, reason="must be a Path")
            if not value.is_absolute():
                raise PublicationLayoutValidationError(field=field, reason="must be absolute")
            # Check for dangerous components.
            for part in value.parts:
                if _is_dangerous_component(part):
                    raise PublicationLayoutValidationError(
                        field=field,
                        reason="must not contain '..', NUL, CR, or LF",
                    )


# =============================================================================
# Error type
# =============================================================================


class PublicationLayoutValidationError(ValueError):
    """Sanitized publication-layout contract failure.

    Stores only the validated field name and a sanitized reason string.
    Does NOT store raw paths or original exceptions.
    """

    __slots__ = ("field", "reason")

    def __init__(self, *, field: str, reason: str) -> None:
        self.field: str = field
        self.reason: str = reason
        super().__init__(f"{field}: {reason}")


# =============================================================================
# Layout contract
# =============================================================================


@dataclass(frozen=True, kw_only=True)
class ResearchPublicationLayout:
    """Immutable, deterministic physical path layout for research publication.

    This dataclass derives all paths through lexical operations only. It never
    reads, writes, resolves, or inspects the filesystem.

    Invariants (enforced at construction):
    - raw_dir and output_dir are distinct absolute Path objects.
    - Neither root is lexical descendant of the other.
    - No path component contains '..', NUL, CR, or LF.
    """

    raw_dir: Path
    """Absolute path to the directory holding raw input files and manifest."""

    output_dir: Path
    """Absolute path to the published research output directory."""

    # -------------------------------------------------------------------------
    # Derived properties (computed on access, no caching, no I/O)
    # -------------------------------------------------------------------------

    @property
    def raw_manifest_path(self) -> Path:
        """Path to the raw manifest file inside raw_dir."""
        return self.raw_dir / MANIFEST_FILE

    @property
    def research_manifest_path(self) -> Path:
        """Path to the research manifest file inside output_dir."""
        return self.output_dir / RESEARCH_MANIFEST_FILE

    @property
    def research_dir(self) -> Path:
        """Path to the published research output directory."""
        return self.output_dir / RESEARCH_OUTPUT_DIRECTORY

    @property
    def failure_dir(self) -> Path:
        """Path to the published failure sidecar directory."""
        return self.output_dir / FAILURE_OUTPUT_DIRECTORY

    @property
    def staging_dir(self) -> Path:
        """Path to the atomic-staging root directory."""
        return self.output_dir / STAGING_DIRECTORY

    @property
    def staging_research_dir(self) -> Path:
        """Path to the staging research output directory."""
        return self.staging_dir / RESEARCH_OUTPUT_DIRECTORY

    @property
    def staging_failure_dir(self) -> Path:
        """Path to the staging failure sidecar directory."""
        return self.staging_dir / FAILURE_OUTPUT_DIRECTORY

    @property
    def staging_manifest_path(self) -> Path:
        """Path to the staging manifest file (tmp-suffixed)."""
        return self.staging_dir / f"{RESEARCH_MANIFEST_FILE}.tmp"

    # -------------------------------------------------------------------------
    # Artifact path derivation
    # -------------------------------------------------------------------------

    def raw_path(self, artifact: ResearchFileArtifact) -> Path:
        """Path to the raw file for the given artifact."""
        exact_artifact = _require_artifact(artifact)
        return self.raw_dir / exact_artifact.raw_name

    def research_path(self, artifact: ResearchFileArtifact) -> Path:
        """Path to the published research file for the given artifact."""
        exact_artifact = _require_artifact(artifact)
        return self.research_dir / exact_artifact.research_name

    def failure_path(self, artifact: ResearchFileArtifact) -> Path:
        """Path to the published failure sidecar file for the given artifact."""
        exact_artifact = _require_artifact(artifact)
        return self.failure_dir / exact_artifact.failure_name

    def staging_research_path(self, artifact: ResearchFileArtifact) -> Path:
        """Path to the staging research file for the given artifact (tmp-suffixed)."""
        exact_artifact = _require_artifact(artifact)
        return self.staging_research_dir / f"{exact_artifact.research_name}.tmp"

    def staging_failure_path(self, artifact: ResearchFileArtifact) -> Path:
        """Path to the staging failure file for the given artifact (tmp-suffixed)."""
        exact_artifact = _require_artifact(artifact)
        return self.staging_failure_dir / f"{exact_artifact.failure_name}.tmp"

    def artifact_paths_for(self, plan: ResearchFilePlan) -> ResearchArtifactPaths:
        """Derive all five canonical artifact paths from a validated plan.

        This method is pure and deterministic: given the same layout and plan,
        it always returns the same paths. No filesystem I/O is performed.
        """
        exact_plan = _require_plan(plan)
        return ResearchArtifactPaths(
            raw_path=self.raw_dir / exact_plan.raw_name,
            research_path=self.research_dir / exact_plan.research_name,
            failure_path=self.failure_dir / exact_plan.failure_name,
            staging_research_path=self.staging_research_dir / f"{exact_plan.research_name}.tmp",
            staging_failure_path=self.staging_failure_dir / f"{exact_plan.failure_name}.tmp",
        )

    # -------------------------------------------------------------------------
    # Construction validation
    # -------------------------------------------------------------------------

    def __post_init__(self) -> None:
        raw = _require_path_root(self.raw_dir, field="raw_dir")
        out = _require_path_root(self.output_dir, field="output_dir")

        # Roots must be distinct
        if raw == out:
            raise PublicationLayoutValidationError(
                field="raw_dir", reason="must not equal output_dir"
            )

        # Neither root may be a lexical descendant of the other.
        if raw.is_relative_to(out):
            raise PublicationLayoutValidationError(
                field="raw_dir",
                reason="must not be a lexical descendant of output_dir",
            )
        if out.is_relative_to(raw):
            raise PublicationLayoutValidationError(
                field="output_dir",
                reason="must not be a lexical descendant of raw_dir",
            )
