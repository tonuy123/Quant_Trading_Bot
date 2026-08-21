"""Shared read-only physical filesystem inspection for research publication.

The helpers in this private module inspect filesystem metadata only. They do
not open or read file contents and never create, modify, rename, or remove
filesystem entries. Their results are point-in-time observations; callers that
mutate the filesystem must revalidate critical paths immediately beforehand.
"""

from __future__ import annotations

import stat
from dataclasses import dataclass
from os import stat_result
from pathlib import Path
from typing import Literal

_EntryKind = Literal["directory", "file"]
_OutputDirectoryState = Literal["existing", "missing"]
_REPARSE_POINT_ATTRIBUTE: int = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class PhysicalInspectionError(ValueError):
    """Sanitized failure from a read-only physical filesystem inspection."""

    __slots__ = ("field", "reason")
    field: str
    reason: str

    def __init__(self, *, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


class PhysicalResolutionError(PhysicalInspectionError):
    """Sanitized strict-resolution failure distinguishable by internal callers."""

    __slots__ = ()


def _error(field: str, reason: str) -> PhysicalInspectionError:
    return PhysicalInspectionError(field=field, reason=reason)


@dataclass(frozen=True, slots=True)
class _InspectedEntry:
    metadata: stat_result
    resolved: Path


@dataclass(frozen=True, slots=True)
class _OutputRoot:
    state: _OutputDirectoryState
    projected: Path


@dataclass(frozen=True, slots=True)
class _DirectoryCreationPlan:
    existing_ancestor: _InspectedEntry
    missing_components: tuple[Path, ...]
    projected: Path


def _component_chain(path: Path) -> tuple[Path, ...]:
    """Return the lexical chain from the filesystem anchor through ``path``."""
    return tuple(reversed((path, *path.parents)))


def _require_local_root(path: Path, *, field: str) -> None:
    if len(path.drive) >= 2 and path.drive[:2] == "\\\\":
        raise _error(field, "must use a local filesystem path")


def _lstat_or_missing(path: Path, *, field: str) -> stat_result | None:
    metadata: stat_result | None = None
    missing = False
    inspection_failed = False
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        missing = True
    except OSError:
        inspection_failed = True

    if inspection_failed:
        raise _error(field, "filesystem entry could not be inspected")
    if missing:
        return None
    return metadata


def _is_junction(path: Path, *, field: str) -> bool:
    junction_method = getattr(path, "is_junction", None)
    if junction_method is None:
        return False

    junction = False
    inspection_failed = False
    try:
        junction = bool(junction_method())
    except OSError:
        inspection_failed = True

    if inspection_failed:
        raise _error(field, "filesystem entry could not be inspected")
    return junction


def _reject_redirection(path: Path, metadata: stat_result, *, field: str) -> None:
    attributes = getattr(metadata, "st_file_attributes", 0)
    if type(attributes) is not int:
        raise _error(field, "filesystem entry could not be inspected")
    if stat.S_ISLNK(metadata.st_mode) or attributes & _REPARSE_POINT_ATTRIBUTE:
        raise _error(field, "filesystem entry must not be a symlink or reparse point")
    if _is_junction(path, field=field):
        raise _error(field, "filesystem entry must not be a symlink or reparse point")


def _resolve_strict(path: Path) -> Path:
    resolved: Path | None = None
    containment_failed = False
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError):
        containment_failed = True

    if containment_failed or resolved is None:
        raise PhysicalResolutionError(
            field="containment",
            reason="filesystem containment could not be verified",
        )
    return resolved


def _inspect_required_entry(
    path: Path,
    *,
    field: str,
    kind: _EntryKind,
) -> _InspectedEntry:
    final_metadata: stat_result | None = None
    for component in _component_chain(path):
        metadata = _lstat_or_missing(component, field=field)
        if metadata is None:
            raise _error(field, "filesystem entry does not exist")
        _reject_redirection(component, metadata, field=field)
        if component != path and not stat.S_ISDIR(metadata.st_mode):
            raise _error(field, "filesystem entry must be a directory")
        final_metadata = metadata

    if final_metadata is None:
        raise _error(field, "filesystem entry could not be inspected")
    if kind == "directory" and not stat.S_ISDIR(final_metadata.st_mode):
        raise _error(field, "filesystem entry must be a directory")
    if kind == "file" and not stat.S_ISREG(final_metadata.st_mode):
        raise _error(field, "filesystem entry must be a regular file")
    return _InspectedEntry(
        metadata=final_metadata,
        resolved=_resolve_strict(path),
    )


def _inspect_optional_entry(
    path: Path,
    *,
    kind: _EntryKind,
) -> _OutputRoot:
    nearest_existing: Path | None = None
    final_metadata: stat_result | None = None
    for component in _component_chain(path):
        field = "output_dir" if component == path else "output_ancestor"
        metadata = _lstat_or_missing(component, field=field)
        if metadata is None:
            if nearest_existing is None:
                raise _error("output_ancestor", "filesystem entry does not exist")
            resolved_ancestor = _resolve_strict(nearest_existing)
            missing_tail = path.relative_to(nearest_existing)
            return _OutputRoot(
                state="missing",
                projected=resolved_ancestor / missing_tail,
            )
        _reject_redirection(component, metadata, field=field)
        if component != path and not stat.S_ISDIR(metadata.st_mode):
            raise _error(field, "filesystem entry must be a directory")
        nearest_existing = component
        final_metadata = metadata

    if nearest_existing is None or final_metadata is None:
        raise _error("output_ancestor", "filesystem entry could not be inspected")
    if kind == "directory" and not stat.S_ISDIR(final_metadata.st_mode):
        raise _error("output_dir", "filesystem entry must be a directory")
    if kind == "file" and not stat.S_ISREG(final_metadata.st_mode):
        raise _error("output_dir", "filesystem entry must be a regular file")
    return _OutputRoot(state="existing", projected=_resolve_strict(path))


def _inspect_output_root(path: Path) -> _OutputRoot:
    return _inspect_optional_entry(path, kind="directory")


def _inspect_output_child(
    path: Path,
    *,
    expected_parent: Path,
    kind: _EntryKind,
) -> _OutputRoot:
    inspected = _inspect_optional_entry(path, kind=kind)
    _require_direct_parent(inspected.projected, expected_parent=expected_parent)
    return inspected


def _inspect_directory_creation_plan(
    path: Path,
    *,
    field: str,
) -> _DirectoryCreationPlan:
    """Inspect a directory path and identify safe missing components to create."""
    nearest_existing_path: Path | None = None
    nearest_existing_metadata: stat_result | None = None
    missing_components: list[Path] = []
    missing_tail_started = False

    for component in _component_chain(path):
        if missing_tail_started:
            missing_components.append(component)
            continue

        metadata = _lstat_or_missing(component, field=field)
        if metadata is None:
            if nearest_existing_path is None or nearest_existing_metadata is None:
                raise _error(field, "filesystem entry does not exist")
            missing_tail_started = True
            missing_components.append(component)
            continue

        _reject_redirection(component, metadata, field=field)
        if not stat.S_ISDIR(metadata.st_mode):
            raise _error(field, "filesystem entry must be a directory")
        nearest_existing_path = component
        nearest_existing_metadata = metadata

    if nearest_existing_path is None or nearest_existing_metadata is None:
        raise _error(field, "filesystem entry could not be inspected")

    resolved_ancestor = _resolve_strict(nearest_existing_path)
    if missing_components:
        projected = resolved_ancestor / path.relative_to(nearest_existing_path)
    else:
        projected = resolved_ancestor

    return _DirectoryCreationPlan(
        existing_ancestor=_InspectedEntry(
            metadata=nearest_existing_metadata,
            resolved=resolved_ancestor,
        ),
        missing_components=tuple(missing_components),
        projected=projected,
    )


def _inspect_entry_presence(
    path: Path,
    *,
    expected_parent: Path,
    field: str,
) -> _OutputRoot:
    """Inspect a target without constraining the kind of an existing entry."""
    nearest_existing: Path | None = None
    final_metadata: stat_result | None = None

    for component in _component_chain(path):
        metadata = _lstat_or_missing(component, field=field)
        if metadata is None:
            if nearest_existing is None:
                raise _error(field, "filesystem entry does not exist")
            resolved_ancestor = _resolve_strict(nearest_existing)
            missing_tail = path.relative_to(nearest_existing)
            inspected = _OutputRoot(
                state="missing",
                projected=resolved_ancestor / missing_tail,
            )
            _require_direct_parent(
                inspected.projected,
                expected_parent=expected_parent,
            )
            return inspected

        _reject_redirection(component, metadata, field=field)
        if component != path and not stat.S_ISDIR(metadata.st_mode):
            raise _error(field, "filesystem entry must be a directory")
        nearest_existing = component
        final_metadata = metadata

    if nearest_existing is None or final_metadata is None:
        raise _error(field, "filesystem entry could not be inspected")

    inspected = _OutputRoot(state="existing", projected=_resolve_strict(path))
    _require_direct_parent(
        inspected.projected,
        expected_parent=expected_parent,
    )
    return inspected


def _require_direct_parent(path: Path, *, expected_parent: Path) -> None:
    if path.parent != expected_parent:
        raise _error("containment", "filesystem containment could not be verified")


def _reject_related_physical_roots(raw_root: Path, output_root: Path) -> None:
    if (
        raw_root == output_root
        or raw_root.is_relative_to(output_root)
        or output_root.is_relative_to(raw_root)
    ):
        raise _error("containment", "physical roots must be distinct and non-nested")


def _require_file_size(metadata: stat_result) -> int:
    size = metadata.st_size
    if type(size) is not int or size < 0:
        raise _error("raw_artifact", "filesystem entry has an invalid size")
    return size


def _physical_device(metadata: stat_result) -> int | None:
    device = metadata.st_dev
    if type(device) is int:
        return device
    return None


def _physical_file_identity(metadata: stat_result) -> tuple[int, int] | None:
    device = metadata.st_dev
    inode = metadata.st_ino
    if type(device) is int and type(inode) is int and inode != 0:
        return device, inode
    return None


def _require_regular_zero_byte_file(
    metadata: stat_result,
    *,
    field: str,
) -> None:
    mode = metadata.st_mode
    if type(mode) is not int or not stat.S_ISREG(mode):
        raise _error(field, "filesystem entry must be a regular file")

    size = metadata.st_size
    if type(size) is not int or size != 0:
        raise _error(field, "filesystem entry must be empty")


def _require_matching_physical_identity(
    first: stat_result,
    second: stat_result,
    *,
    field: str,
) -> tuple[int, int] | None:
    first_identity = _physical_file_identity(first)
    second_identity = _physical_file_identity(second)
    if first_identity is None and second_identity is None:
        return None
    if first_identity is None or second_identity is None:
        raise _error(field, "filesystem entry identity does not match")
    if first_identity != second_identity:
        raise _error(field, "filesystem entry identity does not match")
    return first_identity
