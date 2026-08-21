"""DATA-003: archive checksum verification (read-only, deterministic).

Byte semantics (documented contract, do not silently change)
------------------------------------------------------------
* ``verify_file_checksum`` / ``compute_file_checksum`` hash the **raw file
  bytes** (archive/file integrity), streaming in fixed chunks
  (:data:`CHUNK_SIZE`, 64 KiB) — never the whole file in memory.
* ``verify_bytes_checksum`` hashes an in-memory byte payload; it is the
  verification counterpart of ``compute_dataset_checksum`` (metadata.py),
  which defines the dataset **content** checksum over record payload bytes.
* ``metadata_checksum()`` (metadata.py) is a separate concept: the digest of
  the canonical JSON metadata document itself.  It is never mixed with file
  bytes or record payload bytes.
* The only supported algorithm is SHA-256; expected values must be
  64-character **lowercase** hex digests (same policy as ``DatasetMetadata``).
  Uppercase or otherwise malformed expectations are rejected with
  ``ValueError`` before any file I/O, never silently canonicalized.

Read-only guarantees
--------------------
Verification never writes: no ``.tmp`` files, no manifest rewrite, no data
mutation, no network access.  A file that disappears between scan and read is
reported as ``missing``/``read_failure`` — never as a false PASS.

Path safety
-----------
Expected file names are validated (no separators, no ``.``/``..``) and every
path is resolved and containment-checked against the dataset directory, so a
symlink escaping the directory is reported ``invalid`` and never followed for
hashing.  ``manifest.json`` is dataset bookkeeping and is excluded from the
unexpected-files scan.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

CHUNK_SIZE = 65_536

CHECKSUM_FILE_NAME = "manifest.json"

_HEX64_PATTERN = re.compile(r"^[0-9a-f]{64}$")

ChecksumAlgorithm = Literal["sha256"]
FileStatus = Literal["verified", "mismatch", "missing", "invalid", "read_failure"]
OverallStatus = Literal["verified", "mismatch", "missing", "invalid", "read_failure"]


def validate_expected_checksum(expected: str) -> None:
    """Reject anything that is not a 64-character lowercase SHA-256 hex digest.

    Called before any file I/O so malformed expectations never trigger reads.
    """
    if not isinstance(expected, str):
        raise ValueError("expected checksum must be a string")
    if not _HEX64_PATTERN.match(expected):
        raise ValueError("expected checksum must be a 64-character lowercase SHA-256 hex digest")


def _require_algorithm(algorithm: str) -> ChecksumAlgorithm:
    if algorithm != "sha256":
        raise ValueError(f"unsupported checksum algorithm {algorithm!r}; supported: ['sha256']")
    return "sha256"


def _digest_file(path: Path, chunk_size: int) -> tuple[str, int]:
    digest = hashlib.sha256()
    bytes_read = 0
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
            bytes_read += len(block)
    return digest.hexdigest(), bytes_read


@dataclass(frozen=True, kw_only=True)
class ChecksumVerificationResult:
    """Outcome of verifying one file or byte payload against an expectation."""

    algorithm: ChecksumAlgorithm
    expected: str
    actual: str | None
    matched: bool
    bytes_read: int
    name: str
    error: str | None = None


def compute_file_checksum(
    path: Path, algorithm: str = "sha256", *, chunk_size: int = CHUNK_SIZE
) -> str:
    """Return the SHA-256 digest of raw file bytes (streaming).

    Raises ``FileNotFoundError`` for a missing file, ``ValueError`` for a
    directory or unsupported algorithm, and ``OSError`` for read failures.
    """
    _require_algorithm(algorithm)
    if path.is_dir():
        raise ValueError(f"{path.name!r} is a directory, not a file")
    digest, _ = _digest_file(path, chunk_size)
    return digest


def verify_bytes_checksum(
    payload: bytes,
    expected: str,
    algorithm: str = "sha256",
    *,
    name: str = "<bytes>",
) -> ChecksumVerificationResult:
    """Verify an in-memory byte payload against an expected digest."""
    algorithm = _require_algorithm(algorithm)
    validate_expected_checksum(expected)
    actual = hashlib.sha256(payload).hexdigest()
    return ChecksumVerificationResult(
        algorithm=algorithm,
        expected=expected,
        actual=actual,
        matched=actual == expected,
        bytes_read=len(payload),
        name=name,
    )


def verify_file_checksum(
    path: Path,
    expected: str,
    algorithm: str = "sha256",
    *,
    chunk_size: int = CHUNK_SIZE,
) -> ChecksumVerificationResult:
    """Verify raw file bytes against an expected digest, without raising.

    Missing files, directories, and read failures are reported on the result
    (``matched=False`` with a sanitized ``error``) so callers can keep
    verifying the remaining files.
    """
    algorithm = _require_algorithm(algorithm)
    validate_expected_checksum(expected)
    if not path.exists():
        return ChecksumVerificationResult(
            algorithm=algorithm,
            expected=expected,
            actual=None,
            matched=False,
            bytes_read=0,
            name=path.name,
            error="file not found",
        )
    if path.is_dir():
        return ChecksumVerificationResult(
            algorithm=algorithm,
            expected=expected,
            actual=None,
            matched=False,
            bytes_read=0,
            name=path.name,
            error="path is a directory",
        )
    try:
        actual, bytes_read = _digest_file(path, chunk_size)
    except OSError as error:
        return ChecksumVerificationResult(
            algorithm=algorithm,
            expected=expected,
            actual=None,
            matched=False,
            bytes_read=0,
            name=path.name,
            error=f"cannot read file ({type(error).__name__})",
        )
    return ChecksumVerificationResult(
        algorithm=algorithm,
        expected=expected,
        actual=actual,
        matched=actual == expected,
        bytes_read=bytes_read,
        name=path.name,
    )


@dataclass(frozen=True, kw_only=True)
class FileChecksumReport:
    """Per-file outcome inside a dataset directory verification."""

    name: str
    status: FileStatus
    expected: str | None
    actual: str | None
    bytes_read: int
    error: str | None = None


@dataclass(frozen=True, kw_only=True)
class DatasetChecksumReport:
    """Deterministic report for one dataset directory verification."""

    status: OverallStatus
    files: tuple[FileChecksumReport, ...]
    missing: tuple[str, ...]
    unexpected: tuple[str, ...]
    bytes_read: int


def verify_dataset_directory(
    dataset_dir: Path, expected_files: Mapping[str, str]
) -> DatasetChecksumReport:
    """Verify every expected file in ``dataset_dir`` and report all outcomes.

    Expected checksums are validated before any read.  Report status priority
    is deterministic: ``invalid`` > ``missing`` > ``read_failure`` > ``mismatch``
    > ``verified``.  A read failure counts as a failure -- the file's digest was
    never produced, so the dataset is not fully verified.  A single failure
    never discards the results of the other files.  File names are sorted; the
    scan is non-recursive and never follows a symlink that escapes the dataset
    directory.
    """
    if not isinstance(expected_files, Mapping) or not expected_files:
        raise ValueError("expected_files must be a non-empty mapping of name to checksum")
    validated: list[tuple[str, str]] = []
    for name, checksum in expected_files.items():
        if not isinstance(name, str) or not name:
            raise ValueError("expected file names must be non-empty strings")
        if name in {".", ".."} or "/" in name or "\\" in name:
            raise ValueError(f"invalid expected file name {name!r}")
        validate_expected_checksum(checksum)
        validated.append((name, checksum))
    validated.sort(key=lambda item: item[0])
    if not dataset_dir.exists() or not dataset_dir.is_dir():
        raise ValueError("dataset directory must exist and be a directory")
    root = dataset_dir.resolve()

    reports = [
        _verify_one(root, dataset_dir / name, name, checksum) for name, checksum in validated
    ]
    unexpected = sorted(
        entry.name
        for entry in dataset_dir.iterdir()
        if entry.is_file() and entry.name not in expected_files and entry.name != CHECKSUM_FILE_NAME
    )
    return DatasetChecksumReport(
        status=_overall_status(reports),
        files=tuple(reports),
        missing=tuple(report.name for report in reports if report.status == "missing"),
        unexpected=tuple(unexpected),
        bytes_read=sum(report.bytes_read for report in reports),
    )


def _verify_one(root: Path, path: Path, name: str, checksum: str) -> FileChecksumReport:
    if not path.exists():
        return FileChecksumReport(
            name=name,
            status="missing",
            expected=checksum,
            actual=None,
            bytes_read=0,
            error="file not found",
        )
    try:
        resolved = path.resolve()
    except OSError as error:
        return FileChecksumReport(
            name=name,
            status="invalid",
            expected=checksum,
            actual=None,
            bytes_read=0,
            error=f"cannot resolve path ({type(error).__name__})",
        )
    if not _is_within(root, resolved):
        return FileChecksumReport(
            name=name,
            status="invalid",
            expected=checksum,
            actual=None,
            bytes_read=0,
            error="path escapes the dataset directory",
        )
    if resolved.is_dir():
        return FileChecksumReport(
            name=name,
            status="invalid",
            expected=checksum,
            actual=None,
            bytes_read=0,
            error="path is a directory",
        )
    try:
        actual, bytes_read = _digest_file(resolved, CHUNK_SIZE)
    except OSError as error:
        return FileChecksumReport(
            name=name,
            status="read_failure",
            expected=checksum,
            actual=None,
            bytes_read=0,
            error=f"cannot read file ({type(error).__name__})",
        )
    matched = actual == checksum
    return FileChecksumReport(
        name=name,
        status="verified" if matched else "mismatch",
        expected=checksum,
        actual=actual,
        bytes_read=bytes_read,
    )


def _is_within(root: Path, candidate: Path) -> bool:
    try:
        candidate.relative_to(root)
        return True
    except ValueError:
        return False


def _overall_status(reports: list[FileChecksumReport]) -> OverallStatus:
    """Derive the dataset-level status with fixed failure priority.

    Priority: ``invalid`` > ``missing`` > ``read_failure`` > ``mismatch`` >
    ``verified``.  A read failure is a real failure -- it never falls back to
    ``verified`` because the file's digest was not produced, so a dataset with
    any unreadable expected file is NOT fully verified.
    """
    statuses = {report.status for report in reports}
    if "invalid" in statuses:
        return "invalid"
    if "missing" in statuses:
        return "missing"
    if "read_failure" in statuses:
        return "read_failure"
    if "mismatch" in statuses:
        return "mismatch"
    return "verified"
