"""DATA-005 C3B-2C/4 durable one-pass staged conversion.

This package-private slice consumes one raw archive stream, durably materializes
the already-owned staging pair, and returns metadata. It never promotes,
removes, truncates, reopens for hashing, or writes a manifest.
"""

from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Any, BinaryIO, Literal, NoReturn, cast

import packages.market_data.datasets._publication_fs as _fs
import packages.market_data.datasets._publication_staging as _staging
from packages.market_data.datasets.conversion_manifest import (
    ResearchFileArtifact,
    ResearchFilePlan,
    ResearchManifestValidationError,
)
from packages.market_data.datasets.conversion_stream import (
    ConversionStreamError,
    StreamConversionReport,
    convert_raw_archive_stream,
)
from packages.market_data.datasets.converter import RawConversionContext
from packages.market_data.datasets.publication_layout import ResearchPublicationLayout

StagedConversionOperation = Literal[
    "validate_input",
    "inspect_raw",
    "open_raw",
    "convert",
    "flush_research",
    "flush_failure",
    "fsync_research",
    "fsync_failure",
    "close",
    "verify_staged",
    "build_artifact",
]
StagedConversionCategory = Literal[
    "invalid_contract",
    "unsafe_filesystem",
    "io_failure",
    "conversion_failure",
    "verification_mismatch",
    "concurrent_change",
]
_Failure = tuple[StagedConversionOperation, StagedConversionCategory]
_ERROR_MESSAGES: MappingProxyType[_Failure, str] = MappingProxyType(
    {
        ("validate_input", "invalid_contract"): "conversion input contract is invalid",
        ("inspect_raw", "unsafe_filesystem"): "raw archive filesystem is unsafe",
        ("inspect_raw", "io_failure"): "raw archive inspection failed",
        ("inspect_raw", "concurrent_change"): "raw archive changed concurrently",
        ("open_raw", "unsafe_filesystem"): "raw archive descriptor is unsafe",
        ("open_raw", "io_failure"): "raw archive open failed",
        ("open_raw", "concurrent_change"): "raw archive changed while opening",
        ("convert", "conversion_failure"): "stream conversion failed",
        ("flush_research", "io_failure"): "research stream flush failed",
        ("flush_failure", "io_failure"): "failure stream flush failed",
        ("fsync_research", "io_failure"): "research descriptor fsync failed",
        ("fsync_failure", "io_failure"): "failure descriptor fsync failed",
        ("close", "io_failure"): "stream close failed",
        ("verify_staged", "unsafe_filesystem"): "staged output filesystem is unsafe",
        ("verify_staged", "verification_mismatch"): "staged output verification failed",
        ("verify_staged", "io_failure"): "staged output inspection failed",
        ("verify_staged", "concurrent_change"): "staged output changed concurrently",
        ("build_artifact", "invalid_contract"): "artifact construction failed",
    },
)

_INVALID_ERROR_CONTRACT = "invalid staged conversion error contract"


class StagedConversionError(RuntimeError):
    """Sanitized package-internal staged-conversion failure."""

    __slots__ = ("category", "operation")
    operation: StagedConversionOperation
    category: StagedConversionCategory

    def __init__(
        self,
        *,
        operation: StagedConversionOperation,
        category: StagedConversionCategory,
    ) -> None:
        if type(operation) is not str or type(category) is not str:
            raise ValueError(_INVALID_ERROR_CONTRACT)
        message = _ERROR_MESSAGES.get((operation, category))
        if message is None:
            raise ValueError(_INVALID_ERROR_CONTRACT)
        self.operation = operation
        self.category = category
        super().__init__(message)


def _raise_failure(failure: _Failure) -> NoReturn:
    raise StagedConversionError(operation=failure[0], category=failure[1])


def _require_inputs(
    layout: object,
    plan: object,
    context: object,
    pair: object,
) -> tuple[
    ResearchPublicationLayout,
    ResearchFilePlan,
    RawConversionContext,
    _staging.OwnedStagingPair,
]:
    invalid = (
        type(layout) is not ResearchPublicationLayout
        or type(plan) is not ResearchFilePlan
        or type(context) is not RawConversionContext
        or type(pair) is not _staging.OwnedStagingPair
    )
    if invalid:
        _raise_failure(("validate_input", "invalid_contract"))
    exact_layout = cast(ResearchPublicationLayout, layout)
    exact_plan = cast(ResearchFilePlan, plan)
    exact_context = cast(RawConversionContext, context)
    exact_pair = cast(_staging.OwnedStagingPair, pair)
    if (
        exact_pair.closed
        or exact_pair.paths != exact_layout.artifact_paths_for(exact_plan)
        or exact_context.file_name != exact_plan.raw_name
        or exact_context.symbol != exact_plan.symbol
        or exact_context.interval != exact_plan.interval
    ):
        _raise_failure(("validate_input", "invalid_contract"))
    return exact_layout, exact_plan, exact_context, exact_pair


def _identities_disagree(
    first: tuple[int, int] | None,
    second: tuple[int, int] | None,
) -> bool:
    return first is not None and second is not None and first != second


def _staging_failure(
    error: _staging.StagingPreparationError,
    *,
    operation: StagedConversionOperation,
) -> _Failure:
    if error.category == "io_failure":
        return operation, "io_failure"
    if error.category == "unsafe_filesystem":
        return operation, "unsafe_filesystem"
    return operation, "concurrent_change"


def _inspect_raw(
    layout: ResearchPublicationLayout,
    pair: _staging.OwnedStagingPair,
    *,
    baseline: _staging._CoreInspection | None,
) -> _staging._CoreInspection:
    inspected: _staging._CoreInspection | None = None
    failure: _Failure | None = None
    try:
        inspected = _staging._inspect_core(
            layout,
            pair.paths,
            operation="verify_staging",
            baseline=baseline,
        )
    except _staging.StagingPreparationError as error:
        failure = _staging_failure(error, operation="inspect_raw")
    if failure is not None:
        _raise_failure(failure)
    if inspected is None:
        _raise_failure(("inspect_raw", "io_failure"))
    return inspected


def _open_raw(path: Path) -> BinaryIO:
    stream: BinaryIO | None = None
    failed = False
    try:
        stream = open(path, "rb")
    except OSError:
        failed = True
    if failed or stream is None:
        _raise_failure(("open_raw", "io_failure"))
    return stream


def _descriptor_metadata(
    stream: BinaryIO,
    *,
    operation: StagedConversionOperation,
) -> os.stat_result:
    metadata: os.stat_result | None = None
    failed = False
    try:
        descriptor = stream.fileno()
        if type(descriptor) is not int or descriptor < 0:
            failed = True
        else:
            metadata = os.fstat(descriptor)
    except (OSError, TypeError, ValueError):
        failed = True
    if failed or metadata is None:
        _raise_failure((operation, "io_failure"))
    return metadata


def _require_raw_snapshot(
    descriptor: os.stat_result,
    path_entry: _fs._InspectedEntry,
    *,
    captured_identity: tuple[int, int] | None,
    captured_size: int,
    operation: StagedConversionOperation,
) -> None:
    if not stat.S_ISREG(descriptor.st_mode):
        _raise_failure((operation, "unsafe_filesystem"))
    descriptor_identity = _fs._physical_file_identity(descriptor)
    path_identity = _fs._physical_file_identity(path_entry.metadata)
    if (
        descriptor.st_size != captured_size
        or path_entry.metadata.st_size != captured_size
        or _identities_disagree(descriptor_identity, path_identity)
        or _identities_disagree(descriptor_identity, captured_identity)
        or _identities_disagree(path_identity, captured_identity)
    ):
        _raise_failure((operation, "concurrent_change"))


def _raw_metadata_values(metadata: os.stat_result) -> tuple[int, int, int]:
    size: object = None
    mtime_ns: object = None
    ctime_ns: object = None
    inspection_failed = False
    try:
        size = metadata.st_size
        mtime_ns = metadata.st_mtime_ns
        ctime_ns = metadata.st_ctime_ns
    except (AttributeError, OSError, TypeError, ValueError):
        inspection_failed = True

    if (
        inspection_failed
        or type(size) is not int
        or type(mtime_ns) is not int
        or type(ctime_ns) is not int
    ):
        _raise_failure(("inspect_raw", "io_failure"))
    return size, mtime_ns, ctime_ns


@dataclass(frozen=True, slots=True)
class _RawFingerprint:
    """Exact observable metadata captured from one bound raw file."""

    identity: tuple[int, int] | None
    size: int
    descriptor_mtime_ns: int
    descriptor_ctime_ns: int
    path_mtime_ns: int
    path_ctime_ns: int

    @classmethod
    def capture(
        cls,
        descriptor: os.stat_result,
        path_entry: _fs._InspectedEntry,
    ) -> _RawFingerprint:
        descriptor_values = _raw_metadata_values(descriptor)
        path_values = _raw_metadata_values(path_entry.metadata)
        descriptor_identity = _fs._physical_file_identity(descriptor)
        path_identity = _fs._physical_file_identity(path_entry.metadata)
        if descriptor_values[0] != path_values[0] or _identities_disagree(
            descriptor_identity,
            path_identity,
        ):
            _raise_failure(("inspect_raw", "concurrent_change"))
        return cls(
            identity=(descriptor_identity if descriptor_identity is not None else path_identity),
            size=descriptor_values[0],
            descriptor_mtime_ns=descriptor_values[1],
            descriptor_ctime_ns=descriptor_values[2],
            path_mtime_ns=path_values[1],
            path_ctime_ns=path_values[2],
        )

    def require_descriptor_and_path(
        self,
        descriptor: os.stat_result,
        path_entry: _fs._InspectedEntry,
    ) -> None:
        descriptor_values = _raw_metadata_values(descriptor)
        path_values = _raw_metadata_values(path_entry.metadata)
        expected_descriptor = (
            self.size,
            self.descriptor_mtime_ns,
            self.descriptor_ctime_ns,
        )
        expected_path = self.size, self.path_mtime_ns, self.path_ctime_ns
        descriptor_identity = _fs._physical_file_identity(descriptor)
        path_identity = _fs._physical_file_identity(path_entry.metadata)
        if (
            descriptor_values != expected_descriptor
            or path_values != expected_path
            or _identities_disagree(descriptor_identity, path_identity)
            or _identities_disagree(descriptor_identity, self.identity)
            or _identities_disagree(path_identity, self.identity)
        ):
            _raise_failure(("inspect_raw", "concurrent_change"))

    def require_path(self, path_entry: _fs._InspectedEntry) -> None:
        path_values = _raw_metadata_values(path_entry.metadata)
        path_identity = _fs._physical_file_identity(path_entry.metadata)
        if path_values != (
            self.size,
            self.path_mtime_ns,
            self.path_ctime_ns,
        ) or _identities_disagree(
            path_identity,
            self.identity,
        ):
            _raise_failure(("inspect_raw", "concurrent_change"))


@dataclass(slots=True)
class _HashingReader:
    raw: BinaryIO
    digest: Any
    byte_count: int = 0

    def readline(self, size: int) -> bytes:
        chunk = self.raw.readline(size)
        if type(chunk) is not bytes or len(chunk) > size:
            raise ValueError("raw readline contract violated")
        if chunk:
            self.digest.update(chunk)
            self.byte_count += len(chunk)
        return chunk


def _jit_validate(
    layout: ResearchPublicationLayout,
    plan: ResearchFilePlan,
    pair: _staging.OwnedStagingPair,
) -> None:
    failure: _Failure | None = None
    try:
        _staging._revalidate_owned_staging_pair(layout, plan, pair)
    except _staging.StagingPreparationError as error:
        failure = _staging_failure(error, operation="verify_staged")
    if failure is not None:
        _raise_failure(failure)


def _flush(stream: BinaryIO, *, operation: StagedConversionOperation) -> None:
    failed = False
    try:
        stream.flush()
    except (OSError, ValueError):
        failed = True
    if failed:
        _raise_failure((operation, "io_failure"))


def _verify_output_descriptor(
    stream: BinaryIO,
    *,
    expected_size: int,
    expected_identity: tuple[int, int] | None,
) -> int:
    metadata = _descriptor_metadata(stream, operation="verify_staged")
    if not stat.S_ISREG(metadata.st_mode):
        _raise_failure(("verify_staged", "unsafe_filesystem"))
    if metadata.st_size != expected_size or _identities_disagree(
        _fs._physical_file_identity(metadata), expected_identity
    ):
        _raise_failure(("verify_staged", "verification_mismatch"))
    descriptor = stream.fileno()
    if type(descriptor) is not int or descriptor < 0:
        _raise_failure(("verify_staged", "io_failure"))
    return descriptor


def _fsync(descriptor: int, *, operation: StagedConversionOperation) -> None:
    failed = False
    try:
        os.fsync(descriptor)
    except OSError:
        failed = True
    if failed:
        _raise_failure((operation, "io_failure"))


def _attempt_close(streams: tuple[BinaryIO, ...]) -> bool:
    failed = False
    for stream in streams:
        try:
            stream.close()
        except (OSError, RuntimeError, ValueError):
            failed = True
    for stream in streams:
        try:
            if type(stream.closed) is not bool or not stream.closed:
                failed = True
        except (AttributeError, OSError, RuntimeError, ValueError):
            failed = True
    return failed


def _post_close_stage(
    path: Path,
    *,
    expected_parent: Path,
    expected_size: int,
    expected_identity: tuple[int, int] | None,
) -> None:
    entry: _fs._InspectedEntry | None = None
    failure: _Failure | None = None
    try:
        entry = _fs._inspect_required_entry(path, field="staging", kind="file")
        _fs._require_direct_parent(entry.resolved, expected_parent=expected_parent)
    except _fs.PhysicalInspectionError:
        failure = "verify_staged", "io_failure"
    if failure is not None:
        _raise_failure(failure)
    if entry is None:
        _raise_failure(("verify_staged", "io_failure"))
    if entry.metadata.st_size != expected_size or _identities_disagree(
        _fs._physical_file_identity(entry.metadata), expected_identity
    ):
        _raise_failure(("verify_staged", "verification_mismatch"))


def _require_publication_targets_absent(
    layout: ResearchPublicationLayout,
    pair: _staging.OwnedStagingPair,
) -> None:
    failure: _Failure | None = None
    try:
        inspected = _staging._inspect_core(
            layout,
            pair.paths,
            operation="verify_staging",
            baseline=None,
        )
        for target, parent in (
            (pair.paths.research_path, inspected.research_dir),
            (pair.paths.failure_path, inspected.failure_dir),
            (layout.research_manifest_path, inspected.output_root),
            (layout.staging_manifest_path, inspected.staging_dir),
        ):
            _staging._require_target_absent(
                target,
                expected_parent=parent,
                operation="verify_staging",
                changed=True,
            )
    except _staging.StagingPreparationError as error:
        failure = _staging_failure(error, operation="verify_staged")
    if failure is not None:
        _raise_failure(failure)


def _build_artifact(
    plan: ResearchFilePlan,
    reader: _HashingReader,
    report: StreamConversionReport,
) -> ResearchFileArtifact:
    artifact: ResearchFileArtifact | None = None
    failed = False
    try:
        artifact = ResearchFileArtifact.from_stream_report(
            raw_name=plan.raw_name,
            research_name=plan.research_name,
            failure_name=plan.failure_name,
            symbol=plan.symbol,
            interval=plan.interval,
            raw_sha256=reader.digest.hexdigest(),
            raw_bytes=reader.byte_count,
            report=report,
        )
    except (ResearchManifestValidationError, TypeError, ValueError):
        failed = True
    if failed or artifact is None:
        _raise_failure(("build_artifact", "invalid_contract"))
    return artifact


def materialize_durable_staged_artifact(
    layout: ResearchPublicationLayout,
    *,
    plan: ResearchFilePlan,
    context: RawConversionContext,
    pair: _staging.OwnedStagingPair,
) -> ResearchFileArtifact:
    """Materialize one verified durable staging pair from one raw pass."""
    exact_layout, exact_plan, exact_context, exact_pair = _require_inputs(
        layout,
        plan,
        context,
        pair,
    )
    initial = _inspect_raw(exact_layout, exact_pair, baseline=None)
    captured_identity = _fs._physical_file_identity(initial.raw_artifact.metadata)
    captured_size = _fs._require_file_size(initial.raw_artifact.metadata)

    # Ownership boundary starts immediately after successful raw open.
    # All fallible operations below are inside the try/finally.
    raw_stream = _open_raw(initial.raw_artifact.resolved)

    pending: _Failure | None = None
    report: StreamConversionReport | None = None
    reader: _HashingReader | None = None
    raw_fingerprint: _RawFingerprint | None = None
    close_failed = False

    try:
        current = _inspect_raw(exact_layout, exact_pair, baseline=initial)
        raw_metadata = _descriptor_metadata(raw_stream, operation="open_raw")
        _require_raw_snapshot(
            raw_metadata,
            current.raw_artifact,
            captured_identity=captured_identity,
            captured_size=captured_size,
            operation="open_raw",
        )
        raw_fingerprint = _RawFingerprint.capture(
            raw_metadata,
            current.raw_artifact,
        )

        reader = _HashingReader(raw=raw_stream, digest=sha256())
        _jit_validate(exact_layout, exact_plan, exact_pair)

        try:
            report = convert_raw_archive_stream(
                cast(BinaryIO, reader),
                exact_pair.research_stream,
                exact_pair.failure_stream,
                context=exact_context,
            )
        except ConversionStreamError:
            pending = "convert", "conversion_failure"

        if pending is None and report is not None:
            if reader is None or raw_fingerprint is None:
                _raise_failure(("convert", "conversion_failure"))
            if reader.byte_count != captured_size:
                _raise_failure(("inspect_raw", "concurrent_change"))

            # Re-inspect after conversion but before output durability.
            current = _inspect_raw(exact_layout, exact_pair, baseline=initial)
            raw_metadata = _descriptor_metadata(raw_stream, operation="inspect_raw")
            _require_raw_snapshot(
                raw_metadata,
                current.raw_artifact,
                captured_identity=captured_identity,
                captured_size=captured_size,
                operation="inspect_raw",
            )
            raw_fingerprint.require_descriptor_and_path(
                raw_metadata,
                current.raw_artifact,
            )

            _flush(exact_pair.research_stream, operation="flush_research")
            research_fd = _verify_output_descriptor(
                exact_pair.research_stream,
                expected_size=report.research_bytes,
                expected_identity=exact_pair.research_identity,
            )
            _fsync(research_fd, operation="fsync_research")

            _flush(exact_pair.failure_stream, operation="flush_failure")
            failure_fd = _verify_output_descriptor(
                exact_pair.failure_stream,
                expected_size=report.failure_bytes,
                expected_identity=exact_pair.failure_identity,
            )
            _fsync(failure_fd, operation="fsync_failure")

            current = _inspect_raw(exact_layout, exact_pair, baseline=initial)
            post_durability_metadata = _descriptor_metadata(
                raw_stream,
                operation="inspect_raw",
            )
            raw_fingerprint.require_descriptor_and_path(
                post_durability_metadata,
                current.raw_artifact,
            )

    except StagedConversionError as error:
        pending = error.operation, error.category

    finally:
        close_failed = _attempt_close(
            (raw_stream, exact_pair.research_stream, exact_pair.failure_stream),
        )
        exact_pair.closed = _staging._stream_is_closed(
            exact_pair.research_stream,
        ) and _staging._stream_is_closed(exact_pair.failure_stream)

    if close_failed:
        _raise_failure(("close", "io_failure"))
    if pending is not None:
        _raise_failure(pending)
    if report is None or reader is None or raw_fingerprint is None:
        _raise_failure(("convert", "conversion_failure"))

    _post_close_stage(
        exact_pair.paths.staging_research_path,
        expected_parent=exact_layout.staging_research_dir,
        expected_size=report.research_bytes,
        expected_identity=exact_pair.research_identity,
    )
    _post_close_stage(
        exact_pair.paths.staging_failure_path,
        expected_parent=exact_layout.staging_failure_dir,
        expected_size=report.failure_bytes,
        expected_identity=exact_pair.failure_identity,
    )
    post_close_raw = _inspect_raw(exact_layout, exact_pair, baseline=initial)
    raw_fingerprint.require_path(post_close_raw.raw_artifact)
    _require_publication_targets_absent(exact_layout, exact_pair)
    final_raw = _inspect_raw(exact_layout, exact_pair, baseline=initial)
    raw_fingerprint.require_path(final_raw.raw_artifact)

    return _build_artifact(exact_plan, reader, report)
