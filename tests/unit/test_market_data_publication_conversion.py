"""DATA-005 C3B-2C durable one-pass staged-conversion tests."""

from __future__ import annotations

import ast
import builtins
import json
import os
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, cast

import pytest

import packages.market_data.datasets._publication_conversion as conversion_module
import packages.market_data.datasets._publication_fs as fs_module
import packages.market_data.datasets._publication_staging as staging_module
from packages.market_data.datasets._publication_conversion import (
    StagedConversionError,
    materialize_durable_staged_artifact,
)
from packages.market_data.datasets._publication_staging import (
    OwnedStagingPair,
    prepare_exclusive_staging_pair,
)
from packages.market_data.datasets.conversion_manifest import (
    ResearchFileArtifact,
    ResearchFilePlan,
)
from packages.market_data.datasets.conversion_stream import ConversionStreamError
from packages.market_data.datasets.converter import RawConversionContext
from packages.market_data.datasets.downloader import MANIFEST_FILE
from packages.market_data.datasets.publication_layout import ResearchPublicationLayout

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_OPEN_MS = 1_704_067_200_000
_CLOSE_MS = 1_704_067_260_000
_FEBRUARY_OPEN_MS = 1_706_745_600_000
_MARCH_OPEN_MS = 1_709_251_200_000
_PRIVATE_MARKER = "C3B2C_PRIVATE_MARKER_DO_NOT_LEAK"
_UNCHANGED = object()


def _iso(epoch_ms: int) -> str:
    return (_EPOCH + timedelta(milliseconds=epoch_ms)).isoformat()


def _payload(*, open_ms: int, close_ms: int) -> list[object]:
    return [
        open_ms,
        "50000.00",
        "50001.00",
        "49999.00",
        "50000.50",
        "1.5",
        close_ms - 1,
        "75000.75",
        100,
        "0.75",
        "37500.00",
        "0",
    ]


def _line(*, interval: str = "1m", ending: bytes = b"\n") -> bytes:
    open_ms = _OPEN_MS if interval == "1m" else _FEBRUARY_OPEN_MS
    close_ms = _CLOSE_MS if interval == "1m" else _MARCH_OPEN_MS
    record = {
        "symbol": "BTC/USDT",
        "interval": interval,
        "open_time": _iso(open_ms),
        "close_time": _iso(close_ms),
        "source": "binance_public_rest",
        "payload": _payload(open_ms=open_ms, close_ms=close_ms),
    }
    return json.dumps(record, separators=(",", ":")).encode() + ending


@dataclass(frozen=True)
class _Case:
    layout: ResearchPublicationLayout
    plan: ResearchFilePlan
    context: RawConversionContext


def _make_case(root: Path, raw: bytes = b"", *, interval: str = "1m") -> _Case:
    raw_dir = root / "raw"
    output_dir = root / "output"
    raw_dir.mkdir(parents=True)
    output_dir.mkdir()
    (raw_dir / MANIFEST_FILE).write_bytes(b"raw manifest")
    name = f"BTC-USDT-{interval}.jsonl"
    plan = ResearchFilePlan.from_raw_identity(
        raw_name=name,
        symbol="BTC/USDT",
        interval=interval,
    )
    (raw_dir / name).write_bytes(raw)
    if interval == "1M":
        range_start = datetime(2024, 2, 1, tzinfo=UTC)
        range_end = datetime(2024, 3, 1, tzinfo=UTC)
    else:
        range_start = datetime(2024, 1, 1, tzinfo=UTC)
        range_end = datetime(2024, 1, 2, tzinfo=UTC)
    return _Case(
        layout=ResearchPublicationLayout(raw_dir=raw_dir, output_dir=output_dir),
        plan=plan,
        context=RawConversionContext(
            file_name=name,
            symbol="BTC/USDT",
            interval=interval,
            range_start=range_start,
            range_end=range_end,
        ),
    )


def _prepare(case: _Case) -> OwnedStagingPair:
    return prepare_exclusive_staging_pair(case.layout, plan=case.plan)


def _close_directly(pair: OwnedStagingPair) -> None:
    for stream in (pair.research_stream, pair.failure_stream):
        try:
            stream.close()
        except (OSError, RuntimeError, ValueError):
            pass


def _run(case: _Case, pair: OwnedStagingPair) -> ResearchFileArtifact:
    return materialize_durable_staged_artifact(
        case.layout,
        plan=case.plan,
        context=case.context,
        pair=pair,
    )


def _assert_error(
    raised: pytest.ExceptionInfo[StagedConversionError],
    operation: str,
    category: str,
) -> StagedConversionError:
    error = raised.value
    assert error.operation == operation
    assert error.category == category
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert _PRIVATE_MARKER not in rendered
    return error


def _assert_closed(pair: OwnedStagingPair) -> None:
    assert pair.research_stream.closed
    assert pair.failure_stream.closed
    assert pair.closed is True


def test_empty_raw_returns_durable_zero_byte_artifact(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    pair = _prepare(case)
    artifact = _run(case, pair)
    empty_digest = sha256(b"").hexdigest()
    assert type(artifact) is ResearchFileArtifact
    assert artifact.raw_sha256 == empty_digest
    assert artifact.research_sha256 == empty_digest
    assert artifact.failure_sha256 == empty_digest
    assert (
        artifact.raw_bytes,
        artifact.lines_seen,
        artifact.records_written,
        artifact.records_quarantined,
        artifact.research_bytes,
        artifact.failure_bytes,
    ) == (0, 0, 0, 0, 0, 0)
    assert pair.paths.staging_research_path.read_bytes() == b""
    assert pair.paths.staging_failure_path.read_bytes() == b""
    _assert_closed(pair)


@pytest.mark.parametrize(
    ("interval", "raw"),
    [
        ("1m", _line()),
        ("1M", _line(interval="1M")),
        ("1m", b"ordinary-invalid\n"),
    ],
)
def test_fixed_monthly_and_quarantined_paths(
    tmp_path: Path,
    interval: str,
    raw: bytes,
) -> None:
    case = _make_case(tmp_path, raw, interval=interval)
    pair = _prepare(case)
    artifact = _run(case, pair)
    assert artifact.raw_sha256 == sha256(raw).hexdigest()
    assert artifact.raw_bytes == len(raw)
    assert artifact.lines_seen == 1
    if raw.startswith(b"ordinary"):
        assert artifact.records_quarantined == 1
        assert artifact.status == "partial"
        assert pair.paths.staging_failure_path.stat().st_size == artifact.failure_bytes
    else:
        assert artifact.records_written == 1
        assert artifact.status == "success"
        assert pair.paths.staging_research_path.stat().st_size == artifact.research_bytes
    _assert_closed(pair)


@pytest.mark.parametrize("ending", [b"\n", b"\r\n", b""])
def test_exact_raw_digest_includes_line_ending(tmp_path: Path, ending: bytes) -> None:
    raw = _line(ending=ending)
    case = _make_case(tmp_path, raw)
    artifact = _run(case, _prepare(case))
    assert artifact.raw_sha256 == sha256(raw).hexdigest()
    assert artifact.raw_bytes == len(raw)


class _TrackedRaw:
    def __init__(self, raw: BinaryIO) -> None:
        self.raw = raw
        self.readline_sizes: list[int] = []
        self.close_calls = 0

    def readline(self, size: int) -> bytes:
        self.readline_sizes.append(size)
        return self.raw.readline(size)

    def read(self, *_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("whole-file read forbidden")

    readall = read
    readlines = read
    seek = read

    def __getattr__(self, name: str) -> object:
        return getattr(self.raw, name)

    @property
    def closed(self) -> bool:
        return self.raw.closed

    def close(self) -> None:
        self.close_calls += 1
        self.raw.close()


def _track_raw_open(
    monkeypatch: pytest.MonkeyPatch,
    pair: OwnedStagingPair,
) -> list[_TrackedRaw]:
    original_open = builtins.open
    tracked: list[_TrackedRaw] = []

    def replacement(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        stream = original_open(file, mode, *args, **kwargs)
        if Path(file) == pair.paths.raw_path and mode == "rb":
            wrapper = _TrackedRaw(cast(BinaryIO, stream))
            tracked.append(wrapper)
            return wrapper
        return stream

    monkeypatch.setattr(builtins, "open", replacement)
    return tracked


class _FailingRaw(_TrackedRaw):
    def __init__(self, raw: BinaryIO, *, failure: str) -> None:
        super().__init__(raw)
        self.failure = failure

    def readline(self, size: int) -> bytes:
        if self.failure == "read":
            raise OSError(5, _PRIVATE_MARKER)
        return super().readline(size)

    def close(self) -> None:
        super().close()
        if self.failure == "close":
            raise OSError(5, _PRIVATE_MARKER)


def test_exactly_one_bounded_raw_pass_and_no_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _line() + b"ordinary-invalid\n"
    case = _make_case(tmp_path, raw)
    pair = _prepare(case)
    original_open = builtins.open
    original_convert = conversion_module.convert_raw_archive_stream
    tracked: list[_TrackedRaw] = []
    convert_calls = 0

    def open_replacement(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        stream = original_open(file, mode, *args, **kwargs)
        if Path(file) == pair.paths.raw_path and mode == "rb":
            wrapper = _TrackedRaw(cast(BinaryIO, stream))
            tracked.append(wrapper)
            return wrapper
        return stream

    def convert_replacement(*args: object, **kwargs: object) -> object:
        nonlocal convert_calls
        convert_calls += 1
        return original_convert(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(builtins, "open", open_replacement)
    monkeypatch.setattr(conversion_module, "convert_raw_archive_stream", convert_replacement)
    artifact = _run(case, pair)
    assert artifact.raw_bytes == len(raw)
    assert convert_calls == 1
    assert len(tracked) == 1
    assert tracked[0].readline_sizes
    assert set(tracked[0].readline_sizes) == {case.context.max_line_bytes + 1}
    assert tracked[0].close_calls == 1


def test_short_eof_cannot_return_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original_open = builtins.open

    class ShortEof(_TrackedRaw):
        def readline(self, size: int) -> bytes:
            self.readline_sizes.append(size)
            return b""

    def replacement(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        stream = original_open(file, mode, *args, **kwargs)
        if Path(file) == pair.paths.raw_path and mode == "rb":
            return ShortEof(cast(BinaryIO, stream))
        return stream

    monkeypatch.setattr(builtins, "open", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "inspect_raw", "concurrent_change")
    _assert_closed(pair)


@pytest.mark.parametrize(
    ("failure", "operation", "category"),
    [
        ("read", "convert", "conversion_failure"),
        ("close", "close", "io_failure"),
    ],
)
def test_raw_read_and_close_failures_close_all_owned_handles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    operation: str,
    category: str,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original_open = builtins.open
    wrappers: list[_FailingRaw] = []

    def replacement(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        stream = original_open(file, mode, *args, **kwargs)
        if Path(file) == pair.paths.raw_path and mode == "rb":
            wrapper = _FailingRaw(cast(BinaryIO, stream), failure=failure)
            wrappers.append(wrapper)
            return wrapper
        return stream

    monkeypatch.setattr(builtins, "open", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, operation, category)
    assert wrappers[0].close_calls == 1
    _assert_closed(pair)


@pytest.mark.parametrize(
    ("target_name", "parent_name"),
    [
        ("research_path", "research_dir"),
        ("failure_path", "failure_dir"),
        ("research_manifest_path", "output_dir"),
        ("staging_manifest_path", "staging_dir"),
    ],
)
def test_jit_rejects_appearing_publication_target_without_payload_write(
    tmp_path: Path,
    target_name: str,
    parent_name: str,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    owner = pair.paths if hasattr(pair.paths, target_name) else case.layout
    target = cast(Path, getattr(owner, target_name))
    cast(Path, getattr(case.layout, parent_name)).mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"appeared")
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "verify_staged", "concurrent_change")
    assert pair.paths.staging_research_path.read_bytes() == b""
    assert pair.paths.staging_failure_path.read_bytes() == b""
    _assert_closed(pair)


def test_jit_rejects_nonzero_stage_before_first_write(tmp_path: Path) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    pair.research_stream.write(b"prior")
    pair.research_stream.flush()
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "verify_staged", "concurrent_change")
    assert pair.paths.staging_research_path.read_bytes() == b"prior"
    assert pair.paths.staging_failure_path.read_bytes() == b""
    _assert_closed(pair)


class _StatProxy:
    def __init__(
        self,
        metadata: os.stat_result,
        *,
        inode: object = _UNCHANGED,
        device: object = _UNCHANGED,
        mode: object = _UNCHANGED,
        size: object = _UNCHANGED,
        mtime_ns: object = _UNCHANGED,
        ctime_ns: object = _UNCHANGED,
        reparse: bool = False,
    ) -> None:
        self.st_mode = metadata.st_mode if mode is _UNCHANGED else mode
        self.st_size = metadata.st_size if size is _UNCHANGED else size
        self.st_mtime_ns = metadata.st_mtime_ns if mtime_ns is _UNCHANGED else mtime_ns
        self.st_ctime_ns = metadata.st_ctime_ns if ctime_ns is _UNCHANGED else ctime_ns
        self.st_dev = metadata.st_dev if device is _UNCHANGED else device
        self.st_ino = metadata.st_ino if inode is _UNCHANGED else inode
        attributes = getattr(metadata, "st_file_attributes", 0)
        self.st_file_attributes = attributes | (0x0400 if reparse else 0)


@pytest.mark.parametrize("stage", ["research", "failure"])
def test_jit_rejects_descriptor_path_identity_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    target_fd = (
        pair.research_stream.fileno() if stage == "research" else pair.failure_stream.fileno()
    )
    original = staging_module.os.fstat

    def replacement(fd: int) -> os.stat_result:
        metadata = original(fd)
        if fd == target_fd:
            return cast(os.stat_result, _StatProxy(metadata, inode=metadata.st_ino + 10))
        return metadata

    monkeypatch.setattr(staging_module.os, "fstat", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "verify_staged", "concurrent_change")
    assert pair.paths.staging_research_path.stat().st_size == 0
    assert pair.paths.staging_failure_path.stat().st_size == 0


def test_jit_rejects_stage_parent_redirection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original = fs_module._inspect_required_entry

    def replacement(path: Path, *, field: str, kind: object) -> object:
        entry = original(path, field=field, kind=kind)  # type: ignore[arg-type]
        if path == pair.paths.staging_research_path:
            return fs_module._InspectedEntry(
                metadata=entry.metadata,
                resolved=tmp_path / "wrong-parent" / path.name,
            )
        return entry

    monkeypatch.setattr(fs_module, "_inspect_required_entry", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "verify_staged", "concurrent_change")
    assert pair.paths.staging_research_path.stat().st_size == 0


def test_jit_rejects_stage_reparse_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original = fs_module._lstat_or_missing

    def replacement(path: Path, *, field: str) -> os.stat_result | None:
        metadata = original(path, field=field)
        if path == pair.paths.staging_failure_path and metadata is not None:
            return cast(os.stat_result, _StatProxy(metadata, reparse=True))
        return metadata

    monkeypatch.setattr(fs_module, "_lstat_or_missing", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "verify_staged", "concurrent_change")
    assert pair.paths.staging_research_path.stat().st_size == 0
    assert pair.paths.staging_failure_path.stat().st_size == 0


def test_jit_rejects_device_mismatch_when_usable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original = fs_module._physical_device
    stage_identity = fs_module._physical_file_identity(case.layout.staging_research_dir.lstat())

    def replacement(metadata: os.stat_result) -> int | None:
        device = original(metadata)
        if fs_module._physical_file_identity(metadata) == stage_identity and device is not None:
            return device + 1
        return device

    monkeypatch.setattr(fs_module, "_physical_device", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "verify_staged", "concurrent_change")
    assert pair.paths.staging_research_path.stat().st_size == 0


def test_raw_manifest_hardlink_alias_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    pair = _prepare(case)
    pair.paths.raw_path.unlink()
    try:
        os.link(case.layout.raw_manifest_path, pair.paths.raw_path)
    except OSError:
        _close_directly(pair)
        pytest.skip("host filesystem does not permit hardlinks")
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "inspect_raw", "unsafe_filesystem")
    assert pair.closed is False
    _close_directly(pair)


def test_raw_replacement_between_inspect_and_open_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original = conversion_module._open_raw

    def replacement(path: Path) -> BinaryIO:
        path.unlink()
        path.write_bytes(_line())
        return original(path)

    monkeypatch.setattr(conversion_module, "_open_raw", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "open_raw", "concurrent_change")
    _assert_closed(pair)


def test_raw_descriptor_path_identity_mismatch_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original_open = builtins.open
    original_fstat = conversion_module.os.fstat
    raw_descriptors: set[int] = set()

    def open_replacement(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        stream = original_open(file, mode, *args, **kwargs)
        if Path(file) == pair.paths.raw_path and mode == "rb":
            raw_descriptors.add(stream.fileno())
        return stream

    def fstat_replacement(fd: int) -> os.stat_result:
        metadata = original_fstat(fd)
        if fd in raw_descriptors:
            return cast(os.stat_result, _StatProxy(metadata, inode=metadata.st_ino + 1))
        return metadata

    monkeypatch.setattr(builtins, "open", open_replacement)
    monkeypatch.setattr(conversion_module.os, "fstat", fstat_replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "open_raw", "concurrent_change")
    _assert_closed(pair)


@pytest.mark.parametrize("mutation", ["grow", "truncate", "replace"])
def test_raw_change_during_conversion_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    original = conversion_module.convert_raw_archive_stream
    original_inspect = conversion_module._inspect_raw
    replacement_observed = False

    def replacement(*args: object, **kwargs: object) -> object:
        nonlocal replacement_observed
        report = original(*args, **kwargs)  # type: ignore[arg-type]
        if mutation == "grow":
            with pair.paths.raw_path.open("ab") as stream:
                stream.write(b"x")
        elif mutation == "truncate":
            pair.paths.raw_path.write_bytes(b"")
        else:
            replacement_observed = True
        return report

    def inspect_replacement(*args: object, **kwargs: object) -> object:
        inspected = original_inspect(*args, **kwargs)
        if replacement_observed:
            metadata = inspected.raw_artifact.metadata
            return replace(
                inspected,
                raw_artifact=fs_module._InspectedEntry(
                    metadata=cast(
                        os.stat_result,
                        _StatProxy(metadata, inode=metadata.st_ino + 1),
                    ),
                    resolved=inspected.raw_artifact.resolved,
                ),
            )
        return inspected

    monkeypatch.setattr(conversion_module, "convert_raw_archive_stream", replacement)
    monkeypatch.setattr(conversion_module, "_inspect_raw", inspect_replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "inspect_raw", "concurrent_change")
    _assert_closed(pair)


def test_unusable_inode_metadata_does_not_fail_by_itself(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    monkeypatch.setattr(fs_module, "_physical_file_identity", lambda _metadata: None)
    artifact = _run(case, pair)
    assert artifact.records_written == 1
    _assert_closed(pair)


class _StreamProxy:
    def __init__(
        self,
        stream: BinaryIO,
        *,
        fail_operation: str | None = None,
    ) -> None:
        self.stream = stream
        self.fail_operation = fail_operation
        self.close_calls = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self.stream, name)

    @property
    def closed(self) -> bool:
        return self.stream.closed

    @property
    def mode(self) -> str:
        return self.stream.mode

    def write(self, payload: bytes) -> int:
        if self.fail_operation == "write":
            raise OSError(5, _PRIVATE_MARKER)
        return self.stream.write(payload)

    def flush(self) -> None:
        if self.fail_operation == "flush":
            raise OSError(5, _PRIVATE_MARKER)
        self.stream.flush()

    def close(self) -> None:
        self.close_calls += 1
        self.stream.close()
        if self.fail_operation == "close":
            raise OSError(5, _PRIVATE_MARKER)


@pytest.mark.parametrize(
    ("which", "method", "raw", "operation"),
    [
        ("research", "write", _line(), "convert"),
        ("failure", "write", b"invalid\n", "convert"),
        ("research", "flush", _line(), "flush_research"),
        ("failure", "flush", _line(), "flush_failure"),
        ("research", "close", _line(), "close"),
        ("failure", "close", _line(), "close"),
    ],
)
def test_stream_failure_injection_closes_every_handle_and_preserves_stages(
    tmp_path: Path,
    which: str,
    method: str,
    raw: bytes,
    operation: str,
) -> None:
    case = _make_case(tmp_path, raw)
    pair = _prepare(case)
    research = _StreamProxy(
        pair.research_stream,
        fail_operation=method if which == "research" else None,
    )
    failure = _StreamProxy(
        pair.failure_stream,
        fail_operation=method if which == "failure" else None,
    )
    pair.research_stream = research  # type: ignore[assignment]
    pair.failure_stream = failure  # type: ignore[assignment]
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    category = "conversion_failure" if operation == "convert" else "io_failure"
    _assert_error(raised, operation, category)
    assert research.close_calls == 1
    assert failure.close_calls == 1
    _assert_closed(pair)
    assert pair.paths.staging_research_path.exists()
    assert pair.paths.staging_failure_path.exists()


@pytest.mark.parametrize("which", ["research", "failure"])
def test_fsync_failure_is_detached_and_closes_all(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    which: str,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    target_fd = (
        pair.research_stream.fileno() if which == "research" else pair.failure_stream.fileno()
    )
    original = conversion_module.os.fsync

    def replacement(fd: int) -> None:
        if fd == target_fd:
            raise OSError(5, _PRIVATE_MARKER)
        original(fd)

    monkeypatch.setattr(conversion_module.os, "fsync", replacement)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, f"fsync_{which}", "io_failure")
    _assert_closed(pair)


def test_injected_conversion_stream_error_is_detached_and_closes_all(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)

    def fail(*_args: object, **_kwargs: object) -> object:
        raise ConversionStreamError("read_raw")

    monkeypatch.setattr(conversion_module, "convert_raw_archive_stream", fail)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "convert", "conversion_failure")
    _assert_closed(pair)


def test_raw_open_failure_is_detached_and_does_not_claim_pair_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    pair = _prepare(case)

    original_open = builtins.open

    def fail(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        if Path(file) == pair.paths.raw_path and mode == "rb":
            raise PermissionError(5, _PRIVATE_MARKER, str(file))
        return original_open(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fail)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "open_raw", "io_failure")
    assert pair.closed is False
    _close_directly(pair)


def test_post_close_inspection_failure_is_detached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)

    def fail(*_args: object, **_kwargs: object) -> None:
        raise StagedConversionError(operation="verify_staged", category="io_failure")

    monkeypatch.setattr(conversion_module, "_post_close_stage", fail)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "verify_staged", "io_failure")
    _assert_closed(pair)


def test_artifact_construction_failure_is_detached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)

    def fail(*_args: object, **_kwargs: object) -> object:
        raise ValueError(_PRIVATE_MARKER)

    monkeypatch.setattr(ResearchFileArtifact, "from_stream_report", fail)
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "build_artifact", "invalid_contract")
    _assert_closed(pair)


@pytest.mark.parametrize("failure", [KeyboardInterrupt, SystemExit, MemoryError])
def test_critical_failures_propagate_after_best_effort_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException],
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)

    def fail(*_args: object, **_kwargs: object) -> object:
        raise failure()

    monkeypatch.setattr(conversion_module, "convert_raw_archive_stream", fail)
    with pytest.raises(failure):
        _run(case, pair)
    _assert_closed(pair)


@pytest.mark.parametrize("invalid_name", ["layout", "plan", "context", "pair"])
def test_exact_input_types_are_required(tmp_path: Path, invalid_name: str) -> None:
    case = _make_case(tmp_path)
    pair = _prepare(case)
    values: dict[str, object] = {
        "layout": case.layout,
        "plan": case.plan,
        "context": case.context,
        "pair": pair,
    }
    values[invalid_name] = object()
    with pytest.raises(StagedConversionError) as raised:
        materialize_durable_staged_artifact(
            values["layout"],  # type: ignore[arg-type]
            plan=values["plan"],  # type: ignore[arg-type]
            context=values["context"],  # type: ignore[arg-type]
            pair=values["pair"],  # type: ignore[arg-type]
        )
    _assert_error(raised, "validate_input", "invalid_contract")
    _close_directly(pair)


def test_closed_pair_and_identity_mismatches_are_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    pair = _prepare(case)
    pair.closed = True
    with pytest.raises(StagedConversionError) as raised:
        _run(case, pair)
    _assert_error(raised, "validate_input", "invalid_contract")
    pair.closed = False
    _close_directly(pair)


@pytest.mark.parametrize("mismatch", ["plan", "context", "pair"])
def test_plan_context_and_pair_identity_mismatches_are_rejected(
    tmp_path: Path,
    mismatch: str,
) -> None:
    case = _make_case(tmp_path)
    pair = _prepare(case)
    plan = case.plan
    context = case.context
    if mismatch == "plan":
        plan = ResearchFilePlan.from_raw_identity(
            raw_name="ETH-USDT-1m.jsonl",
            symbol="ETH/USDT",
            interval="1m",
        )
    elif mismatch == "context":
        context = RawConversionContext(
            file_name="ETH-USDT-1m.jsonl",
            symbol="ETH/USDT",
            interval="1m",
            range_start=context.range_start,
            range_end=context.range_end,
        )
    else:
        pair.paths = replace(
            pair.paths,
            research_path=case.layout.research_dir / "other.jsonl",
        )
    with pytest.raises(StagedConversionError) as raised:
        materialize_durable_staged_artifact(
            case.layout,
            plan=plan,
            context=context,
            pair=pair,
        )
    _assert_error(raised, "validate_input", "invalid_contract")
    _close_directly(pair)


def test_validation_remains_active_under_python_optimized_mode() -> None:
    script = """
from packages.market_data.datasets._publication_conversion import (
    StagedConversionError, materialize_durable_staged_artifact,
)
try:
    materialize_durable_staged_artifact(object(), plan=object(), context=object(), pair=object())
except StagedConversionError as error:
    if error.operation != 'validate_input' or error.category != 'invalid_contract':
        raise SystemExit(2)
    if error.__cause__ is not None or error.__context__ is not None:
        raise SystemExit(3)
else:
    raise SystemExit(4)
try:
    StagedConversionError(operation=['PRIVATE_OPTIMIZED_MARKER'], category='invalid_contract')
except ValueError as error:
    if str(error) != 'invalid staged conversion error contract':
        raise SystemExit(5)
    if 'PRIVATE_OPTIMIZED_MARKER' in f'{error!s}{error!r}{vars(error)!r}':
        raise SystemExit(6)
    if error.__cause__ is not None or error.__context__ is not None:
        raise SystemExit(7)
else:
    raise SystemExit(8)
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_source_contract_has_no_broad_catch_assert_cleanup_or_promotion() -> None:
    tree = ast.parse(Path(conversion_module.__file__).read_text(encoding="utf-8"))
    broad_handlers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler)
        and (
            node.type is None
            or (isinstance(node.type, ast.Name) and node.type.id in {"Exception", "BaseException"})
        )
    ]
    called_attributes = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    called_name_list = [
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]
    called_names = set(called_name_list)
    forbidden = {
        "read",
        "readall",
        "readlines",
        "remove",
        "rename",
        "replace",
        "rmdir",
        "seek",
        "truncate",
        "unlink",
    }
    assert broad_handlers == []
    assert not any(isinstance(node, ast.Assert) for node in ast.walk(tree))
    assert forbidden.isdisjoint(called_attributes | called_names)
    assert called_name_list.count("open") == 1
    assert called_name_list.count("convert_raw_archive_stream") == 1


# =============================================================================
# FINDING 1 REGRESSION — post-open ownership boundary
# Monkeypatch sha256() to raise MemoryError after raw is opened but before
# _HashingReader construction completes. All three handles must be
# attempt-closed and pair.closed must be True.
# =============================================================================


def test_sha256_memory_error_closes_all_handles_and_marks_pair_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    tracked_raw = _track_raw_open(monkeypatch, pair)
    research = _StreamProxy(pair.research_stream)
    failure = _StreamProxy(pair.failure_stream)
    pair.research_stream = research  # type: ignore[assignment]
    pair.failure_stream = failure  # type: ignore[assignment]
    open_state: tuple[bool, bool] | None = None

    def failing_sha256() -> object:
        nonlocal open_state
        open_state = research.closed, failure.closed
        raise MemoryError(_PRIVATE_MARKER)

    monkeypatch.setattr(conversion_module, "sha256", failing_sha256)

    with pytest.raises(MemoryError):
        _run(case, pair)

    assert open_state == (False, False)
    assert len(tracked_raw) == 1
    assert tracked_raw[0].close_calls == 1
    assert tracked_raw[0].closed
    assert research.close_calls == 1
    assert failure.close_calls == 1
    _assert_closed(pair)
    assert pair.paths.staging_research_path.exists()
    assert pair.paths.staging_failure_path.exists()


def test_hashing_reader_construction_failure_closes_all_handles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    tracked_raw = _track_raw_open(monkeypatch, pair)
    research = _StreamProxy(pair.research_stream)
    failure = _StreamProxy(pair.failure_stream)
    pair.research_stream = research  # type: ignore[assignment]
    pair.failure_stream = failure  # type: ignore[assignment]

    def fail_reader(*_args: object, **_kwargs: object) -> object:
        raise MemoryError(_PRIVATE_MARKER)

    monkeypatch.setattr(conversion_module, "_HashingReader", fail_reader)
    with pytest.raises(MemoryError):
        _run(case, pair)

    assert len(tracked_raw) == 1
    assert tracked_raw[0].close_calls == 1
    assert tracked_raw[0].closed
    assert research.close_calls == 1
    assert failure.close_calls == 1
    _assert_closed(pair)
    assert pair.paths.staging_research_path.exists()
    assert pair.paths.staging_failure_path.exists()


# =============================================================================
# FINDING 2 REGRESSION — same-size raw mutation after EOF
# Mutate raw file in-place with same size after conversion but before
# research fsync completes. Artifact must NOT be returned.
# =============================================================================


@pytest.mark.parametrize("stage", ["research", "failure"])
def test_same_size_raw_mutation_during_output_fsync_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    raw_content = _line()
    case = _make_case(tmp_path, raw_content)
    pair = _prepare(case)
    tracked_raw = _track_raw_open(monkeypatch, pair)
    target_fd = (
        pair.research_stream.fileno() if stage == "research" else pair.failure_stream.fileno()
    )
    original_open = builtins.open
    real_fsync = conversion_module.os.fsync
    mutation_offset = raw_content.index(b"BTC/USDT")
    mutated = bytearray(raw_content)
    mutated[mutation_offset] = ord("E")
    before = pair.paths.raw_path.stat()
    mutation_observed = False
    metadata_observed = False

    def mutate_during_fsync(descriptor: int) -> None:
        nonlocal metadata_observed, mutation_observed
        if descriptor == target_fd and not mutation_observed:
            with original_open(pair.paths.raw_path, "r+b") as writer:
                writer.seek(mutation_offset)
                writer.write(bytes((mutated[mutation_offset],)))
                writer.flush()
                real_fsync(writer.fileno())
            after = pair.paths.raw_path.stat()
            metadata_observed = (
                after.st_mtime_ns != before.st_mtime_ns or after.st_ctime_ns != before.st_ctime_ns
            )
            mutation_observed = True
        real_fsync(descriptor)

    monkeypatch.setattr(conversion_module.os, "fsync", mutate_during_fsync)
    raised_error: StagedConversionError | None = None
    artifact_returned = False
    try:
        _run(case, pair)
        artifact_returned = True
    except StagedConversionError as error:
        raised_error = error

    assert mutation_observed
    assert pair.paths.raw_path.read_bytes() == bytes(mutated)
    assert pair.paths.raw_path.stat().st_size == len(raw_content)
    if not metadata_observed:
        pytest.skip("host filesystem did not expose same-size mutation in mtime_ns/ctime_ns")
    assert artifact_returned is False
    assert raised_error is not None
    assert raised_error.operation == "inspect_raw"
    assert raised_error.category == "concurrent_change"
    assert raised_error.__cause__ is None
    assert raised_error.__context__ is None
    assert len(tracked_raw) == 1
    assert tracked_raw[0].closed
    _assert_closed(pair)
    assert pair.paths.staging_research_path.exists()
    assert pair.paths.staging_failure_path.exists()


# =============================================================================
# FINDING 3 REGRESSION — invalid StagedConversionError constructor
# Direct construction with invalid operation/category must raise a fixed
# sanitized ValueError, not a KeyError that leaks the marker.
# =============================================================================


def test_invalid_error_constructor_raises_fixed_value_error_not_key_error() -> None:
    with pytest.raises(ValueError) as raised:
        StagedConversionError(
            operation=f"{_PRIVATE_MARKER}_operation",  # type: ignore[arg-type]
            category=f"{_PRIVATE_MARKER}_category",  # type: ignore[arg-type]
        )

    error = raised.value
    assert type(error) is ValueError
    assert str(error) == "invalid staged conversion error contract"
    assert _PRIVATE_MARKER not in f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert vars(error) == {}
    assert not hasattr(error, "operation")
    assert not hasattr(error, "category")
    assert error.__cause__ is None
    assert error.__context__ is None


def test_invalid_error_constructor_operation_leaks_nothing() -> None:
    with pytest.raises(ValueError) as raised:
        StagedConversionError(
            operation=f"{_PRIVATE_MARKER}_operation",  # type: ignore[arg-type]
            category="invalid_contract",
        )

    error = raised.value
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert _PRIVATE_MARKER not in rendered
    assert str(error) == "invalid staged conversion error contract"
    assert error.__cause__ is None
    assert error.__context__ is None


def test_invalid_error_constructor_category_leaks_nothing() -> None:
    with pytest.raises(ValueError) as raised:
        StagedConversionError(
            operation="validate_input",
            category=f"{_PRIVATE_MARKER}_category",  # type: ignore[arg-type]
        )

    error = raised.value
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert _PRIVATE_MARKER not in rendered
    assert str(error) == "invalid staged conversion error contract"
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    ("operation", "category"),
    [
        ("validate_input", "io_failure"),
        (True, "invalid_contract"),
        ([f"{_PRIVATE_MARKER}_list"], "invalid_contract"),
        (object(), "invalid_contract"),
        (None, "invalid_contract"),
        ("validate_input", False),
        ("validate_input", [f"{_PRIVATE_MARKER}_list"]),
        ("validate_input", object()),
        ("validate_input", None),
    ],
)
def test_invalid_error_constructor_runtime_inputs_are_sanitized(
    operation: object,
    category: object,
) -> None:
    with pytest.raises(ValueError) as raised:
        StagedConversionError(
            operation=operation,  # type: ignore[arg-type]
            category=category,  # type: ignore[arg-type]
        )

    error = raised.value
    assert type(error) is ValueError
    assert str(error) == "invalid staged conversion error contract"
    assert _PRIVATE_MARKER not in f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert vars(error) == {}
    assert not hasattr(error, "operation")
    assert not hasattr(error, "category")
    assert error.__cause__ is None
    assert error.__context__ is None


def test_valid_error_constructor_preserves_mapped_contract() -> None:
    error = StagedConversionError(operation="inspect_raw", category="io_failure")
    assert error.operation == "inspect_raw"
    assert error.category == "io_failure"
    assert str(error) == "raw archive inspection failed"


# =============================================================================
# FINDING 1 supplemental — critical failures propagate correctly
# MemoryError during JIT validation must still close handles and propagate.
# =============================================================================


def test_memory_error_during_jit_validation_closes_handles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MemoryError during JIT validation closes raw/research/failure and propagates."""
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)

    def fail_jit(
        layout: object,
        plan: object,
        pair_arg: object,
    ) -> None:
        raise MemoryError("inject during jit validate")

    monkeypatch.setattr(staging_module, "_revalidate_owned_staging_pair", fail_jit)

    with pytest.raises(MemoryError):
        _run(case, pair)

    # Handles must be closed even when MemoryError occurs during JIT validation.
    assert pair.closed is True
    # Staging files must NOT be deleted.
    assert pair.paths.staging_research_path.exists()
    assert pair.paths.staging_failure_path.exists()


def test_clean_post_fsync_raw_fingerprint_returns_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, _line())
    pair = _prepare(case)
    research_fd = pair.research_stream.fileno()
    failure_fd = pair.failure_stream.fileno()
    real_fsync = conversion_module.os.fsync
    observed: list[int] = []

    def tracking_fsync(descriptor: int) -> None:
        observed.append(descriptor)
        real_fsync(descriptor)

    monkeypatch.setattr(conversion_module.os, "fsync", tracking_fsync)
    artifact = _run(case, pair)
    assert type(artifact) is ResearchFileArtifact
    assert observed == [research_fd, failure_fd]
    assert artifact.raw_sha256 == sha256(_line()).hexdigest()
    _assert_closed(pair)


@pytest.mark.parametrize("mutation", ["replace", "disappear"])
def test_raw_path_change_after_fsync_and_close_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    raw = _line()
    case = _make_case(tmp_path, raw)
    pair = _prepare(case)
    tracked_raw = _track_raw_open(monkeypatch, pair)
    original_close = conversion_module._attempt_close
    before = pair.paths.raw_path.stat()
    mutation_observable = mutation == "disappear"

    def close_then_mutate(streams: tuple[BinaryIO, ...]) -> bool:
        nonlocal mutation_observable
        close_failed = original_close(streams)
        pair.paths.raw_path.unlink()
        if mutation == "replace":
            pair.paths.raw_path.write_bytes(raw)
            after = pair.paths.raw_path.stat()
            identity_changed = (
                type(before.st_dev) is int
                and type(before.st_ino) is int
                and before.st_ino != 0
                and type(after.st_dev) is int
                and type(after.st_ino) is int
                and after.st_ino != 0
                and (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
            )
            mutation_observable = identity_changed or (
                before.st_mtime_ns != after.st_mtime_ns or before.st_ctime_ns != after.st_ctime_ns
            )
        return close_failed

    monkeypatch.setattr(conversion_module, "_attempt_close", close_then_mutate)
    raised_error: StagedConversionError | None = None
    artifact_returned = False
    try:
        _run(case, pair)
        artifact_returned = True
    except StagedConversionError as error:
        raised_error = error

    if not mutation_observable:
        pytest.skip("host filesystem did not expose replacement identity or ns timestamps")
    assert artifact_returned is False
    assert raised_error is not None
    assert raised_error.operation == "inspect_raw"
    assert raised_error.category == "concurrent_change"
    assert raised_error.__cause__ is None
    assert raised_error.__context__ is None
    assert len(tracked_raw) == 1
    assert tracked_raw[0].closed
    _assert_closed(pair)
    assert pair.paths.staging_research_path.exists()
    assert pair.paths.staging_failure_path.exists()


@pytest.mark.parametrize("surface", ["descriptor", "path"])
@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("size", True),
        ("mtime_ns", None),
        ("ctime_ns", 1.5),
    ],
)
def test_invalid_raw_fingerprint_metadata_is_sanitized(
    tmp_path: Path,
    surface: str,
    field: str,
    invalid: object,
) -> None:
    path = tmp_path / _PRIVATE_MARKER
    path.write_bytes(b"raw")
    metadata = path.stat()
    overrides: dict[str, object] = {
        "size": _UNCHANGED,
        "mtime_ns": _UNCHANGED,
        "ctime_ns": _UNCHANGED,
    }
    overrides[field] = invalid
    invalid_metadata = cast(
        os.stat_result,
        _StatProxy(
            metadata,
            size=overrides["size"],
            mtime_ns=overrides["mtime_ns"],
            ctime_ns=overrides["ctime_ns"],
        ),
    )
    descriptor_metadata = invalid_metadata if surface == "descriptor" else metadata
    path_metadata = invalid_metadata if surface == "path" else metadata
    entry = fs_module._InspectedEntry(metadata=path_metadata, resolved=path.resolve())

    with pytest.raises(StagedConversionError) as raised:
        conversion_module._RawFingerprint.capture(descriptor_metadata, entry)

    _assert_error(raised, "inspect_raw", "io_failure")
