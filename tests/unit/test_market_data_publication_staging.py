"""DATA-005 C3B-2B package-internal exclusive staging tests."""

from __future__ import annotations

import ast
import builtins
import os
import stat
import subprocess
import sys
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier
from typing import BinaryIO, cast

import pytest

import packages.market_data.datasets as datasets_package
import packages.market_data.datasets._publication_fs as fs_module
import packages.market_data.datasets._publication_staging as staging_module
from packages.market_data.datasets._publication_staging import (
    OwnedStagingPair,
    StagingPreparationError,
    close_owned_staging_pair,
    prepare_exclusive_staging_pair,
)
from packages.market_data.datasets.conversion_manifest import ResearchFilePlan
from packages.market_data.datasets.downloader import MANIFEST_FILE
from packages.market_data.datasets.publication_layout import (
    ResearchArtifactPaths,
    ResearchPublicationLayout,
)

_PRIVATE_MARKER = "C3B2B_PRIVATE_PATH_DO_NOT_LEAK"
_UNCHANGED = object()


@dataclass(frozen=True)
class _StagingCase:
    root: Path
    layout: ResearchPublicationLayout
    plan: ResearchFilePlan

    @property
    def paths(self) -> ResearchArtifactPaths:
        return self.layout.artifact_paths_for(self.plan)


def _make_case(
    root: Path,
    *,
    output_exists: bool = True,
    output_tail: tuple[str, ...] = ("output",),
    raw_payload: bytes = b"raw archive bytes\n",
) -> _StagingCase:
    raw_dir = root / "raw"
    output_dir = root.joinpath(*output_tail)
    raw_dir.mkdir(parents=True)
    (raw_dir / MANIFEST_FILE).write_bytes(b"raw manifest bytes")
    plan = ResearchFilePlan.from_raw_identity(
        raw_name="BTC-USDT-1m.jsonl",
        symbol="BTC/USDT",
        interval="1m",
    )
    (raw_dir / plan.raw_name).write_bytes(raw_payload)
    if output_exists:
        output_dir.mkdir(parents=True)
    return _StagingCase(
        root=root,
        layout=ResearchPublicationLayout(raw_dir=raw_dir, output_dir=output_dir),
        plan=plan,
    )


def _required_directories(layout: ResearchPublicationLayout) -> tuple[Path, ...]:
    return (
        layout.output_dir,
        layout.research_dir,
        layout.failure_dir,
        layout.staging_dir,
        layout.staging_research_dir,
        layout.staging_failure_dir,
    )


def _artifact_targets(case: _StagingCase) -> tuple[Path, ...]:
    paths = case.paths
    return (
        paths.research_path,
        paths.failure_path,
        paths.staging_research_path,
        paths.staging_failure_path,
    )


def _all_absence_targets(case: _StagingCase) -> tuple[Path, ...]:
    return (
        case.layout.research_manifest_path,
        case.layout.staging_manifest_path,
        *_artifact_targets(case),
    )


def _create_output_directories(layout: ResearchPublicationLayout) -> None:
    for path in _required_directories(layout):
        path.mkdir(parents=True, exist_ok=True)


def _close_directly(pair: OwnedStagingPair) -> None:
    for stream in (pair.research_stream, pair.failure_stream):
        try:
            stream.close()
        except (OSError, RuntimeError, ValueError):
            pass


@pytest.fixture
def owned_pairs() -> Iterator[list[OwnedStagingPair]]:
    pairs: list[OwnedStagingPair] = []
    yield pairs
    for pair in pairs:
        _close_directly(pair)


def _prepare(
    case: _StagingCase,
    owned_pairs: list[OwnedStagingPair],
) -> OwnedStagingPair:
    pair = prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    owned_pairs.append(pair)
    return pair


def _assert_staging_error(
    exc_info: pytest.ExceptionInfo[StagingPreparationError],
    *,
    operation: str,
    category: str,
) -> StagingPreparationError:
    error = exc_info.value
    assert error.operation == operation
    assert error.category == category
    assert error.__cause__ is None
    assert error.__context__ is None
    return error


def _symlink_or_skip(link: Path, target: Path, *, directory: bool) -> None:
    try:
        link.symlink_to(target, target_is_directory=directory)
    except (OSError, NotImplementedError):
        pytest.skip("host does not permit creation of the required symbolic link")


class _StatProxy:
    def __init__(
        self,
        metadata: os.stat_result,
        *,
        mode: object = _UNCHANGED,
        size: object = _UNCHANGED,
        device: object = _UNCHANGED,
        inode: object = _UNCHANGED,
        reparse: bool = False,
    ) -> None:
        self.st_mode = metadata.st_mode if mode is _UNCHANGED else mode
        self.st_size = metadata.st_size if size is _UNCHANGED else size
        self.st_dev = metadata.st_dev if device is _UNCHANGED else device
        self.st_ino = metadata.st_ino if inode is _UNCHANGED else inode
        attributes = getattr(metadata, "st_file_attributes", 0)
        self.st_file_attributes = (
            attributes | stat.FILE_ATTRIBUTE_REPARSE_POINT if reparse else attributes
        )


def _patch_entry_metadata(
    monkeypatch: pytest.MonkeyPatch,
    target: Path,
    *,
    reparse: bool = False,
    mode: object = _UNCHANGED,
    size: object = _UNCHANGED,
    device: object = _UNCHANGED,
) -> None:
    original = fs_module._lstat_or_missing

    def replacement(path: Path, *, field: str) -> os.stat_result | None:
        metadata = original(path, field=field)
        if path == target and metadata is not None:
            return _StatProxy(  # type: ignore[return-value]
                metadata,
                reparse=reparse,
                mode=mode,
                size=size,
                device=device,
            )
        return metadata

    monkeypatch.setattr(fs_module, "_lstat_or_missing", replacement)


class _TrackingStream:
    def __init__(self, *, close_error: BaseException | None = None) -> None:
        self.closed = False
        self.close_calls = 0
        self.close_error = close_error

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


class _NoWriteStream:
    def __init__(self, stream: BinaryIO) -> None:
        self._stream = stream
        self.write_calls = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._stream, name)

    @property
    def closed(self) -> bool:
        return self._stream.closed

    @property
    def mode(self) -> str:
        return self._stream.mode

    def close(self) -> None:
        self._stream.close()

    def fileno(self) -> int:
        return self._stream.fileno()

    def tell(self) -> int:
        return self._stream.tell()

    def writable(self) -> bool:
        return self._stream.writable()

    def write(self, data: object) -> int:
        self.write_calls += 1
        raise AssertionError(f"staging preparation wrote payload: {type(data)!r}")


def _pair_for_close(
    research_stream: object,
    failure_stream: object,
) -> OwnedStagingPair:
    root = Path.cwd()
    paths = ResearchArtifactPaths(
        raw_path=root / "raw.jsonl",
        research_path=root / "research.jsonl",
        failure_path=root / "failure.jsonl",
        staging_research_path=root / "research.jsonl.tmp",
        staging_failure_path=root / "failure.jsonl.tmp",
    )
    return OwnedStagingPair(
        paths=paths,
        research_stream=research_stream,  # type: ignore[arg-type]
        failure_stream=failure_stream,  # type: ignore[arg-type]
        research_identity=None,
        failure_identity=None,
    )


@pytest.mark.parametrize("invalid", [None, object(), "layout", 1])
def test_layout_requires_exact_type(invalid: object) -> None:
    plan = ResearchFilePlan.from_raw_identity(
        raw_name="BTC-USDT-1m.jsonl",
        symbol="BTC/USDT",
        interval="1m",
    )
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(invalid, plan=plan)  # type: ignore[arg-type]
    _assert_staging_error(
        exc_info,
        operation="validate_input",
        category="invalid_contract",
    )


def test_layout_subclass_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)

    class LayoutSubclass(ResearchPublicationLayout):
        pass

    layout = LayoutSubclass(
        raw_dir=case.layout.raw_dir,
        output_dir=case.layout.output_dir,
    )
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="validate_input",
        category="invalid_contract",
    )


@pytest.mark.parametrize("invalid", [None, object(), "plan", 1])
def test_plan_requires_exact_type(tmp_path: Path, invalid: object) -> None:
    case = _make_case(tmp_path)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=invalid)  # type: ignore[arg-type]
    _assert_staging_error(
        exc_info,
        operation="validate_input",
        category="invalid_contract",
    )


def test_plan_subclass_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)

    class PlanSubclass(ResearchFilePlan):
        pass

    plan = PlanSubclass(
        raw_name=case.plan.raw_name,
        research_name=case.plan.research_name,
        failure_name=case.plan.failure_name,
        symbol=case.plan.symbol,
        interval=case.plan.interval,
    )
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=plan)
    _assert_staging_error(
        exc_info,
        operation="validate_input",
        category="invalid_contract",
    )


def test_preflight_is_called_with_empty_artifact_tuple(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)
    original = staging_module._preflight.preflight_research_publication
    calls: list[tuple[ResearchPublicationLayout, tuple[object, ...]]] = []

    def replacement(
        layout: ResearchPublicationLayout,
        artifacts: tuple[object, ...],
    ) -> object:
        calls.append((layout, artifacts))
        return original(layout, artifacts)  # type: ignore[arg-type]

    monkeypatch.setattr(
        staging_module._preflight,
        "preflight_research_publication",
        replacement,
    )
    _prepare(case, owned_pairs)
    assert calls == [(case.layout, ())]


def test_snapshot_and_fresh_inspections_precede_every_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path, output_exists=False)
    events: list[tuple[str, object]] = []
    original_preflight = staging_module._preflight.preflight_research_publication
    original_inspect = staging_module._inspect_core
    original_absent = staging_module._require_target_absent
    original_mkdir = staging_module._mkdir_component
    original_open = staging_module._open_exclusive

    def preflight_replacement(*args: object, **kwargs: object) -> object:
        events.append(("snapshot", None))
        return original_preflight(*args, **kwargs)

    def inspect_replacement(*args: object, **kwargs: object) -> object:
        events.append(("inspect", kwargs.get("operation")))
        return original_inspect(*args, **kwargs)

    def absent_replacement(path: Path, **kwargs: object) -> object:
        events.append(("absent", path))
        return original_absent(path, **kwargs)

    def mkdir_replacement(path: Path) -> None:
        events.append(("mkdir", path))
        original_mkdir(path)

    def open_replacement(path: Path) -> BinaryIO:
        events.append(("open", path))
        return original_open(path)

    monkeypatch.setattr(
        staging_module._preflight,
        "preflight_research_publication",
        preflight_replacement,
    )
    monkeypatch.setattr(staging_module, "_inspect_core", inspect_replacement)
    monkeypatch.setattr(staging_module, "_require_target_absent", absent_replacement)
    monkeypatch.setattr(staging_module, "_mkdir_component", mkdir_replacement)
    monkeypatch.setattr(staging_module, "_open_exclusive", open_replacement)
    _prepare(case, owned_pairs)

    assert events[0] == ("snapshot", None)
    first_mutation = next(
        index for index, event in enumerate(events) if event[0] in {"mkdir", "open"}
    )
    initial_absence = [event[1] for event in events[:first_mutation] if event[0] == "absent"]
    assert initial_absence == list(_all_absence_targets(case))

    previous_mutation = -1
    for index, event in enumerate(events):
        if event[0] not in {"mkdir", "open"}:
            continue
        assert any(prior[0] == "inspect" for prior in events[previous_mutation + 1 : index])
        previous_mutation = index


def test_existing_safe_directory_tree_is_reused(
    tmp_path: Path,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    before = {path: path.stat().st_ino for path in _required_directories(case.layout)}
    pair = _prepare(case, owned_pairs)
    assert {path: path.stat().st_ino for path in before} == before
    assert pair.paths == case.paths


def test_completely_missing_output_tree_is_created_component_by_component(
    tmp_path: Path,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path, output_exists=False)
    _prepare(case, owned_pairs)
    assert all(path.is_dir() for path in _required_directories(case.layout))


def test_multicomponent_missing_output_path_uses_exact_mkdir_flags_and_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(
        tmp_path,
        output_exists=False,
        output_tail=("one", "two", "output"),
    )
    original = Path.mkdir
    calls: list[tuple[Path, bool, bool]] = []

    def replacement(
        self: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        calls.append((self, parents, exist_ok))
        original(self, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", replacement)
    _prepare(case, owned_pairs)
    expected = (
        tmp_path / "one",
        tmp_path / "one" / "two",
        case.layout.output_dir,
        case.layout.research_dir,
        case.layout.failure_dir,
        case.layout.staging_dir,
        case.layout.staging_research_dir,
        case.layout.staging_failure_dir,
    )
    assert tuple(path for path, _, _ in calls) == expected
    assert all(not parents and not exist_ok for _, parents, exist_ok in calls)


def test_wrong_kind_output_descendant_is_rejected_before_staging(
    tmp_path: Path,
) -> None:
    case = _make_case(tmp_path)
    case.layout.research_dir.write_bytes(b"wrong kind")
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")
    assert not case.paths.staging_research_path.exists()


def test_symlink_output_ancestor_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    external = tmp_path / "external"
    external.mkdir()
    _symlink_or_skip(case.layout.research_dir, external, directory=True)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")


def test_junction_output_ancestor_is_rejected_where_supported(tmp_path: Path) -> None:
    if os.name != "nt":
        pytest.skip("Windows junction test")
    case = _make_case(tmp_path)
    external = tmp_path / "external-junction"
    external.mkdir()
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(case.layout.research_dir), str(external)],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip("host does not permit creation of the required junction")
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")


def test_reparse_output_ancestor_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    case.layout.research_dir.mkdir()
    _patch_entry_metadata(monkeypatch, case.layout.research_dir, reparse=True)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")


@pytest.mark.skipif(os.name != "nt", reason="Windows namespace contract")
@pytest.mark.parametrize("raw_root", [r"\\server\share\raw", r"\\?\C:\private\raw"])
def test_unc_and_device_namespaces_are_rejected_before_mutation(
    tmp_path: Path,
    raw_root: str,
) -> None:
    layout = ResearchPublicationLayout(
        raw_dir=Path(raw_root),
        output_dir=tmp_path / "output",
    )
    plan = ResearchFilePlan.from_raw_identity(
        raw_name="BTC-USDT-1m.jsonl",
        symbol="BTC/USDT",
        interval="1m",
    )
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(layout, plan=plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")
    assert not layout.output_dir.exists()


@pytest.mark.parametrize("raw_kind", ["missing", "directory"])
def test_raw_path_must_be_a_required_regular_file(tmp_path: Path, raw_kind: str) -> None:
    case = _make_case(tmp_path)
    case.paths.raw_path.unlink()
    if raw_kind == "directory":
        case.paths.raw_path.mkdir()
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")
    assert not case.paths.staging_research_path.exists()


def test_raw_file_symlink_alias_of_manifest_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    case.paths.raw_path.unlink()
    _symlink_or_skip(case.paths.raw_path, case.layout.raw_manifest_path, directory=False)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")


def test_raw_file_hardlink_alias_of_manifest_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    case.paths.raw_path.unlink()
    try:
        os.link(case.layout.raw_manifest_path, case.paths.raw_path)
    except OSError:
        pytest.skip("host filesystem does not permit hardlink creation")
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")


@pytest.mark.parametrize("target_name", ["research_manifest_path", "staging_manifest_path"])
def test_existing_manifest_target_is_rejected(tmp_path: Path, target_name: str) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    target = getattr(case.layout, target_name)
    target.write_bytes(b"existing")
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="entry_exists")


@pytest.mark.parametrize("index", [0, 1])
def test_existing_final_artifact_target_is_rejected(tmp_path: Path, index: int) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    target = _artifact_targets(case)[index]
    target.write_bytes(b"existing")
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="entry_exists")


@pytest.mark.parametrize("index", [2, 3])
def test_existing_staging_artifact_target_is_rejected(tmp_path: Path, index: int) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    target = _artifact_targets(case)[index]
    target.write_bytes(b"stale")
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="entry_exists")


def test_redirected_existing_target_is_rejected_as_unsafe(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    external = tmp_path / "external-target"
    external.write_bytes(b"external")
    _symlink_or_skip(case.paths.staging_research_path, external, directory=False)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(exc_info, operation="preflight", category="unsafe_filesystem")


def test_staging_files_are_created_research_then_failure_with_exact_xb_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)
    original = builtins.open
    calls: list[tuple[Path, str]] = []

    def replacement(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        calls.append((Path(file), mode))  # type: ignore[arg-type]
        return original(file, mode, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", replacement)
    _prepare(case, owned_pairs)
    assert calls == [
        (case.paths.staging_research_path, "xb"),
        (case.paths.staging_failure_path, "xb"),
    ]


def test_success_returns_canonical_open_zero_byte_binary_pair(
    tmp_path: Path,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)
    pair = _prepare(case, owned_pairs)
    assert pair.paths == case.paths
    assert pair.closed is False
    for stream, target in (
        (pair.research_stream, case.paths.staging_research_path),
        (pair.failure_stream, case.paths.staging_failure_path),
    ):
        assert stream.closed is False
        assert stream.writable()
        assert "b" in stream.mode
        assert stream.tell() == 0
        assert os.get_inheritable(stream.fileno()) is False
        assert target.is_file()
        assert target.stat().st_size == 0


def test_exclusive_create_race_allows_exactly_one_invocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    original = staging_module._open_exclusive
    barrier = Barrier(2)

    def replacement(path: Path) -> BinaryIO:
        if path == case.paths.staging_research_path:
            barrier.wait(timeout=10)
        return original(path)

    def invoke() -> OwnedStagingPair | StagingPreparationError:
        try:
            return prepare_exclusive_staging_pair(case.layout, plan=case.plan)
        except StagingPreparationError as error:
            return error

    monkeypatch.setattr(staging_module, "_open_exclusive", replacement)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _index: invoke(), range(2)))

    pairs = [result for result in results if isinstance(result, OwnedStagingPair)]
    errors = [result for result in results if isinstance(result, StagingPreparationError)]
    assert len(pairs) == 1
    assert len(errors) == 1
    owned_pairs.extend(pairs)
    assert errors[0].operation == "create_staging"
    assert errors[0].category == "entry_exists"
    assert errors[0].__cause__ is None
    assert errors[0].__context__ is None
    assert pairs[0].research_stream.closed is False
    assert pairs[0].failure_stream.closed is False
    assert case.paths.staging_research_path.stat().st_size == 0
    assert case.paths.staging_failure_path.stat().st_size == 0


def test_mkdir_file_exists_race_maps_to_entry_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, output_exists=False)
    original = Path.mkdir

    def fail(
        self: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        if self == case.layout.output_dir:
            raise FileExistsError(_PRIVATE_MARKER)
        original(self, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", fail)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="create_directory",
        category="entry_exists",
    )


def test_mkdir_oserror_maps_to_io_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path, output_exists=False)
    original = Path.mkdir

    def fail(
        self: Path,
        mode: int = 0o777,
        parents: bool = False,
        exist_ok: bool = False,
    ) -> None:
        if self == case.layout.output_dir:
            raise PermissionError(13, _PRIVATE_MARKER, str(case.layout.output_dir))
        original(self, mode=mode, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", fail)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    error = _assert_staging_error(
        exc_info,
        operation="create_directory",
        category="io_failure",
    )
    assert _PRIVATE_MARKER not in f"{error!s}{error!r}{vars(error)!r}"


def test_exclusive_open_file_exists_race_maps_to_entry_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)

    def fail(_path: Path) -> object:
        raise FileExistsError(_PRIVATE_MARKER)

    monkeypatch.setattr(staging_module, "_open_exclusive", fail)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="create_staging",
        category="entry_exists",
    )


def test_exclusive_open_oserror_maps_to_io_failure_and_does_not_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)

    def fail(_path: Path) -> object:
        raise PermissionError(13, _PRIVATE_MARKER, str(case.paths.staging_research_path))

    monkeypatch.setattr(staging_module, "_open_exclusive", fail)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    error = _assert_staging_error(
        exc_info,
        operation="create_staging",
        category="io_failure",
    )
    assert _PRIVATE_MARKER not in f"{error!s}{error!r}{vars(error)!r}"


def test_jit_resolve_oserror_maps_to_detached_io_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    original_preflight = staging_module._preflight.preflight_research_publication
    original_resolve = Path.resolve
    snapshot_complete = False

    def preflight_replacement(*args: object, **kwargs: object) -> object:
        nonlocal snapshot_complete
        result = original_preflight(*args, **kwargs)  # type: ignore[arg-type]
        snapshot_complete = True
        return result

    def resolve_replacement(self: Path, strict: bool = False) -> Path:
        if snapshot_complete and self == case.paths.raw_path:
            raise PermissionError(13, _PRIVATE_MARKER, str(self))
        return original_resolve(self, strict=strict)

    monkeypatch.setattr(
        staging_module._preflight,
        "preflight_research_publication",
        preflight_replacement,
    )
    monkeypatch.setattr(Path, "resolve", resolve_replacement)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    error = _assert_staging_error(
        exc_info,
        operation="preflight",
        category="io_failure",
    )
    assert _PRIVATE_MARKER not in f"{error!s}{error!r}{vars(error)!r}"
    assert not case.paths.staging_research_path.exists()
    assert not case.paths.staging_failure_path.exists()


def test_descriptor_path_identity_mismatch_is_concurrent_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    original = staging_module.os.fstat
    calls = 0

    def replacement(fd: int) -> os.stat_result:
        nonlocal calls
        metadata = original(fd)
        calls += 1
        if calls == 1:
            return _StatProxy(metadata, inode=metadata.st_ino + 1)  # type: ignore[return-value]
        return metadata

    monkeypatch.setattr(staging_module.os, "fstat", replacement)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="verify_staging",
        category="concurrent_change",
    )
    assert case.paths.staging_research_path.is_file()


@pytest.mark.parametrize(
    ("surface", "mode", "size"),
    [
        ("descriptor", stat.S_IFDIR | 0o700, 0),
        ("descriptor", stat.S_IFREG | 0o600, 1),
        ("path", stat.S_IFDIR | 0o700, 0),
        ("path", stat.S_IFREG | 0o600, 1),
    ],
)
def test_nonregular_or_nonzero_descriptor_or_path_is_concurrent_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    mode: int,
    size: int,
) -> None:
    case = _make_case(tmp_path)
    original = staging_module.os.fstat

    def replacement(fd: int) -> os.stat_result:
        metadata = original(fd)
        return _StatProxy(metadata, mode=mode, size=size)  # type: ignore[return-value]

    if surface == "descriptor":
        monkeypatch.setattr(staging_module.os, "fstat", replacement)
    else:
        _patch_entry_metadata(
            monkeypatch,
            case.paths.staging_research_path,
            mode=mode,
            size=size,
        )
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="verify_staging",
        category="concurrent_change",
    )


def test_parent_swap_detected_during_post_create_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    original = fs_module._inspect_required_entry

    def replacement(path: Path, *, field: str, kind: object) -> object:
        inspected = original(path, field=field, kind=kind)  # type: ignore[arg-type]
        if path == case.paths.staging_research_path:
            return fs_module._InspectedEntry(
                metadata=inspected.metadata,
                resolved=tmp_path / "elsewhere" / path.name,
            )
        return inspected

    monkeypatch.setattr(fs_module, "_inspect_required_entry", replacement)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="verify_staging",
        category="concurrent_change",
    )


def test_cross_device_staging_and_final_parents_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    original = fs_module._physical_device
    research_metadata = case.layout.research_dir.lstat()
    staging_metadata = case.layout.staging_research_dir.lstat()

    def replacement(metadata: os.stat_result) -> int | None:
        identity = fs_module._physical_file_identity(metadata)
        if identity == fs_module._physical_file_identity(research_metadata):
            return 100
        if identity == fs_module._physical_file_identity(staging_metadata):
            return 200
        return original(metadata)

    monkeypatch.setattr(fs_module, "_physical_device", replacement)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="create_staging",
        category="unsafe_filesystem",
    )


def test_failure_stage_create_failure_leaves_research_stage_present_and_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    original = staging_module._open_exclusive
    opened: list[object] = []

    def replacement(path: Path) -> object:
        if path == case.paths.staging_failure_path:
            raise PermissionError(13, _PRIVATE_MARKER, str(path))
        stream = original(path)
        opened.append(stream)
        return stream

    monkeypatch.setattr(staging_module, "_open_exclusive", replacement)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="create_staging",
        category="io_failure",
    )
    assert len(opened) == 1
    assert opened[0].closed  # type: ignore[attr-defined]
    assert case.paths.staging_research_path.is_file()
    assert case.paths.staging_failure_path.exists() is False


def test_second_post_create_failure_closes_both_owned_streams(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    original_open = staging_module._open_exclusive
    original_verify = staging_module._verify_opened_staging
    opened: list[object] = []

    def open_replacement(path: Path) -> object:
        stream = original_open(path)
        opened.append(stream)
        return stream

    def verify_replacement(*args: object, **kwargs: object) -> object:
        if kwargs.get("path") == case.paths.staging_failure_path:
            raise StagingPreparationError(
                operation="verify_staging",
                category="concurrent_change",
            )
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(staging_module, "_open_exclusive", open_replacement)
    monkeypatch.setattr(staging_module, "_verify_opened_staging", verify_replacement)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="verify_staging",
        category="concurrent_change",
    )
    assert len(opened) == 2
    assert all(stream.closed for stream in opened)  # type: ignore[attr-defined]
    assert case.paths.staging_research_path.is_file()
    assert case.paths.staging_failure_path.is_file()


def test_post_open_device_change_is_concurrent_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    _create_output_directories(case.layout)
    original = fs_module._physical_device
    final_parent_identity = fs_module._physical_file_identity(case.layout.research_dir.lstat())
    final_parent_inspections = 0

    def replacement(metadata: os.stat_result) -> int | None:
        nonlocal final_parent_inspections
        device = original(metadata)
        if fs_module._physical_file_identity(metadata) == final_parent_identity:
            final_parent_inspections += 1
            if final_parent_inspections > 1 and device is not None:
                return device + 1
        return device

    monkeypatch.setattr(fs_module, "_physical_device", replacement)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    _assert_staging_error(
        exc_info,
        operation="verify_staging",
        category="concurrent_change",
    )
    assert case.paths.staging_research_path.is_file()


@pytest.mark.parametrize(
    ("failing_path_name", "exception_type", "expected_opened"),
    [
        ("staging_research_path", KeyboardInterrupt, 1),
        ("staging_failure_path", MemoryError, 2),
    ],
)
def test_propagating_failures_close_every_opened_owned_stream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_path_name: str,
    exception_type: type[BaseException],
    expected_opened: int,
) -> None:
    case = _make_case(tmp_path)
    original_open = staging_module._open_exclusive
    original_verify = staging_module._verify_opened_staging
    failing_path = cast(Path, getattr(case.paths, failing_path_name))
    opened: list[object] = []

    def open_replacement(path: Path) -> object:
        stream = original_open(path)
        opened.append(stream)
        return stream

    def verify_replacement(*args: object, **kwargs: object) -> object:
        if kwargs.get("path") == failing_path:
            raise exception_type()
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(staging_module, "_open_exclusive", open_replacement)
    monkeypatch.setattr(staging_module, "_verify_opened_staging", verify_replacement)
    with pytest.raises(exception_type):
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    assert len(opened) == expected_opened
    assert all(stream.closed for stream in opened)  # type: ignore[attr-defined]
    assert case.paths.staging_research_path.is_file()
    assert case.paths.staging_failure_path.is_file() is (
        failing_path == case.paths.staging_failure_path
    )


def test_close_owned_pair_closes_both_streams_and_marks_pair_closed() -> None:
    research = _TrackingStream()
    failure = _TrackingStream()
    pair = _pair_for_close(research, failure)
    close_owned_staging_pair(pair)
    assert research.closed is True
    assert failure.closed is True
    assert research.close_calls == 1
    assert failure.close_calls == 1
    assert pair.closed is True


def test_close_attempts_second_stream_when_first_close_fails() -> None:
    research = _TrackingStream(close_error=OSError(_PRIVATE_MARKER))
    failure = _TrackingStream()
    pair = _pair_for_close(research, failure)
    with pytest.raises(StagingPreparationError) as exc_info:
        close_owned_staging_pair(pair)
    _assert_staging_error(exc_info, operation="close", category="io_failure")
    assert research.close_calls == 1
    assert failure.close_calls == 1
    assert failure.closed is True
    assert pair.closed is False


def test_repeated_successful_close_is_a_deterministic_noop() -> None:
    research = _TrackingStream()
    failure = _TrackingStream()
    pair = _pair_for_close(research, failure)
    close_owned_staging_pair(pair)
    close_owned_staging_pair(pair)
    assert research.close_calls == 1
    assert failure.close_calls == 1
    assert pair.closed is True


def test_close_requires_exact_owned_pair_type() -> None:
    with pytest.raises(StagingPreparationError) as exc_info:
        close_owned_staging_pair(object())  # type: ignore[arg-type]
    _assert_staging_error(
        exc_info,
        operation="validate_input",
        category="invalid_contract",
    )


def test_errors_are_detached_sanitized_and_store_only_fixed_contract_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path / _PRIVATE_MARKER)

    def fail(_path: Path) -> object:
        raise OSError(5, _PRIVATE_MARKER, str(case.paths.staging_research_path))

    monkeypatch.setattr(staging_module, "_open_exclusive", fail)
    with pytest.raises(StagingPreparationError) as exc_info:
        prepare_exclusive_staging_pair(case.layout, plan=case.plan)
    error = _assert_staging_error(
        exc_info,
        operation="create_staging",
        category="io_failure",
    )
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert _PRIVATE_MARKER not in rendered
    assert str(case.paths.staging_research_path) not in rendered
    assert set(vars(error)) <= {"operation", "category"}


def test_python_optimized_subprocess_preserves_strict_validation() -> None:
    script = """
from packages.market_data.datasets._publication_staging import (
    StagingPreparationError,
    prepare_exclusive_staging_pair,
)
from packages.market_data.datasets.conversion_manifest import ResearchFilePlan

plan = ResearchFilePlan.from_raw_identity(
    raw_name='BTC-USDT-1m.jsonl',
    symbol='BTC/USDT',
    interval='1m',
)
try:
    prepare_exclusive_staging_pair(object(), plan=plan)
except StagingPreparationError as error:
    if error.operation != 'validate_input' or error.category != 'invalid_contract':
        raise SystemExit(2)
    if error.__cause__ is not None or error.__context__ is not None:
        raise SystemExit(3)
else:
    raise SystemExit(4)
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_prepare_never_reads_raw_or_manifest_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)

    def forbidden_read(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("content read is forbidden")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read)
    monkeypatch.setattr(Path, "read_text", forbidden_read)
    monkeypatch.setattr(Path, "open", forbidden_read)
    _prepare(case, owned_pairs)


def test_prepare_never_writes_payload_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)
    original = builtins.open
    wrappers: list[_NoWriteStream] = []

    def replacement(file: object, mode: str = "r", *args: object, **kwargs: object) -> object:
        stream = original(file, mode, *args, **kwargs)
        wrapper = _NoWriteStream(cast(BinaryIO, stream))
        wrappers.append(wrapper)
        return wrapper

    monkeypatch.setattr(builtins, "open", replacement)
    _prepare(case, owned_pairs)
    assert len(wrappers) == 2
    assert all(wrapper.write_calls == 0 for wrapper in wrappers)


def test_production_source_has_no_forbidden_publication_operations() -> None:
    source_path = Path(staging_module.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    forbidden_attributes = {
        "flush",
        "fsync",
        "read",
        "read_bytes",
        "read_text",
        "remove",
        "rename",
        "replace",
        "rmdir",
        "unlink",
        "write",
        "write_bytes",
        "write_text",
    }
    forbidden_calls = {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    } & forbidden_attributes
    forbidden_imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
        if alias.name in {"hashlib", "shutil", "tempfile"}
    }
    assert forbidden_calls == set()
    assert forbidden_imports == set()
    assert not any(isinstance(node, ast.Assert) for node in ast.walk(tree))


def test_internal_api_is_not_added_to_public_lazy_exports() -> None:
    internal_names = {
        "OwnedStagingPair",
        "StagingCategory",
        "StagingOperation",
        "StagingPreparationError",
        "close_owned_staging_pair",
        "prepare_exclusive_staging_pair",
    }
    assert internal_names.isdisjoint(datasets_package.__all__)
    for name in internal_names:
        with pytest.raises(AttributeError):
            getattr(datasets_package, name)


def test_prepare_mutates_only_the_designated_output_tree(
    tmp_path: Path,
    owned_pairs: list[OwnedStagingPair],
) -> None:
    case = _make_case(tmp_path)
    sentinel = tmp_path / "outside-output.bin"
    sentinel.write_bytes(b"unchanged")
    raw_before = {
        path: (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns)
        for path in (case.layout.raw_manifest_path, case.paths.raw_path, sentinel)
    }
    _prepare(case, owned_pairs)
    raw_after = {
        path: (path.read_bytes(), path.stat().st_size, path.stat().st_mtime_ns)
        for path in raw_before
    }
    assert raw_after == raw_before
    assert all(target.is_relative_to(case.layout.output_dir) for target in _artifact_targets(case))
