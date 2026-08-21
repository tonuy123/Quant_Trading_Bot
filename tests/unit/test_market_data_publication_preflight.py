"""DATA-005 B-2B C2/5 read-only physical publication preflight tests."""

from __future__ import annotations

import ast
import builtins
import os
import stat
import subprocess
import sys
from collections.abc import Callable
from dataclasses import FrozenInstanceError, dataclass, fields
from hashlib import sha256
from pathlib import Path

import pytest

import packages.market_data.datasets as datasets_package
import packages.market_data.datasets._publication_fs as fs_module
import packages.market_data.datasets.publication_preflight as preflight_module
from packages.market_data.datasets.conversion_manifest import ResearchFileArtifact
from packages.market_data.datasets.conversion_stream import StreamConversionReport
from packages.market_data.datasets.downloader import MANIFEST_FILE
from packages.market_data.datasets.publication_layout import ResearchPublicationLayout
from packages.market_data.datasets.publication_preflight import (
    PublicationPreflightError,
    PublicationPreflightResult,
    preflight_research_publication,
)

_EMPTY_SHA256 = sha256(b"").hexdigest()
_OPEN_MS = 1_704_067_200_000
_CLOSE_MS = _OPEN_MS + 60_000
_UNCHANGED = object()


def _make_artifact(
    *,
    symbol: str = "BTC/USDT",
    interval: str = "1m",
    empty: bool = False,
    raw_bytes: int = 64,
) -> ResearchFileArtifact:
    prefix = f"{symbol.replace('/', '-')}-{interval}"
    raw_name = f"{prefix}.jsonl"
    if empty:
        report = StreamConversionReport(
            file=raw_name,
            lines_seen=0,
            records_written=0,
            records_quarantined=0,
            coverage_start_ms=None,
            coverage_end_ms=None,
            research_sha256=_EMPTY_SHA256,
            failure_sha256=_EMPTY_SHA256,
            research_bytes=0,
            failure_bytes=0,
            status="success",
        )
        raw_sha256 = _EMPTY_SHA256
        artifact_raw_bytes = 0
    else:
        report = StreamConversionReport(
            file=raw_name,
            lines_seen=1,
            records_written=1,
            records_quarantined=0,
            coverage_start_ms=_OPEN_MS,
            coverage_end_ms=_CLOSE_MS,
            research_sha256="c" * 64,
            failure_sha256=_EMPTY_SHA256,
            research_bytes=128,
            failure_bytes=0,
            status="success",
        )
        raw_sha256 = "a" * 64
        artifact_raw_bytes = raw_bytes
    return ResearchFileArtifact.from_stream_report(
        raw_name=raw_name,
        research_name=raw_name,
        failure_name=f"{prefix}.failures.jsonl",
        symbol=symbol,
        interval=interval,
        raw_sha256=raw_sha256,
        raw_bytes=artifact_raw_bytes,
        report=report,
    )


@dataclass(frozen=True)
class _FilesystemCase:
    root: Path
    layout: ResearchPublicationLayout
    artifacts: tuple[ResearchFileArtifact, ...]


def _make_filesystem_case(
    root: Path,
    *,
    artifacts: tuple[ResearchFileArtifact, ...] | None = None,
    output_exists: bool = True,
    payloads: dict[str, bytes] | None = None,
    manifest_bytes: bytes = b"synthetic manifest content",
) -> _FilesystemCase:
    root.mkdir(parents=True, exist_ok=True)
    completed = (_make_artifact(),) if artifacts is None else artifacts
    raw_dir = root / "raw"
    output_dir = root / "output"
    raw_dir.mkdir()
    (raw_dir / MANIFEST_FILE).write_bytes(manifest_bytes)
    for artifact in completed:
        payload = (
            payloads[artifact.raw_name]
            if payloads is not None and artifact.raw_name in payloads
            else b"x" * artifact.raw_bytes
        )
        (raw_dir / artifact.raw_name).write_bytes(payload)
    if output_exists:
        output_dir.mkdir()
    return _FilesystemCase(
        root=root,
        layout=ResearchPublicationLayout(raw_dir=raw_dir, output_dir=output_dir),
        artifacts=completed,
    )


def _output_directories(layout: ResearchPublicationLayout) -> tuple[Path, ...]:
    return (
        layout.research_dir,
        layout.failure_dir,
        layout.staging_dir,
        layout.staging_research_dir,
        layout.staging_failure_dir,
    )


def _artifact_output_targets(
    layout: ResearchPublicationLayout,
    artifact: ResearchFileArtifact,
) -> tuple[Path, ...]:
    return (
        layout.research_path(artifact),
        layout.failure_path(artifact),
        layout.staging_research_path(artifact),
        layout.staging_failure_path(artifact),
    )


def _manifest_output_targets(layout: ResearchPublicationLayout) -> tuple[Path, Path]:
    return layout.research_manifest_path, layout.staging_manifest_path


def _symlink_or_skip(link: Path, target: Path, *, directory: bool) -> None:
    try:
        link.symlink_to(target, target_is_directory=directory)
    except (OSError, NotImplementedError):
        pytest.skip("host does not permit creation of the required symbolic link")


def _junction_or_skip(link: Path, target: Path) -> None:
    if os.name != "nt":
        pytest.skip("Windows junction test")
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip("host does not permit creation of the required junction")


def _snapshot_tree(root: Path) -> tuple[tuple[object, ...], ...]:
    rows: list[tuple[object, ...]] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        metadata = path.lstat()
        content = path.read_bytes() if stat.S_ISREG(metadata.st_mode) else None
        rows.append(
            (
                path.relative_to(root).as_posix(),
                stat.S_IFMT(metadata.st_mode),
                content,
                metadata.st_size,
                metadata.st_mtime_ns,
            )
        )
    return tuple(rows)


class _StatProxy:
    def __init__(
        self,
        metadata: os.stat_result,
        *,
        reparse: bool = False,
        size: object = _UNCHANGED,
        device: object = _UNCHANGED,
        inode: object = _UNCHANGED,
    ) -> None:
        self.st_mode = metadata.st_mode
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
    size: object = _UNCHANGED,
) -> None:
    original = fs_module._lstat_or_missing

    def replacement(path: Path, *, field: str) -> os.stat_result | None:
        metadata = original(path, field=field)
        if path == target and metadata is not None:
            return _StatProxy(metadata, reparse=reparse, size=size)  # type: ignore[return-value]
        return metadata

    monkeypatch.setattr(fs_module, "_lstat_or_missing", replacement)


def _patch_lstat_failure(
    monkeypatch: pytest.MonkeyPatch,
    target: Path,
    error_factory: Callable[[], OSError],
) -> None:
    path_type = type(target)
    original = path_type.lstat

    def replacement(self: Path) -> os.stat_result:
        if self == target:
            raise error_factory()
        return original(self)

    monkeypatch.setattr(path_type, "lstat", replacement)


def _assert_detached_and_sanitized(error: BaseException, *markers: str) -> None:
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered_values = (str(error), repr(error), repr(vars(error)))
    for marker in markers:
        assert all(marker not in rendered for rendered in rendered_values)


def test_preflight_uses_shared_inspection_seam(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    required_paths: list[Path] = []
    optional_paths: list[Path] = []
    original_required = fs_module._inspect_required_entry
    original_optional = fs_module._inspect_optional_entry

    def inspect_required(
        path: Path,
        *,
        field: str,
        kind: fs_module._EntryKind,
    ) -> fs_module._InspectedEntry:
        required_paths.append(path)
        return original_required(path, field=field, kind=kind)

    def inspect_optional(
        path: Path,
        *,
        kind: fs_module._EntryKind,
    ) -> fs_module._OutputRoot:
        optional_paths.append(path)
        return original_optional(path, kind=kind)

    monkeypatch.setattr(fs_module, "_inspect_required_entry", inspect_required)
    monkeypatch.setattr(fs_module, "_inspect_optional_entry", inspect_optional)

    result = preflight_research_publication(case.layout, case.artifacts)

    artifact = case.artifacts[0]
    assert result.raw_file_count == 1
    assert required_paths == [
        case.layout.raw_dir,
        case.layout.raw_manifest_path,
        case.layout.raw_path(artifact),
    ]
    assert set(optional_paths) == {
        case.layout.output_dir,
        *_output_directories(case.layout),
        *_manifest_output_targets(case.layout),
        *_artifact_output_targets(case.layout, artifact),
    }


@pytest.mark.parametrize("kind", ["directory", "file"])
def test_shared_required_entry_accepts_expected_kind(tmp_path: Path, kind: str) -> None:
    target = tmp_path / "required-entry"
    if kind == "directory":
        target.mkdir()
    else:
        target.write_bytes(b"metadata only")

    inspected = fs_module._inspect_required_entry(
        target,
        field="raw_artifact",
        kind=kind,  # type: ignore[arg-type]
    )

    assert inspected.resolved == target.resolve(strict=True)
    assert inspected.metadata.st_mode == target.lstat().st_mode


@pytest.mark.parametrize(
    ("actual_kind", "required_kind", "reason"),
    [
        ("file", "directory", "filesystem entry must be a directory"),
        ("directory", "file", "filesystem entry must be a regular file"),
    ],
)
def test_shared_required_entry_rejects_wrong_kind(
    tmp_path: Path,
    actual_kind: str,
    required_kind: str,
    reason: str,
) -> None:
    target = tmp_path / "wrong-kind"
    if actual_kind == "directory":
        target.mkdir()
    else:
        target.write_bytes(b"metadata only")

    with pytest.raises(fs_module.PhysicalInspectionError) as raised:
        fs_module._inspect_required_entry(
            target,
            field="raw_artifact",
            kind=required_kind,  # type: ignore[arg-type]
        )

    assert raised.value.field == "raw_artifact"
    assert raised.value.reason == reason


@pytest.mark.parametrize("existing", [True, False], ids=["existing", "missing"])
def test_shared_optional_entry_projects_existing_and_missing(
    tmp_path: Path,
    existing: bool,
) -> None:
    safe_parent = tmp_path / "output"
    safe_parent.mkdir()
    target = safe_parent / "research"
    if existing:
        target.mkdir()

    inspected = fs_module._inspect_optional_entry(target, kind="directory")

    assert inspected.state == ("existing" if existing else "missing")
    assert inspected.projected == safe_parent.resolve(strict=True) / "research"


def test_shared_symlink_and_reparse_metadata_are_rejected(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target"
    target.write_bytes(b"metadata only")
    link = tmp_path / "link"
    _symlink_or_skip(link, target, directory=False)

    with pytest.raises(fs_module.PhysicalInspectionError) as symlink_error:
        fs_module._inspect_required_entry(
            link,
            field="raw_artifact",
            kind="file",
        )
    assert symlink_error.value.reason == ("filesystem entry must not be a symlink or reparse point")

    reparse_metadata = _StatProxy(target.lstat(), reparse=True)
    with pytest.raises(fs_module.PhysicalInspectionError) as reparse_error:
        fs_module._reject_redirection(
            target,
            reparse_metadata,  # type: ignore[arg-type]
            field="raw_artifact",
        )
    assert reparse_error.value.reason == symlink_error.value.reason


@pytest.mark.parametrize("relationship", ["equal", "raw_nested", "output_nested"])
def test_shared_physical_roots_must_be_distinct_and_non_nested(
    tmp_path: Path,
    relationship: str,
) -> None:
    raw_root = tmp_path / "raw"
    output_root = tmp_path / "output"
    if relationship == "equal":
        output_root = raw_root
    elif relationship == "raw_nested":
        raw_root = output_root / "raw"
    else:
        output_root = raw_root / "output"

    with pytest.raises(fs_module.PhysicalInspectionError) as raised:
        fs_module._reject_related_physical_roots(raw_root, output_root)

    assert raised.value.field == "containment"
    assert raised.value.reason == "physical roots must be distinct and non-nested"


@pytest.mark.parametrize("size", [0, 19])
def test_shared_file_size_accepts_non_negative_real_int(tmp_path: Path, size: int) -> None:
    target = tmp_path / "size"
    target.write_bytes(b"x")
    metadata = _StatProxy(target.lstat(), size=size)

    assert fs_module._require_file_size(metadata) == size  # type: ignore[arg-type]


@pytest.mark.parametrize("size", [True, 1.5, "1", -1, None])
def test_shared_file_size_rejects_invalid_values(tmp_path: Path, size: object) -> None:
    target = tmp_path / "invalid-size"
    target.write_bytes(b"x")
    metadata = _StatProxy(target.lstat(), size=size)

    with pytest.raises(fs_module.PhysicalInspectionError) as raised:
        fs_module._require_file_size(metadata)  # type: ignore[arg-type]

    assert raised.value.field == "raw_artifact"
    assert raised.value.reason == "filesystem entry has an invalid size"


@pytest.mark.parametrize(
    ("device", "inode", "expected"),
    [
        (7, 11, (7, 11)),
        (-1, -2, (-1, -2)),
        (True, 11, None),
        (7, False, None),
        ("7", 11, None),
        (7, "11", None),
        (7, 0, None),
    ],
)
def test_shared_physical_file_identity_uses_only_usable_inode_metadata(
    tmp_path: Path,
    device: object,
    inode: object,
    expected: tuple[int, int] | None,
) -> None:
    target = tmp_path / "identity"
    target.write_bytes(b"x")
    metadata = _StatProxy(target.lstat(), device=device, inode=inode)

    assert fs_module._physical_file_identity(metadata) == expected  # type: ignore[arg-type]


@pytest.mark.parametrize("exception_type", [PermissionError, OSError])
def test_shared_lstat_errors_are_detached_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exception_type: type[OSError],
) -> None:
    marker = "SYNTHETIC_PRIVATE_SHARED_LSTAT"
    target = tmp_path / marker
    _patch_lstat_failure(
        monkeypatch,
        target,
        lambda: exception_type(9876, f"{marker} strerror", f"{marker} filename"),
    )

    with pytest.raises(fs_module.PhysicalInspectionError) as raised:
        fs_module._lstat_or_missing(target, field="raw_artifact")

    assert raised.value.field == "raw_artifact"
    assert raised.value.reason == "filesystem entry could not be inspected"
    _assert_detached_and_sanitized(raised.value, marker, "9876")


@pytest.mark.parametrize("exception_type", [OSError, RuntimeError])
def test_shared_resolve_errors_are_detached_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exception_type: type[Exception],
) -> None:
    marker = "SYNTHETIC_PRIVATE_SHARED_RESOLVE"
    target = tmp_path / marker
    path_type = type(target)

    def replacement(self: Path, *, strict: bool = False) -> Path:
        raise exception_type(marker)

    monkeypatch.setattr(path_type, "resolve", replacement)
    with pytest.raises(fs_module.PhysicalInspectionError) as raised:
        fs_module._resolve_strict(target)

    assert raised.value.field == "containment"
    assert raised.value.reason == "filesystem containment could not be verified"
    _assert_detached_and_sanitized(raised.value, marker)


def test_shared_error_is_translated_to_detached_public_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "SYNTHETIC_PRIVATE_SHARED_TRANSLATION"
    case = _make_filesystem_case(tmp_path / marker, artifacts=())

    def fail_local_root(path: Path, *, field: str) -> None:
        raise fs_module.PhysicalInspectionError(
            field=field,
            reason="filesystem entry could not be inspected",
        )

    monkeypatch.setattr(fs_module, "_require_local_root", fail_local_root)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())

    assert raised.value.field == "raw_dir"
    assert raised.value.reason == "filesystem entry could not be inspected"
    _assert_detached_and_sanitized(raised.value, marker)


def test_existing_safe_paths_return_existing_result(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)

    result = preflight_research_publication(case.layout, case.artifacts)

    assert result.raw_file_count == 1
    assert result.raw_bytes == (case.layout.raw_dir / case.artifacts[0].raw_name).stat().st_size
    assert result.raw_bytes == case.artifacts[0].raw_bytes
    assert result.output_directory_state == "existing"


def test_multiple_artifacts_count_actual_bytes_and_exclude_manifest(tmp_path: Path) -> None:
    artifacts = (
        _make_artifact(raw_bytes=7),
        _make_artifact(symbol="ETH/USDT", raw_bytes=19),
    )
    payloads = {artifacts[0].raw_name: b"a" * 7, artifacts[1].raw_name: b"b" * 19}
    case = _make_filesystem_case(
        tmp_path,
        artifacts=artifacts,
        payloads=payloads,
        manifest_bytes=b"manifest bytes are deliberately not counted" * 5,
    )

    result = preflight_research_publication(case.layout, artifacts)

    assert result.raw_file_count == 2
    assert result.raw_bytes == 26
    assert result.raw_bytes != case.layout.raw_manifest_path.stat().st_size + 26


def test_missing_output_with_safe_existing_parent_returns_missing(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    safe_parent = tmp_path / "future"
    safe_parent.mkdir()
    layout = ResearchPublicationLayout(
        raw_dir=case.layout.raw_dir,
        output_dir=safe_parent / "deep" / "research",
    )

    result = preflight_research_publication(layout, case.artifacts)

    assert result.output_directory_state == "missing"
    assert not layout.output_dir.exists()


def test_empty_artifact_tuple_is_valid_with_manifest(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())

    result = preflight_research_publication(case.layout, ())

    assert result == PublicationPreflightResult(
        raw_file_count=0,
        raw_bytes=0,
        output_directory_state="existing",
    )


def test_zero_byte_raw_artifact_is_valid(tmp_path: Path) -> None:
    artifact = _make_artifact(empty=True)
    case = _make_filesystem_case(
        tmp_path,
        artifacts=(artifact,),
        payloads={artifact.raw_name: b""},
    )

    result = preflight_research_publication(case.layout, case.artifacts)

    assert result.raw_file_count == 1
    assert result.raw_bytes == 0


@pytest.mark.parametrize("physical_size", [63, 65], ids=["smaller", "larger"])
def test_physical_raw_size_mismatch_is_rejected_and_sanitized(
    tmp_path: Path,
    physical_size: int,
) -> None:
    artifact = _make_artifact(raw_bytes=64)
    private_root = tmp_path / "SYNTHETIC_PRIVATE_MARKER"
    payload = (b"PRIVATE_PAYLOAD" * 8)[:physical_size]
    case = _make_filesystem_case(
        private_root,
        artifacts=(artifact,),
        payloads={artifact.raw_name: payload},
    )

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    error = raised.value
    assert error.field == "raw_artifact"
    assert error.reason == "physical size must match artifact raw_bytes"
    assert error.__cause__ is None
    assert error.__context__ is None
    for rendered in (str(error), repr(error), repr(vars(error))):
        assert "SYNTHETIC_PRIVATE_MARKER" not in rendered
        assert "PRIVATE_PAYLOAD" not in rendered
        assert artifact.raw_name not in rendered
        assert str(physical_size) not in rendered


def test_1m_and_1M_keep_distinct_names_and_preflight_independently(tmp_path: Path) -> None:
    artifacts = (_make_artifact(interval="1m"), _make_artifact(interval="1M"))
    assert artifacts[0].raw_name != artifacts[1].raw_name

    for index, artifact in enumerate(artifacts):
        case = _make_filesystem_case(tmp_path / str(index), artifacts=(artifact,))
        result = preflight_research_publication(case.layout, case.artifacts)
        assert result.raw_file_count == 1


@pytest.mark.skipif(os.name != "nt", reason="Windows case-insensitive path contract")
def test_1m_and_1M_same_physical_file_are_rejected(tmp_path: Path) -> None:
    artifacts = (_make_artifact(interval="1m"), _make_artifact(interval="1M"))
    case = _make_filesystem_case(tmp_path, artifacts=artifacts)
    first = case.layout.raw_path(artifacts[0])
    second = case.layout.raw_path(artifacts[1])
    if not first.samefile(second):
        pytest.skip("temporary directory is case-sensitive")

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, artifacts)

    assert raised.value.field == "artifacts"
    assert raised.value.reason == "must resolve to distinct physical raw paths"


@pytest.mark.parametrize("value", [None, object(), "layout"], ids=["none", "object", "string"])
def test_wrong_layout_type_is_rejected(value: object) -> None:
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(value, ())  # type: ignore[arg-type]
    assert raised.value.field == "layout"


def test_layout_subclass_is_rejected(tmp_path: Path) -> None:
    class LayoutSubclass(ResearchPublicationLayout):
        pass

    raw_dir = tmp_path / "raw"
    output_dir = tmp_path / "output"
    layout = LayoutSubclass(raw_dir=raw_dir, output_dir=output_dir)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, ())
    assert raised.value.reason == "must be an exact ResearchPublicationLayout"


@pytest.mark.parametrize(
    "factory",
    [lambda: [], lambda: iter(()), lambda: set(), lambda: None],
    ids=["list", "generator", "set", "none"],
)
def test_artifacts_must_be_exact_tuple(factory: Callable[[], object]) -> None:
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(  # type: ignore[arg-type]
            ResearchPublicationLayout(
                raw_dir=Path(r"D:\synthetic\raw"),
                output_dir=Path(r"D:\synthetic\output"),
            ),
            factory(),
        )
    assert raised.value.field == "artifacts"
    assert raised.value.reason == "must be an exact tuple"


@dataclass(frozen=True)
class _FakeArtifact:
    raw_name: str = "BTC-USDT-1m.jsonl"


@pytest.mark.parametrize("value", [None, object(), _FakeArtifact()])
def test_artifact_items_must_have_exact_type(tmp_path: Path, value: object) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, (value,))  # type: ignore[arg-type]
    assert raised.value.field == "artifacts"
    assert raised.value.reason == "must contain exact ResearchFileArtifact values"


def test_artifact_subclass_is_rejected(tmp_path: Path) -> None:
    class ArtifactSubclass(ResearchFileArtifact):
        pass

    base = _make_artifact()
    subclass = ArtifactSubclass(**{field.name: getattr(base, field.name) for field in fields(base)})
    case = _make_filesystem_case(tmp_path, artifacts=())

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, (subclass,))
    assert raised.value.reason == "must contain exact ResearchFileArtifact values"


def test_duplicate_raw_name_is_rejected_before_filesystem_use(tmp_path: Path) -> None:
    artifact = _make_artifact()
    case = _make_filesystem_case(tmp_path, artifacts=(artifact,))

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, (artifact, artifact))
    assert raised.value.field == "artifacts"
    assert raised.value.reason == "must not contain duplicate raw names"


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("raw_file_count", True),
        ("raw_file_count", 1.0),
        ("raw_file_count", "SYNTHETIC_PRIVATE_MARKER"),
        ("raw_file_count", None),
        ("raw_file_count", -1),
        ("raw_bytes", False),
        ("raw_bytes", 1.0),
        ("raw_bytes", "SYNTHETIC_PRIVATE_MARKER"),
        ("raw_bytes", None),
        ("raw_bytes", -1),
    ],
)
def test_result_counters_reject_non_real_or_negative_int(
    field_name: str,
    value: object,
) -> None:
    values: dict[str, object] = {
        "raw_file_count": 0,
        "raw_bytes": 0,
        "output_directory_state": "existing",
    }
    values[field_name] = value
    with pytest.raises(PublicationPreflightError) as raised:
        PublicationPreflightResult(**values)  # type: ignore[arg-type]
    assert raised.value.field == field_name
    assert raised.value.reason == f"{field_name} must be a non-negative integer"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    for rendered in (str(raised.value), repr(raised.value), repr(vars(raised.value))):
        assert "SYNTHETIC_PRIVATE_MARKER" not in rendered


@pytest.mark.parametrize("value", ["complete", "", None, 1])
def test_result_rejects_invalid_output_state(value: object) -> None:
    with pytest.raises(PublicationPreflightError) as raised:
        PublicationPreflightResult(
            raw_file_count=0,
            raw_bytes=0,
            output_directory_state=value,  # type: ignore[arg-type]
        )
    assert raised.value.field == "output_dir"


def test_result_is_frozen() -> None:
    result = PublicationPreflightResult(
        raw_file_count=0,
        raw_bytes=0,
        output_directory_state="missing",
    )
    with pytest.raises(FrozenInstanceError):
        result.raw_bytes = 1  # type: ignore[misc]


def test_python_optimized_subprocess_keeps_validation() -> None:
    program = """
import packages.market_data.datasets._publication_fs as fs_module
from packages.market_data.datasets import (
    PublicationPreflightError,
    PublicationPreflightResult,
    preflight_research_publication,
)

try:
    PublicationPreflightResult(
        raw_file_count=True,
        raw_bytes=0,
        output_directory_state="existing",
    )
except PublicationPreflightError as error:
    if error.field != "raw_file_count":
        raise SystemExit(3)
else:
    raise SystemExit(1)

try:
    PublicationPreflightResult(
        raw_file_count=0,
        raw_bytes=False,
        output_directory_state="existing",
    )
except PublicationPreflightError as error:
    if error.field != "raw_bytes":
        raise SystemExit(4)
else:
    raise SystemExit(5)

class InvalidMetadata:
    st_size = True

try:
    fs_module._require_file_size(InvalidMetadata())
except fs_module.PhysicalInspectionError as error:
    if error.field != "raw_artifact":
        raise SystemExit(6)
else:
    raise SystemExit(7)

try:
    preflight_research_publication(None, ())
except PublicationPreflightError:
    raise SystemExit(0)
raise SystemExit(2)
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", program],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_missing_raw_directory_is_rejected(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    layout = ResearchPublicationLayout(
        raw_dir=tmp_path / "missing-raw",
        output_dir=output_dir,
    )
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, ())
    assert raised.value.field == "raw_dir"
    assert raised.value.reason == "filesystem entry does not exist"


def test_raw_directory_must_not_be_regular_file(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.write_bytes(b"not a directory")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    layout = ResearchPublicationLayout(raw_dir=raw_dir, output_dir=output_dir)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, ())
    assert raised.value.reason == "filesystem entry must be a directory"


def test_raw_directory_symlink_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "raw-target"
    target.mkdir()
    (target / MANIFEST_FILE).write_bytes(b"manifest")
    link = tmp_path / "raw-link"
    _symlink_or_skip(link, target, directory=True)
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    layout = ResearchPublicationLayout(raw_dir=link, output_dir=output_dir)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, ())
    assert "symlink or reparse point" in raised.value.reason


def test_raw_directory_junction_is_rejected(tmp_path: Path) -> None:
    target = tmp_path / "raw-target"
    target.mkdir()
    (target / MANIFEST_FILE).write_bytes(b"manifest")
    junction = tmp_path / "raw-junction"
    _junction_or_skip(junction, target)
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    layout = ResearchPublicationLayout(raw_dir=junction, output_dir=output_dir)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, ())
    assert "symlink or reparse point" in raised.value.reason


def test_missing_raw_manifest_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    case.layout.raw_manifest_path.unlink()
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())
    assert raised.value.field == "raw_manifest"
    assert raised.value.reason == "filesystem entry does not exist"


def test_raw_manifest_directory_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    case.layout.raw_manifest_path.unlink()
    case.layout.raw_manifest_path.mkdir()
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())
    assert raised.value.reason == "filesystem entry must be a regular file"


def test_raw_manifest_symlink_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    case.layout.raw_manifest_path.unlink()
    target = tmp_path / "private-manifest"
    target.write_bytes(b"private")
    _symlink_or_skip(case.layout.raw_manifest_path, target, directory=False)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())
    assert "symlink or reparse point" in raised.value.reason


def test_missing_raw_artifact_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    case.layout.raw_path(case.artifacts[0]).unlink()
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert raised.value.field == "raw_artifact"
    assert raised.value.reason == "filesystem entry does not exist"


def test_raw_artifact_directory_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    raw_path = case.layout.raw_path(case.artifacts[0])
    raw_path.unlink()
    raw_path.mkdir()
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert raised.value.reason == "filesystem entry must be a regular file"


def test_raw_artifact_symlink_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    raw_path = case.layout.raw_path(case.artifacts[0])
    raw_path.unlink()
    target = tmp_path / "private-raw"
    target.write_bytes(b"private")
    _symlink_or_skip(raw_path, target, directory=False)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert "symlink or reparse point" in raised.value.reason


def test_raw_artifact_reparse_attribute_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    target = case.layout.raw_path(case.artifacts[0])
    _patch_entry_metadata(monkeypatch, target, reparse=True)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert "symlink or reparse point" in raised.value.reason


def test_physical_raw_artifact_containment_escape_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    artifact_path = case.layout.raw_path(case.artifacts[0])
    original = fs_module._resolve_strict

    def replacement(path: Path) -> Path:
        if path == artifact_path:
            return tmp_path / "outside" / path.name
        return original(path)

    monkeypatch.setattr(fs_module, "_resolve_strict", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert raised.value.field == "containment"


def test_duplicate_hard_linked_raw_files_are_rejected(tmp_path: Path) -> None:
    artifacts = (_make_artifact(), _make_artifact(symbol="ETH/USDT"))
    case = _make_filesystem_case(tmp_path, artifacts=(artifacts[0],))
    source = case.layout.raw_path(artifacts[0])
    alias = case.layout.raw_path(artifacts[1])
    try:
        os.link(source, alias)
    except OSError:
        pytest.skip("host filesystem does not permit hard-link creation")

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, artifacts)
    assert raised.value.reason == "must resolve to distinct physical raw paths"


def test_duplicate_canonical_raw_paths_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifacts = (_make_artifact(), _make_artifact(symbol="ETH/USDT"))
    case = _make_filesystem_case(tmp_path, artifacts=artifacts)
    first_path = case.layout.raw_path(artifacts[0])
    second_path = case.layout.raw_path(artifacts[1])
    canonical_first = first_path.resolve(strict=True)
    original = fs_module._resolve_strict

    def replacement(path: Path) -> Path:
        if path == first_path or path == second_path:
            return canonical_first
        return original(path)

    monkeypatch.setattr(fs_module, "_resolve_strict", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, artifacts)
    assert raised.value.reason == "must resolve to distinct physical raw paths"


def test_manifest_and_artifact_hardlink_identity_is_rejected(tmp_path: Path) -> None:
    artifact = _make_artifact(raw_bytes=64)
    case = _make_filesystem_case(
        tmp_path,
        artifacts=(),
        manifest_bytes=b"m" * artifact.raw_bytes,
    )
    try:
        os.link(
            case.layout.raw_manifest_path,
            case.layout.raw_path(artifact),
        )
    except OSError:
        pytest.skip("host filesystem does not permit hard-link creation")

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, (artifact,))

    error = raised.value
    assert error.field == "artifacts"
    assert error.reason == "must resolve to distinct physical raw paths"
    assert error.__cause__ is None
    assert error.__context__ is None


def test_manifest_and_artifact_canonical_alias_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    artifact_path = case.layout.raw_path(case.artifacts[0])
    canonical_manifest = case.layout.raw_manifest_path.resolve(strict=True)
    original = fs_module._resolve_strict

    def replacement(path: Path) -> Path:
        if path == artifact_path:
            return canonical_manifest
        return original(path)

    monkeypatch.setattr(fs_module, "_resolve_strict", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.field == "artifacts"
    assert raised.value.reason == "must resolve to distinct physical raw paths"


@pytest.mark.parametrize(
    "error_factory",
    [
        lambda: PermissionError(
            13,
            "SYNTHETIC_PRIVATE_MARKER strerror",
            "SYNTHETIC_PRIVATE_MARKER filename",
        ),
        lambda: OSError(
            9876,
            "SYNTHETIC_PRIVATE_MARKER strerror",
            "SYNTHETIC_PRIVATE_MARKER filename",
        ),
    ],
    ids=["permission", "oserror"],
)
def test_lstat_os_errors_are_detached_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error_factory: Callable[[], OSError],
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    _patch_lstat_failure(monkeypatch, case.layout.raw_manifest_path, error_factory)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())

    error = raised.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert error.field == "raw_manifest"
    for rendered in (str(error), repr(error), repr(vars(error))):
        assert "SYNTHETIC_PRIVATE_MARKER" not in rendered
        assert "9876" not in rendered


@pytest.mark.parametrize("exception_type", [OSError, RuntimeError])
def test_resolve_errors_are_detached_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exception_type: type[Exception],
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    path_type = type(case.layout.raw_dir)
    original = path_type.resolve

    def replacement(self: Path, *, strict: bool = False) -> Path:
        if self == case.layout.raw_dir:
            raise exception_type("SYNTHETIC_PRIVATE_MARKER")
        return original(self, strict=strict)

    monkeypatch.setattr(path_type, "resolve", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert "SYNTHETIC_PRIVATE_MARKER" not in repr(raised.value)


@pytest.mark.parametrize("size", [True, 1.5, "1", -1, None])
def test_invalid_raw_file_size_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    size: object,
) -> None:
    case = _make_filesystem_case(tmp_path)
    _patch_entry_metadata(
        monkeypatch,
        case.layout.raw_path(case.artifacts[0]),
        size=size,
    )
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert raised.value.field == "raw_artifact"
    assert raised.value.reason == "filesystem entry has an invalid size"


def test_existing_output_path_must_not_be_regular_file(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    case.layout.output_dir.write_bytes(b"not a directory")
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert raised.value.field == "output_dir"
    assert raised.value.reason == "filesystem entry must be a directory"


def test_existing_output_symlink_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    target = tmp_path / "output-target"
    target.mkdir()
    _symlink_or_skip(case.layout.output_dir, target, directory=True)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert "symlink or reparse point" in raised.value.reason


def test_existing_output_junction_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    target = tmp_path / "output-target"
    target.mkdir()
    _junction_or_skip(case.layout.output_dir, target)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert "symlink or reparse point" in raised.value.reason


def test_existing_output_reparse_attribute_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    _patch_entry_metadata(monkeypatch, case.layout.output_dir, reparse=True)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert raised.value.field == "output_dir"
    assert "symlink or reparse point" in raised.value.reason


def test_missing_output_descendant_directories_are_accepted(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    assert all(not path.exists() for path in _output_directories(case.layout))

    result = preflight_research_publication(case.layout, case.artifacts)

    assert result.output_directory_state == "existing"
    assert all(not path.exists() for path in _output_directories(case.layout))


def test_missing_manifest_output_targets_are_accepted_with_empty_artifacts(
    tmp_path: Path,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    targets = _manifest_output_targets(case.layout)
    assert all(not target.exists() for target in targets)

    result = preflight_research_publication(case.layout, ())

    assert result.raw_file_count == 0
    assert all(not target.exists() for target in targets)


@pytest.mark.parametrize(
    "target_index",
    [0, 1],
    ids=["research-manifest", "staging-manifest"],
)
def test_existing_regular_manifest_output_target_is_accepted(
    tmp_path: Path,
    target_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    target = _manifest_output_targets(case.layout)[target_index]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"existing manifest destination")

    result = preflight_research_publication(case.layout, ())

    assert result.raw_file_count == 0
    assert target.read_bytes() == b"existing manifest destination"


@pytest.mark.parametrize(
    "target_index",
    [0, 1],
    ids=["research-manifest", "staging-manifest"],
)
def test_manifest_output_target_directory_is_rejected_with_empty_artifacts(
    tmp_path: Path,
    target_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    target = _manifest_output_targets(case.layout)[target_index]
    target.mkdir(parents=True)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())

    error = raised.value
    assert error.field == "output_dir"
    assert error.reason == "filesystem entry must be a regular file"
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    "target_index",
    [0, 1],
    ids=["research-manifest", "staging-manifest"],
)
def test_manifest_output_target_symlink_is_rejected(
    tmp_path: Path,
    target_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    target = _manifest_output_targets(case.layout)[target_index]
    target.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / f"SYNTHETIC_PRIVATE_MANIFEST_{target_index}"
    outside.write_bytes(b"private")
    _symlink_or_skip(target, outside, directory=False)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())

    error = raised.value
    assert error.field == "output_dir"
    assert "symlink or reparse point" in error.reason
    assert error.__cause__ is None
    assert error.__context__ is None
    for rendered in (str(error), repr(error), repr(vars(error))):
        assert "SYNTHETIC_PRIVATE_MANIFEST" not in rendered


@pytest.mark.parametrize(
    "target_index",
    [0, 1],
    ids=["research-manifest", "staging-manifest"],
)
def test_manifest_output_target_reparse_attribute_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    target = _manifest_output_targets(case.layout)[target_index]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"existing")
    _patch_entry_metadata(monkeypatch, target, reparse=True)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())

    error = raised.value
    assert error.field == "output_dir"
    assert "symlink or reparse point" in error.reason
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    "target_index",
    [0, 1],
    ids=["research-manifest", "staging-manifest"],
)
def test_manifest_output_target_canonical_escape_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    target = _manifest_output_targets(case.layout)[target_index]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"existing")
    outside = tmp_path / "SYNTHETIC_PRIVATE_ESCAPE" / target.name
    original = fs_module._resolve_strict

    def replacement(path: Path) -> Path:
        return outside if path == target else original(path)

    monkeypatch.setattr(fs_module, "_resolve_strict", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())

    error = raised.value
    assert error.field == "containment"
    assert error.reason == "filesystem containment could not be verified"
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "SYNTHETIC_PRIVATE_ESCAPE" not in repr(error)


@pytest.mark.parametrize(
    "target_index",
    [0, 1],
    ids=["research-manifest", "staging-manifest"],
)
def test_manifest_output_target_inspection_error_is_sanitized_and_detached(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    target = _manifest_output_targets(case.layout)[target_index]
    target.parent.mkdir(parents=True, exist_ok=True)
    _patch_lstat_failure(
        monkeypatch,
        target,
        lambda: PermissionError(
            13,
            "SYNTHETIC_PRIVATE_MANIFEST strerror",
            "SYNTHETIC_PRIVATE_MANIFEST filename",
        ),
    )

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())

    error = raised.value
    assert error.field == "output_dir"
    assert error.reason == "filesystem entry could not be inspected"
    assert error.__cause__ is None
    assert error.__context__ is None
    for rendered in (str(error), repr(error), repr(vars(error))):
        assert "SYNTHETIC_PRIVATE_MANIFEST" not in rendered


def test_existing_safe_output_descendant_directories_are_accepted(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    for path in _output_directories(case.layout):
        path.mkdir(parents=True, exist_ok=True)

    result = preflight_research_publication(case.layout, case.artifacts)

    assert result.output_directory_state == "existing"


@pytest.mark.parametrize(
    "directory_index",
    [0, 1, 2, 3, 4],
    ids=["research", "failures", "staging", "staging-research", "staging-failures"],
)
def test_output_descendant_directory_must_not_be_regular_file(
    tmp_path: Path,
    directory_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path)
    target = _output_directories(case.layout)[directory_index]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"not a directory")

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.reason == "filesystem entry must be a directory"


@pytest.mark.parametrize(
    "directory_attribute",
    ["research_dir", "failure_dir", "staging_dir"],
    ids=["research", "failures", "staging"],
)
def test_output_descendant_directory_symlink_is_rejected(
    tmp_path: Path,
    directory_attribute: str,
) -> None:
    case = _make_filesystem_case(tmp_path)
    outside = tmp_path / f"outside-{directory_attribute}"
    outside.mkdir()
    redirected = getattr(case.layout, directory_attribute)
    _symlink_or_skip(redirected, outside, directory=True)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.field == "output_dir"
    assert "symlink or reparse point" in raised.value.reason


def test_nested_output_descendant_symlink_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    case.layout.staging_dir.mkdir()
    outside = tmp_path / "outside-nested"
    outside.mkdir()
    _symlink_or_skip(case.layout.staging_research_dir, outside, directory=True)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.field == "output_dir"
    assert "symlink or reparse point" in raised.value.reason


def test_output_descendant_reparse_attribute_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    case.layout.research_dir.mkdir()
    _patch_entry_metadata(monkeypatch, case.layout.research_dir, reparse=True)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.field == "output_dir"
    assert "symlink or reparse point" in raised.value.reason


def test_output_descendant_junction_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    outside = tmp_path / "outside-junction"
    outside.mkdir()
    _junction_or_skip(case.layout.failure_dir, outside)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.field == "output_dir"
    assert "symlink or reparse point" in raised.value.reason


def test_junction_inspection_error_is_detached_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    case.layout.research_dir.mkdir()
    path_type = type(case.layout.research_dir)
    original = getattr(path_type, "is_junction", None)

    def replacement(self: Path) -> bool:
        if self == case.layout.research_dir:
            raise PermissionError(
                13,
                "SYNTHETIC_PRIVATE_MARKER strerror",
                "SYNTHETIC_PRIVATE_MARKER filename",
            )
        return False if original is None else original(self)

    monkeypatch.setattr(path_type, "is_junction", replacement, raising=False)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    error = raised.value
    assert error.field == "output_dir"
    assert error.reason == "filesystem entry could not be inspected"
    assert error.__cause__ is None
    assert error.__context__ is None
    for rendered in (str(error), repr(error), repr(vars(error))):
        assert "SYNTHETIC_PRIVATE_MARKER" not in rendered


def test_existing_regular_artifact_output_targets_are_accepted(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    for target in _artifact_output_targets(case.layout, case.artifacts[0]):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"existing output bytes")

    result = preflight_research_publication(case.layout, case.artifacts)

    assert result.raw_file_count == 1


@pytest.mark.parametrize(
    "target_index",
    [0, 1, 2, 3],
    ids=["research", "failure", "staging-research", "staging-failure"],
)
def test_artifact_output_target_symlink_is_rejected(
    tmp_path: Path,
    target_index: int,
) -> None:
    case = _make_filesystem_case(tmp_path)
    target = _artifact_output_targets(case.layout, case.artifacts[0])[target_index]
    target.parent.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / f"SYNTHETIC_PRIVATE_TARGET_{target_index}"
    outside.write_bytes(b"private")
    _symlink_or_skip(target, outside, directory=False)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    error = raised.value
    assert error.field == "output_dir"
    assert "symlink or reparse point" in error.reason
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "SYNTHETIC_PRIVATE_TARGET" not in repr(error)


def test_artifact_output_target_directory_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    target = case.layout.research_path(case.artifacts[0])
    target.mkdir(parents=True)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.field == "output_dir"
    assert raised.value.reason == "filesystem entry must be a regular file"


def test_output_descendant_containment_is_component_aware(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    case.layout.research_dir.mkdir()
    original = fs_module._resolve_strict
    sibling_prefix = tmp_path / "output-sibling" / "research"

    def replacement(path: Path) -> Path:
        if path == case.layout.research_dir:
            return sibling_prefix
        return original(path)

    monkeypatch.setattr(fs_module, "_resolve_strict", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)

    assert raised.value.field == "containment"
    assert raised.value.reason == "filesystem containment could not be verified"


def test_missing_output_existing_ancestor_symlink_is_rejected(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    target = tmp_path / "ancestor-target"
    target.mkdir()
    link = tmp_path / "ancestor-link"
    _symlink_or_skip(link, target, directory=True)
    layout = ResearchPublicationLayout(
        raw_dir=case.layout.raw_dir,
        output_dir=link / "future" / "research",
    )
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, case.artifacts)
    assert raised.value.field == "output_ancestor"
    assert "symlink or reparse point" in raised.value.reason


def test_missing_output_existing_ancestor_reparse_attribute_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    safe_parent = tmp_path / "future-parent"
    safe_parent.mkdir()
    layout = ResearchPublicationLayout(
        raw_dir=case.layout.raw_dir,
        output_dir=safe_parent / "future" / "research",
    )
    _patch_entry_metadata(monkeypatch, safe_parent, reparse=True)

    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, case.artifacts)
    assert raised.value.field == "output_ancestor"
    assert "symlink or reparse point" in raised.value.reason


def test_missing_output_without_usable_existing_ancestor_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    original = fs_module._lstat_or_missing
    anchor = Path(case.layout.output_dir.anchor)

    def replacement(path: Path, *, field: str) -> os.stat_result | None:
        if field == "output_ancestor" and path == anchor:
            return None
        return original(path, field=field)

    monkeypatch.setattr(fs_module, "_lstat_or_missing", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, case.artifacts)
    assert raised.value.field == "output_ancestor"


def test_missing_output_ancestor_inspection_error_is_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path, output_exists=False)
    safe_parent = tmp_path / "future-parent"
    safe_parent.mkdir()
    layout = ResearchPublicationLayout(
        raw_dir=case.layout.raw_dir,
        output_dir=safe_parent / "missing" / "research",
    )
    _patch_lstat_failure(
        monkeypatch,
        safe_parent,
        lambda: PermissionError(13, "SYNTHETIC_PRIVATE_MARKER", str(safe_parent)),
    )
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, case.artifacts)
    assert raised.value.field == "output_ancestor"
    assert raised.value.__context__ is None
    assert "SYNTHETIC_PRIVATE_MARKER" not in repr(raised.value)


@pytest.mark.parametrize("relationship", ["equal", "raw_nested", "output_nested"])
def test_physical_equal_or_nested_roots_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relationship: str,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    original = fs_module._resolve_strict
    canonical_raw = case.layout.raw_dir.resolve(strict=True)
    candidates = {
        "equal": canonical_raw,
        "raw_nested": canonical_raw.parent,
        "output_nested": canonical_raw / "physical-output",
    }

    def replacement(path: Path) -> Path:
        if path == case.layout.output_dir:
            return candidates[relationship]
        return original(path)

    monkeypatch.setattr(fs_module, "_resolve_strict", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())
    assert raised.value.field == "containment"
    assert raised.value.reason == "physical roots must be distinct and non-nested"


@pytest.mark.parametrize("relationship", ["equal", "raw_nested", "output_nested"])
@pytest.mark.skipif(os.name != "nt", reason="Windows case-insensitive path contract")
def test_mixed_case_physical_root_alias_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relationship: str,
) -> None:
    case = _make_filesystem_case(tmp_path, artifacts=())
    original = fs_module._resolve_strict
    canonical_raw = case.layout.raw_dir.resolve(strict=True)
    candidates = {
        "equal": Path(str(canonical_raw).swapcase()),
        "raw_nested": Path(str(canonical_raw.parent).swapcase()),
        "output_nested": Path(str(canonical_raw / "physical-output").swapcase()),
    }

    def replacement(path: Path) -> Path:
        if path == case.layout.output_dir:
            return candidates[relationship]
        return original(path)

    monkeypatch.setattr(fs_module, "_resolve_strict", replacement)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(case.layout, ())
    assert raised.value.reason == "physical roots must be distinct and non-nested"


def test_successful_preflight_does_not_change_tree_bytes_sizes_or_mtimes(
    tmp_path: Path,
) -> None:
    artifacts = (_make_artifact(), _make_artifact(symbol="ETH/USDT"))
    case = _make_filesystem_case(tmp_path, artifacts=artifacts)
    for target in _manifest_output_targets(case.layout):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"existing manifest destination")
    before = _snapshot_tree(tmp_path)

    preflight_research_publication(case.layout, case.artifacts)

    assert _snapshot_tree(tmp_path) == before


def test_failed_preflight_does_not_change_tree_bytes_sizes_or_mtimes(tmp_path: Path) -> None:
    case = _make_filesystem_case(tmp_path)
    case.layout.raw_path(case.artifacts[0]).unlink()
    before = _snapshot_tree(tmp_path)

    with pytest.raises(PublicationPreflightError):
        preflight_research_publication(case.layout, case.artifacts)

    assert _snapshot_tree(tmp_path) == before


def test_preflight_never_reads_manifest_or_raw_contents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_filesystem_case(tmp_path)
    for target in _manifest_output_targets(case.layout):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"existing manifest destination")

    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("content read attempted")

    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    monkeypatch.setattr(Path, "read_bytes", forbidden)

    result = preflight_research_publication(case.layout, case.artifacts)

    assert result.raw_file_count == 1


def test_production_source_has_no_mutation_content_read_or_runtime_assert() -> None:
    forbidden_calls = {
        "open",
        "read_text",
        "read_bytes",
        "write_text",
        "write_bytes",
        "mkdir",
        "touch",
        "unlink",
        "rmdir",
        "remove",
        "rename",
        "replace",
        "chmod",
        "chown",
        "truncate",
        "fsync",
        "exists",
        "samefile",
        "startswith",
        "sha256",
    }
    preflight_source = Path(preflight_module.__file__).read_text(encoding="utf-8")
    sources = (
        preflight_source,
        Path(fs_module.__file__).read_text(encoding="utf-8"),
    )
    for source in sources:
        tree = ast.parse(source)
        calls: list[str] = []
        forbidden_imports: list[str] = []
        broad_handlers: list[str] = []
        raises_in_except = 0
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name) and node.func.id in forbidden_calls:
                    calls.append(node.func.id)
                if isinstance(node.func, ast.Attribute) and node.func.attr in forbidden_calls:
                    calls.append(node.func.attr)
            if isinstance(node, ast.Import):
                forbidden_imports.extend(
                    name.name
                    for name in node.names
                    if name.name in {"hashlib", "shutil", "tempfile"}
                )
            if isinstance(node, ast.ImportFrom) and node.module in {
                "hashlib",
                "shutil",
                "tempfile",
            }:
                forbidden_imports.append(node.module)
            if isinstance(node, ast.ExceptHandler):
                raises_in_except += sum(isinstance(child, ast.Raise) for child in ast.walk(node))
                if isinstance(node.type, ast.Name) and node.type.id in {
                    "Exception",
                    "BaseException",
                }:
                    broad_handlers.append(node.type.id)

        assert calls == []
        assert forbidden_imports == []
        assert broad_handlers == []
        assert raises_in_except == 0
        assert not any(isinstance(node, ast.Assert) for node in ast.walk(tree))
    assert "TOCTOU" in preflight_source


def test_exact_four_lazy_exports_import_smoke() -> None:
    expected = {
        "OutputDirectoryState",
        "PublicationPreflightError",
        "PublicationPreflightResult",
        "preflight_research_publication",
    }
    actual = {
        name
        for name, module_name in datasets_package._EXPORTS.items()
        if module_name == "publication_preflight"
    }
    assert actual == expected
    assert "_publication_fs" not in set(datasets_package._EXPORTS.values())
    for name in expected:
        assert getattr(datasets_package, name) is getattr(preflight_module, name)


@pytest.mark.skipif(os.name != "nt", reason="Windows UNC policy")
def test_unc_root_is_rejected_before_any_filesystem_inspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ResearchPublicationLayout(
        raw_dir=Path("//SYNTHETIC_PRIVATE_SERVER/share/raw"),
        output_dir=Path(r"D:\safe\output"),
    )
    path_type = type(layout.raw_dir)

    def forbidden_lstat(self: Path) -> os.stat_result:
        raise AssertionError("UNC path reached filesystem inspection")

    monkeypatch.setattr(path_type, "lstat", forbidden_lstat)
    with pytest.raises(PublicationPreflightError) as raised:
        preflight_research_publication(layout, ())
    assert raised.value.field == "raw_dir"
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert "SYNTHETIC_PRIVATE_SERVER" not in repr(raised.value)
