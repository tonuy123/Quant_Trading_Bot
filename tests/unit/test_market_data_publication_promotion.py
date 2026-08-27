"""DATA-005 C3B-2D-A/2 one-entry no-clobber promotion tests."""

from __future__ import annotations

import ast
import builtins
import os
import stat
import subprocess
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, dataclass, fields
from hashlib import sha256
from pathlib import Path
from threading import Barrier

import pytest

import packages.market_data.datasets as datasets_package
import packages.market_data.datasets._publication_fs as fs_module
import packages.market_data.datasets._publication_promotion as promotion_module
from packages.market_data.datasets._publication_promotion import (
    PromotedEntryState,
    PromotionEntryError,
    promote_staged_entry_no_clobber,
)
from packages.market_data.datasets.conversion_manifest import (
    ResearchFileArtifact,
    ResearchFilePlan,
)
from packages.market_data.datasets.conversion_stream import StreamConversionReport
from packages.market_data.datasets.publication_layout import (
    ResearchArtifactPaths,
    ResearchPublicationLayout,
)
from packages.market_data.datasets.publication_preflight import PublicationPreflightError

_PRIVATE_MARKER = "C3B2D_PROMOTION_PRIVATE_MARKER_DO_NOT_LEAK"
_EMPTY_SHA256 = sha256(b"").hexdigest()
_UNCHANGED = object()


@dataclass(frozen=True)
class _PromotionCase:
    root: Path
    layout: ResearchPublicationLayout
    artifact: ResearchFileArtifact
    plan: ResearchFilePlan
    paths: ResearchArtifactPaths
    raw_payload: bytes
    research_payload: bytes
    failure_payload: bytes


def _make_artifact(
    *,
    raw_payload: bytes,
    research_payload: bytes,
    failure_payload: bytes,
) -> ResearchFileArtifact:
    raw_name = "BTC-USDT-1m.jsonl"
    records_written = int(bool(research_payload))
    records_quarantined = int(bool(failure_payload))
    lines_seen = records_written + records_quarantined
    if (lines_seen == 0) != (len(raw_payload) == 0):
        raise ValueError("test artifact raw/line contract is inconsistent")
    report = StreamConversionReport(
        file=raw_name,
        lines_seen=lines_seen,
        records_written=records_written,
        records_quarantined=records_quarantined,
        coverage_start_ms=(1_704_067_200_000 if records_written else None),
        coverage_end_ms=(1_704_067_260_000 if records_written else None),
        research_sha256=sha256(research_payload).hexdigest(),
        failure_sha256=sha256(failure_payload).hexdigest(),
        research_bytes=len(research_payload),
        failure_bytes=len(failure_payload),
        status=("partial" if records_quarantined else "success"),
    )
    return ResearchFileArtifact.from_stream_report(
        raw_name=raw_name,
        research_name=raw_name,
        failure_name="BTC-USDT-1m.failures.jsonl",
        symbol="BTC/USDT",
        interval="1m",
        raw_sha256=sha256(raw_payload).hexdigest(),
        raw_bytes=len(raw_payload),
        report=report,
    )


def _make_case(
    root: Path,
    *,
    raw_payload: bytes = b"raw archive bytes\nraw archive bytes 2\n",
    research_payload: bytes = b"canonical research row\n",
    failure_payload: bytes = b"quarantined raw row\n",
) -> _PromotionCase:
    raw_dir = root / "raw"
    output_dir = root / "output"
    raw_dir.mkdir(parents=True)
    output_dir.mkdir()
    artifact = _make_artifact(
        raw_payload=raw_payload,
        research_payload=research_payload,
        failure_payload=failure_payload,
    )
    plan = ResearchFilePlan(
        raw_name=artifact.raw_name,
        research_name=artifact.research_name,
        failure_name=artifact.failure_name,
        symbol=artifact.symbol,
        interval=artifact.interval,
    )
    layout = ResearchPublicationLayout(raw_dir=raw_dir, output_dir=output_dir)
    paths = layout.artifact_paths_for(plan)
    for directory in (
        layout.research_dir,
        layout.failure_dir,
        layout.staging_dir,
        layout.staging_research_dir,
        layout.staging_failure_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    layout.raw_manifest_path.write_bytes(b"raw manifest bytes")
    paths.raw_path.write_bytes(raw_payload)
    paths.staging_research_path.write_bytes(research_payload)
    paths.staging_failure_path.write_bytes(failure_payload)
    return _PromotionCase(
        root=root,
        layout=layout,
        artifact=artifact,
        plan=plan,
        paths=paths,
        raw_payload=raw_payload,
        research_payload=research_payload,
        failure_payload=failure_payload,
    )


def _selected_paths(case: _PromotionCase, kind: str) -> tuple[Path, Path, bytes]:
    if kind == "failure":
        return (
            case.paths.staging_failure_path,
            case.paths.failure_path,
            case.failure_payload,
        )
    return (
        case.paths.staging_research_path,
        case.paths.research_path,
        case.research_payload,
    )


def _run(case: _PromotionCase, kind: str) -> PromotedEntryState:
    return promote_staged_entry_no_clobber(
        case.layout,
        artifact=case.artifact,
        kind=kind,  # type: ignore[arg-type]
    )


def _assert_error(
    raised: pytest.ExceptionInfo[PromotionEntryError],
    *,
    operation: str,
    category: str,
    markers: tuple[str, ...] = (),
) -> PromotionEntryError:
    return _assert_error_value(
        raised.value,
        operation=operation,
        category=category,
        markers=markers,
    )


def _assert_error_value(
    error: PromotionEntryError,
    *,
    operation: str,
    category: str,
    markers: tuple[str, ...] = (),
) -> PromotionEntryError:
    assert error.operation == operation
    assert error.category == category
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    for marker in (_PRIVATE_MARKER, *markers):
        assert marker not in rendered
    assert set(vars(error)) <= {"operation", "category"}
    return error


def _independent_identity(path: Path) -> tuple[int, int] | None:
    metadata = path.stat()
    if type(metadata.st_dev) is int and type(metadata.st_ino) is int and metadata.st_ino != 0:
        return metadata.st_dev, metadata.st_ino
    return None


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
        size: object = _UNCHANGED,
        device: object = _UNCHANGED,
        inode: object = _UNCHANGED,
        reparse: bool = False,
    ) -> None:
        self.st_mode = metadata.st_mode
        self.st_size = metadata.st_size if size is _UNCHANGED else size
        self.st_dev = metadata.st_dev if device is _UNCHANGED else device
        self.st_ino = metadata.st_ino if inode is _UNCHANGED else inode
        attributes = getattr(metadata, "st_file_attributes", 0)
        self.st_file_attributes = (
            attributes | stat.FILE_ATTRIBUTE_REPARSE_POINT if reparse else attributes
        )


def _patch_metadata(
    monkeypatch: pytest.MonkeyPatch,
    target: Path,
    *,
    active: Callable[[], bool] = lambda: True,
    size: object = _UNCHANGED,
    device: object = _UNCHANGED,
    inode: object = _UNCHANGED,
    reparse: bool = False,
) -> None:
    original = fs_module._lstat_or_missing

    def replacement(path: Path, *, field: str) -> os.stat_result | None:
        metadata = original(path, field=field)
        if path == target and metadata is not None and active():
            return _StatProxy(  # type: ignore[return-value]
                metadata,
                size=size,
                device=device,
                inode=inode,
                reparse=reparse,
            )
        return metadata

    monkeypatch.setattr(fs_module, "_lstat_or_missing", replacement)


def _mutate_before_jit(
    monkeypatch: pytest.MonkeyPatch,
    mutation: Callable[[], None],
) -> None:
    original = promotion_module._inspect_promotion_state
    mutated = False

    def replacement(
        layout: ResearchPublicationLayout,
        artifact: ResearchFileArtifact,
        paths: ResearchArtifactPaths,
        selected: object,
        *,
        baseline: object,
    ) -> object:
        nonlocal mutated
        if baseline is not None and not mutated:
            mutation()
            mutated = True
        return original(  # type: ignore[arg-type]
            layout,
            artifact,
            paths,
            selected,
            baseline=baseline,
        )

    monkeypatch.setattr(promotion_module, "_inspect_promotion_state", replacement)


@pytest.fixture(autouse=True)
def _portable_windows_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    if os.name != "nt":
        monkeypatch.setattr(
            promotion_module,
            "_supports_windows_no_clobber_rename",
            lambda: True,
        )


@pytest.mark.parametrize("kind", ["failure", "research"])
def test_windows_success_promotes_exact_entry(tmp_path: Path, kind: str) -> None:
    case = _make_case(tmp_path)
    source, destination, payload = _selected_paths(case, kind)
    source_identity = _independent_identity(source)

    state = _run(case, kind)

    assert type(state) is PromotedEntryState
    assert state.kind == kind
    assert state.byte_count == len(payload)
    assert state.physical_identity == _independent_identity(destination)
    if source_identity is not None and state.physical_identity is not None:
        assert state.physical_identity == source_identity
    assert not source.exists()
    assert destination.read_bytes() == payload
    assert not case.layout.research_manifest_path.exists()
    assert not case.layout.staging_manifest_path.exists()


@pytest.mark.parametrize(
    ("kind", "research_payload", "failure_payload"),
    [
        ("failure", b"canonical research row\n", b""),
        ("research", b"", b"quarantined raw row\n"),
    ],
)
def test_zero_byte_stage_promotes_as_real_file(
    tmp_path: Path,
    kind: str,
    research_payload: bytes,
    failure_payload: bytes,
) -> None:
    case = _make_case(
        tmp_path,
        raw_payload=b"one raw line\n",
        research_payload=research_payload,
        failure_payload=failure_payload,
    )
    source, destination, _ = _selected_paths(case, kind)

    state = _run(case, kind)

    assert state.byte_count == 0
    assert not source.exists()
    assert destination.is_file()
    assert destination.stat().st_size == 0


@pytest.mark.parametrize("kind", ["failure", "research"])
def test_success_uses_two_jit_snapshots_and_exactly_one_canonical_os_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    case = _make_case(tmp_path)
    source, destination, _ = _selected_paths(case, kind)
    real_rename = os.rename
    rename_calls: list[tuple[Path, Path]] = []
    baseline_flags: list[bool] = []
    real_inspect = promotion_module._inspect_promotion_state

    def tracking_rename(first: object, second: object) -> None:
        rename_calls.append((Path(first), Path(second)))
        real_rename(first, second)

    def tracking_inspection(*args: object, **kwargs: object) -> object:
        baseline_flags.append(kwargs["baseline"] is not None)
        return real_inspect(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    monkeypatch.setattr(promotion_module, "_inspect_promotion_state", tracking_inspection)

    _run(case, kind)

    assert baseline_flags == [False, True]
    assert rename_calls == [
        (
            source.resolve(strict=False),
            destination.parent.resolve(strict=True) / destination.name,
        )
    ]


def test_preflight_receives_exact_artifact_tuple(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    original = promotion_module._preflight.preflight_research_publication
    calls: list[tuple[ResearchPublicationLayout, tuple[ResearchFileArtifact, ...]]] = []

    def replacement(
        layout: ResearchPublicationLayout,
        artifacts: tuple[ResearchFileArtifact, ...],
    ) -> object:
        calls.append((layout, artifacts))
        return original(layout, artifacts)

    monkeypatch.setattr(
        promotion_module._preflight,
        "preflight_research_publication",
        replacement,
    )
    _run(case, "failure")
    assert calls == [(case.layout, (case.artifact,))]


@pytest.mark.parametrize("invalid", [None, object(), "layout", 1, True])
def test_wrong_layout_type_is_rejected_before_filesystem_use(invalid: object) -> None:
    with pytest.raises(PromotionEntryError) as raised:
        promote_staged_entry_no_clobber(
            invalid,  # type: ignore[arg-type]
            artifact=object(),  # type: ignore[arg-type]
            kind="failure",
        )
    _assert_error(raised, operation="validate_input", category="invalid_contract")


def test_layout_subclass_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)

    class LayoutSubclass(ResearchPublicationLayout):
        pass

    layout = LayoutSubclass(raw_dir=case.layout.raw_dir, output_dir=case.layout.output_dir)
    with pytest.raises(PromotionEntryError) as raised:
        promote_staged_entry_no_clobber(layout, artifact=case.artifact, kind="failure")
    _assert_error(raised, operation="validate_input", category="invalid_contract")


@pytest.mark.parametrize("invalid", [None, object(), "artifact", 1, True])
def test_wrong_artifact_type_is_rejected(tmp_path: Path, invalid: object) -> None:
    case = _make_case(tmp_path)
    with pytest.raises(PromotionEntryError) as raised:
        promote_staged_entry_no_clobber(
            case.layout,
            artifact=invalid,  # type: ignore[arg-type]
            kind="failure",
        )
    _assert_error(raised, operation="validate_input", category="invalid_contract")


def test_artifact_subclass_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)

    class ArtifactSubclass(ResearchFileArtifact):
        pass

    values = {field.name: getattr(case.artifact, field.name) for field in fields(case.artifact)}
    artifact = ArtifactSubclass(**values)
    with pytest.raises(PromotionEntryError) as raised:
        promote_staged_entry_no_clobber(case.layout, artifact=artifact, kind="failure")
    _assert_error(raised, operation="validate_input", category="invalid_contract")


@pytest.mark.parametrize("invalid", ["other", True, b"failure", None, object(), 1])
def test_kind_requires_exact_allowed_string(tmp_path: Path, invalid: object) -> None:
    case = _make_case(tmp_path)
    with pytest.raises(PromotionEntryError) as raised:
        promote_staged_entry_no_clobber(
            case.layout,
            artifact=case.artifact,
            kind=invalid,  # type: ignore[arg-type]
        )
    _assert_error(raised, operation="validate_input", category="invalid_contract")


def test_non_windows_fails_closed_before_preflight_or_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    monkeypatch.setattr(
        promotion_module,
        "_supports_windows_no_clobber_rename",
        lambda: False,
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("filesystem work reached on unsupported platform")

    monkeypatch.setattr(
        promotion_module._preflight,
        "preflight_research_publication",
        forbidden,
    )
    monkeypatch.setattr(promotion_module.os, "rename", forbidden)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="validate_input",
        category="unsupported_platform",
    )


def test_missing_selected_staging_source_is_verification_mismatch(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    case.paths.staging_failure_path.unlink()
    case.paths.staging_research_path.unlink()
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_source",
        category="verification_mismatch",
    )
    assert not case.paths.failure_path.exists()


def test_wrong_kind_stage_does_not_substitute_for_selected_source(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    case.paths.staging_failure_path.unlink()
    assert case.paths.staging_research_path.is_file()
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_source",
        category="verification_mismatch",
    )


@pytest.mark.parametrize("mode", ["symlink", "reparse"])
def test_redirected_or_reparse_staging_source_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    if mode == "symlink":
        source.unlink()
        outside = tmp_path / "outside-stage.bin"
        outside.write_bytes(case.failure_payload)
        _symlink_or_skip(source, outside, directory=False)
    else:
        _patch_metadata(monkeypatch, source, reparse=True)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(raised, operation="preflight", category="unsafe_filesystem")
    assert not case.paths.failure_path.exists()


@pytest.mark.parametrize("delta", [-1, 1], ids=["smaller", "larger"])
def test_staging_source_size_must_match_artifact(
    tmp_path: Path,
    delta: int,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    source.write_bytes(b"x" * (case.artifact.failure_bytes + delta))
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_source",
        category="verification_mismatch",
    )


@pytest.mark.parametrize("mutation", ["size", "replacement"])
def test_staging_source_change_between_snapshot_and_jit_is_concurrent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    before_identity = _independent_identity(source)

    def mutate() -> None:
        if mutation == "size":
            source.write_bytes(case.failure_payload + b"x")
            return
        source.unlink()
        source.write_bytes(case.failure_payload)
        after_identity = _independent_identity(source)
        if before_identity is None or after_identity is None or before_identity == after_identity:
            pytest.skip("host did not expose staging path replacement identity")

    _mutate_before_jit(monkeypatch, mutate)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_source",
        category="concurrent_change",
    )
    assert source.exists()
    assert not case.paths.failure_path.exists()


def test_staging_source_redirection_between_snapshot_and_jit_is_concurrent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path

    def mutate() -> None:
        backup = tmp_path / "stage-backup.bin"
        source.rename(backup)
        _symlink_or_skip(source, backup, directory=False)

    _mutate_before_jit(monkeypatch, mutate)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_source",
        category="concurrent_change",
    )


def test_initial_logical_containment_violation_is_unsafe_filesystem(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path / _PRIVATE_MARKER)
    source = case.paths.staging_failure_path
    source_resolved = source.resolve(strict=True)
    original_preflight = promotion_module._run_preflight
    original_require_parent = fs_module._require_direct_parent
    preflight_complete = False

    def tracking_preflight(
        layout: ResearchPublicationLayout,
        artifact: ResearchFileArtifact,
    ) -> None:
        nonlocal preflight_complete
        original_preflight(layout, artifact)
        preflight_complete = True

    def fail_selected_containment(path: Path, *, expected_parent: Path) -> None:
        if preflight_complete and path == source_resolved:
            raise fs_module.PhysicalInspectionError(
                field=_PRIVATE_MARKER,
                reason="filesystem containment could not be verified",
            )
        original_require_parent(path, expected_parent=expected_parent)

    monkeypatch.setattr(promotion_module, "_run_preflight", tracking_preflight)
    monkeypatch.setattr(fs_module, "_require_direct_parent", fail_selected_containment)

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")

    _assert_error(
        raised,
        operation="inspect_source",
        category="unsafe_filesystem",
        markers=(str(source),),
    )
    assert source.is_file()
    assert not case.paths.failure_path.exists()


def test_jit_logical_containment_change_is_concurrent_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path / _PRIVATE_MARKER)
    source = case.paths.staging_failure_path
    source_resolved = source.resolve(strict=True)
    original_require_parent = fs_module._require_direct_parent
    jit_active = False

    def activate_jit_change() -> None:
        nonlocal jit_active
        jit_active = True

    def fail_selected_containment(path: Path, *, expected_parent: Path) -> None:
        if jit_active and path == source_resolved:
            raise fs_module.PhysicalInspectionError(
                field=_PRIVATE_MARKER,
                reason="filesystem containment could not be verified",
            )
        original_require_parent(path, expected_parent=expected_parent)

    _mutate_before_jit(monkeypatch, activate_jit_change)
    monkeypatch.setattr(fs_module, "_require_direct_parent", fail_selected_containment)

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")

    _assert_error(
        raised,
        operation="inspect_source",
        category="concurrent_change",
        markers=(str(source),),
    )
    assert source.is_file()
    assert not case.paths.failure_path.exists()


def test_genuine_resolution_os_failure_is_sanitized_io_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path / _PRIVATE_MARKER)
    source = case.paths.staging_failure_path
    original_preflight = promotion_module._run_preflight
    original_resolve = Path.resolve
    preflight_complete = False

    def tracking_preflight(
        layout: ResearchPublicationLayout,
        artifact: ResearchFileArtifact,
    ) -> None:
        nonlocal preflight_complete
        original_preflight(layout, artifact)
        preflight_complete = True

    def fail_selected_resolve(path: Path, *, strict: bool = False) -> Path:
        if preflight_complete and path == source:
            raise OSError(5, _PRIVATE_MARKER, str(source))
        return original_resolve(path, strict=strict)

    monkeypatch.setattr(promotion_module, "_run_preflight", tracking_preflight)
    monkeypatch.setattr(Path, "resolve", fail_selected_resolve)

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")

    _assert_error(
        raised,
        operation="inspect_source",
        category="io_failure",
        markers=(str(source), "[Errno 5]"),
    )
    assert source.is_file()
    assert not case.paths.failure_path.exists()


@pytest.mark.parametrize("mutation", ["size", "replacement", "redirection"])
def test_raw_change_between_snapshot_and_jit_is_concurrent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    case = _make_case(tmp_path)
    raw_path = case.paths.raw_path
    before_identity = _independent_identity(raw_path)

    def mutate() -> None:
        if mutation == "size":
            raw_path.write_bytes(case.raw_payload + b"x")
            return
        if mutation == "replacement":
            raw_path.unlink()
            raw_path.write_bytes(case.raw_payload)
            after_identity = _independent_identity(raw_path)
            if (
                before_identity is None
                or after_identity is None
                or before_identity == after_identity
            ):
                pytest.skip("host did not expose raw path replacement identity")
            return
        backup = tmp_path / "raw-backup.bin"
        raw_path.rename(backup)
        _symlink_or_skip(raw_path, backup, directory=False)

    _mutate_before_jit(monkeypatch, mutate)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_source",
        category="concurrent_change",
    )
    assert not case.paths.failure_path.exists()


def test_raw_artifact_hardlink_alias_of_manifest_is_rejected(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    case.paths.raw_path.unlink()
    try:
        os.link(case.layout.raw_manifest_path, case.paths.raw_path)
    except OSError:
        pytest.skip("host does not permit hard-link creation")
    object.__setattr__(case.artifact, "raw_bytes", case.layout.raw_manifest_path.stat().st_size)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(raised, operation="preflight", category="unsafe_filesystem")


@pytest.mark.parametrize("mode", ["wrong_kind", "symlink", "reparse"])
def test_final_parent_wrong_kind_or_redirection_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    case = _make_case(tmp_path)
    parent = case.layout.failure_dir
    if mode == "wrong_kind":
        parent.rmdir()
        parent.write_bytes(b"not a directory")
    elif mode == "symlink":
        parent.rmdir()
        outside = tmp_path / "outside-final-parent"
        outside.mkdir()
        _symlink_or_skip(parent, outside, directory=True)
    else:
        _patch_metadata(monkeypatch, parent, reparse=True)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(raised, operation="preflight", category="unsafe_filesystem")


def test_existing_regular_destination_is_not_clobbered(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    existing = b"existing destination bytes"
    case.paths.failure_path.write_bytes(existing)
    source_before = case.paths.staging_failure_path.read_bytes()
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_destination",
        category="entry_exists",
    )
    assert case.paths.failure_path.read_bytes() == existing
    assert case.paths.staging_failure_path.read_bytes() == source_before


@pytest.mark.parametrize("mode", ["directory", "symlink", "reparse"])
def test_existing_unsafe_destination_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    case = _make_case(tmp_path)
    destination = case.paths.failure_path
    if mode == "directory":
        destination.mkdir()
    elif mode == "symlink":
        outside = tmp_path / "outside-destination.bin"
        outside.write_bytes(b"outside")
        _symlink_or_skip(destination, outside, directory=False)
    else:
        destination.write_bytes(b"existing")
        _patch_metadata(monkeypatch, destination, reparse=True)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(raised, operation="preflight", category="unsafe_filesystem")
    assert case.paths.staging_failure_path.exists()


@pytest.mark.parametrize(
    ("target_name", "mode"),
    [
        ("research_manifest_path", "regular"),
        ("staging_manifest_path", "regular"),
        ("research_manifest_path", "reparse"),
        ("staging_manifest_path", "reparse"),
    ],
)
def test_existing_or_reparse_manifest_target_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_name: str,
    mode: str,
) -> None:
    case = _make_case(tmp_path)
    target = getattr(case.layout, target_name)
    target.write_bytes(b"manifest target")
    if mode == "reparse":
        _patch_metadata(monkeypatch, target, reparse=True)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    if mode == "regular":
        _assert_error(
            raised,
            operation="inspect_destination",
            category="entry_exists",
        )
    else:
        _assert_error(raised, operation="preflight", category="unsafe_filesystem")
    assert target.read_bytes() == b"manifest target"


def test_cross_device_metadata_is_rejected_before_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    source_device = case.paths.staging_failure_path.stat().st_dev
    _patch_metadata(
        monkeypatch,
        case.layout.failure_dir,
        device=source_device + 1,
    )

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("rename reached after cross-device metadata")

    monkeypatch.setattr(promotion_module.os, "rename", forbidden)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="inspect_destination",
        category="unsafe_filesystem",
    )


@pytest.mark.skipif(os.name != "nt", reason="requires real Windows os.rename no-clobber")
def test_last_moment_destination_race_never_clobbers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    raced_bytes = b"racer destination"
    real_rename = os.rename
    calls = 0

    def racing_rename(first: object, second: object) -> None:
        nonlocal calls
        calls += 1
        Path(second).write_bytes(raced_bytes)
        real_rename(first, second)

    monkeypatch.setattr(promotion_module.os, "rename", racing_rename)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(raised, operation="promote", category="entry_exists")
    assert calls == 1
    assert destination.read_bytes() == raced_bytes
    assert source.read_bytes() == case.failure_payload


@pytest.mark.skipif(os.name != "nt", reason="requires real Windows os.rename no-clobber")
def test_identical_concurrent_calls_allow_observational_agreement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    source_payload = case.failure_payload
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    real_rename = os.rename
    barrier = Barrier(2)
    rename_calls = 0

    def synchronized_rename(first: object, second: object) -> None:
        nonlocal rename_calls
        rename_calls += 1
        barrier.wait(timeout=10)
        real_rename(first, second)

    monkeypatch.setattr(promotion_module.os, "rename", synchronized_rename)

    def invoke() -> PromotedEntryState | PromotionEntryError:
        try:
            return _run(case, "failure")
        except PromotionEntryError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _index: invoke(), range(2)))

    successes = [result for result in results if type(result) is PromotedEntryState]
    failures = [result for result in results if type(result) is PromotionEntryError]

    assert len(successes) + len(failures) == 2
    assert successes
    final_identity = _independent_identity(destination)
    for state in successes:
        assert state.kind == "failure"
        assert state.byte_count == len(source_payload)
        assert state.physical_identity == final_identity
    if len(successes) == 2:
        assert successes[0] == successes[1]
    for error in failures:
        assert (error.operation, error.category) in {
            ("promote", "entry_exists"),
            ("promote", "io_failure"),
            ("verify_destination", "concurrent_change"),
            ("verify_destination", "io_failure"),
        }
        _assert_error_value(
            error,
            operation=error.operation,
            category=error.category,
            markers=(str(source), str(destination)),
        )

    # Both invocations reached their one allowed rename call. This count does
    # not identify a physical winner; only the final filesystem state does.
    assert rename_calls == 2
    assert not source.exists()
    assert destination.is_file()
    assert destination.read_bytes() == source_payload
    assert {entry.resolve() for entry in case.layout.failure_dir.iterdir()} == {
        destination.resolve()
    }


@pytest.mark.skipif(os.name != "nt", reason="requires real Windows os.rename no-clobber")
def test_two_different_sources_race_one_destination_only_one_wins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_a = tmp_path / "source-a.bin"
    source_b = tmp_path / "source-b.bin"
    destination = tmp_path / "destination.bin"
    payload_a = b"first distinct staged payload"
    payload_b = b"second distinct staged payload"
    source_a.write_bytes(payload_a)
    source_b.write_bytes(payload_b)
    real_rename = os.rename
    barrier = Barrier(2)
    rename_calls = 0

    def synchronized_rename(first: object, second: object) -> None:
        nonlocal rename_calls
        rename_calls += 1
        barrier.wait(timeout=10)
        real_rename(first, second)

    monkeypatch.setattr(promotion_module.os, "rename", synchronized_rename)

    def invoke(source: Path) -> PromotionEntryError | None:
        try:
            promotion_module._perform_rename(source, destination)
        except PromotionEntryError as error:
            return error
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(invoke, (source_a, source_b)))

    failures = [result for result in results if type(result) is PromotionEntryError]
    assert rename_calls == 2
    assert results.count(None) == 1
    assert len(failures) == 1
    _assert_error_value(
        failures[0],
        operation="promote",
        category="entry_exists",
        markers=(str(source_a), str(source_b), str(destination)),
    )

    assert destination.is_file()
    assert source_a.exists() != source_b.exists()
    if source_a.exists():
        losing_source = source_a
        losing_payload = payload_a
        winning_payload = payload_b
    else:
        losing_source = source_b
        losing_payload = payload_b
        winning_payload = payload_a
    assert losing_source.read_bytes() == losing_payload
    assert destination.read_bytes() == winning_payload
    assert {entry.resolve() for entry in tmp_path.iterdir()} == {
        losing_source.resolve(),
        destination.resolve(),
    }


@pytest.mark.parametrize("mismatch", ["size", "identity", "redirection"])
def test_post_rename_destination_mismatch_fails_without_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mismatch: str,
) -> None:
    case = _make_case(tmp_path)
    destination = case.paths.failure_path
    real_rename = os.rename
    renamed = False

    def tracking_rename(first: object, second: object) -> None:
        nonlocal renamed
        real_rename(first, second)
        renamed = True

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    if mismatch == "size":
        _patch_metadata(
            monkeypatch,
            destination,
            active=lambda: renamed,
            size=case.artifact.failure_bytes + 1,
        )
    elif mismatch == "identity":
        source_identity = _independent_identity(case.paths.staging_failure_path)
        if source_identity is None:
            pytest.skip("host does not expose usable source identity")
        _patch_metadata(
            monkeypatch,
            destination,
            active=lambda: renamed,
            inode=source_identity[1] + 100_003,
        )
    else:
        original_resolve = fs_module._resolve_strict

        def redirected_resolve(path: Path) -> Path:
            resolved = original_resolve(path)
            if renamed and path == destination:
                return tmp_path / "outside-final" / destination.name
            return resolved

        monkeypatch.setattr(fs_module, "_resolve_strict", redirected_resolve)

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    expected_category = (
        "concurrent_change" if mismatch == "redirection" else "verification_mismatch"
    )
    _assert_error(
        raised,
        operation="verify_destination",
        category=expected_category,
    )
    assert renamed
    assert destination.exists()
    assert not case.paths.staging_failure_path.exists()


def test_second_destination_size_observation_is_concurrent_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    changed_payload = case.failure_payload + b"x"
    real_rename = os.rename
    original_inspect = promotion_module._inspect_promoted_destination
    rename_calls = 0
    first_observation_verified = False

    def tracking_rename(first: object, second: object) -> None:
        nonlocal rename_calls
        rename_calls += 1
        real_rename(first, second)

    def mutate_after_first_observation(
        selected: object,
        snapshot: object,
        *,
        previous: object,
    ) -> object:
        nonlocal first_observation_verified
        result = original_inspect(  # type: ignore[arg-type]
            selected,
            snapshot,
            previous=previous,
        )
        if previous is None:
            assert result.size == case.artifact.failure_bytes
            destination.write_bytes(changed_payload)
            first_observation_verified = True
        return result

    def forbidden_delete(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("size-drift verification attempted cleanup")

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    monkeypatch.setattr(
        promotion_module,
        "_inspect_promoted_destination",
        mutate_after_first_observation,
    )
    monkeypatch.setattr(os, "remove", forbidden_delete)
    monkeypatch.setattr(Path, "unlink", forbidden_delete)

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")

    _assert_error(
        raised,
        operation="verify_destination",
        category="concurrent_change",
    )
    assert first_observation_verified
    assert rename_calls == 1
    assert destination.read_bytes() == changed_payload
    assert not source.exists()


@pytest.mark.parametrize(
    ("scenario", "succeeds"),
    [
        ("source_usable_destination_unavailable", False),
        ("source_unavailable_destination_usable", False),
        ("both_unavailable", True),
        ("usable_equal", True),
        ("usable_different", False),
    ],
)
def test_source_to_destination_identity_availability_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    succeeds: bool,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    source_identity = _independent_identity(source)
    if scenario != "both_unavailable" and source_identity is None:
        pytest.skip("host does not expose usable source identity for availability test")

    renamed = False
    real_rename = os.rename

    def tracking_rename(first: object, second: object) -> None:
        nonlocal renamed
        real_rename(first, second)
        renamed = True

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    if scenario in {"source_unavailable_destination_usable", "both_unavailable"}:
        _patch_metadata(monkeypatch, source, inode=0)
    if scenario in {"source_usable_destination_unavailable", "both_unavailable"}:
        _patch_metadata(
            monkeypatch,
            destination,
            active=lambda: renamed,
            inode=0,
        )
    elif scenario == "usable_different":
        if source_identity is None:
            pytest.skip("host does not expose usable source identity for mismatch test")
        _patch_metadata(
            monkeypatch,
            destination,
            active=lambda: renamed,
            inode=source_identity[1] + 100_019,
        )

    if succeeds:
        state = _run(case, "failure")
        expected_identity = None if scenario == "both_unavailable" else source_identity
        assert state.physical_identity == expected_identity
    else:
        with pytest.raises(PromotionEntryError) as raised:
            _run(case, "failure")
        _assert_error(
            raised,
            operation="verify_destination",
            category="verification_mismatch",
        )
    assert renamed
    assert destination.is_file()
    assert not source.exists()


@pytest.mark.parametrize(
    ("scenario", "succeeds"),
    [
        ("source_usable_destination_unavailable", False),
        ("source_unavailable_destination_usable", False),
        ("both_unavailable", True),
        ("usable_equal", True),
        ("usable_different", False),
    ],
)
def test_source_to_destination_device_availability_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    succeeds: bool,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    source_device = source.stat().st_dev
    if type(source_device) is not int:
        pytest.skip("host does not expose usable source device metadata")

    renamed = False
    real_rename = os.rename

    def tracking_rename(first: object, second: object) -> None:
        nonlocal renamed
        real_rename(first, second)
        renamed = True

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    source_device_value: object = (
        None
        if scenario in {"source_unavailable_destination_usable", "both_unavailable"}
        else _UNCHANGED
    )
    destination_device_value: object = (
        None
        if scenario in {"source_usable_destination_unavailable", "both_unavailable"}
        else _UNCHANGED
    )
    if scenario == "usable_different":
        destination_device_value = source_device + 1

    _patch_metadata(
        monkeypatch,
        source,
        inode=0,
        device=source_device_value,
    )
    _patch_metadata(
        monkeypatch,
        destination,
        active=lambda: renamed,
        inode=0,
        device=destination_device_value,
    )

    if succeeds:
        state = _run(case, "failure")
        assert state.physical_identity is None
    else:
        with pytest.raises(PromotionEntryError) as raised:
            _run(case, "failure")
        _assert_error(
            raised,
            operation="verify_destination",
            category="verification_mismatch",
        )
    assert renamed
    assert destination.is_file()
    assert not source.exists()


@pytest.mark.parametrize("surface", ["identity", "device", "size"])
def test_malformed_destination_metadata_is_fail_closed_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
) -> None:
    case = _make_case(tmp_path / _PRIVATE_MARKER)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    if surface == "identity" and _independent_identity(source) is None:
        pytest.skip("host does not expose usable source identity for malformed metadata test")

    renamed = False
    real_rename = os.rename

    def tracking_rename(first: object, second: object) -> None:
        nonlocal renamed
        real_rename(first, second)
        renamed = True

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    if surface == "identity":
        _patch_metadata(
            monkeypatch,
            destination,
            active=lambda: renamed,
            inode=True,
        )
    elif surface == "device":
        _patch_metadata(monkeypatch, source, inode=0)
        _patch_metadata(
            monkeypatch,
            destination,
            active=lambda: renamed,
            inode=0,
            device=True,
        )
    else:
        _patch_metadata(
            monkeypatch,
            destination,
            active=lambda: renamed,
            size=True,
        )

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")

    _assert_error(
        raised,
        operation="verify_destination",
        category="verification_mismatch",
        markers=(str(source), str(destination)),
    )
    assert renamed
    assert destination.is_file()
    assert not source.exists()


@pytest.mark.parametrize(
    ("surface", "direction"),
    [
        ("identity", "usable_to_unavailable"),
        ("identity", "unavailable_to_usable"),
        ("device", "usable_to_unavailable"),
        ("device", "unavailable_to_usable"),
    ],
)
def test_destination_metadata_availability_change_between_reinspections_is_concurrent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    direction: str,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    if surface == "identity" and _independent_identity(source) is None:
        pytest.skip("host does not expose usable identity for final reinspection test")

    if direction == "unavailable_to_usable":
        _patch_metadata(
            monkeypatch,
            source,
            inode=0,
            device=(None if surface == "device" else _UNCHANGED),
        )
    elif surface == "device":
        _patch_metadata(monkeypatch, source, inode=0)

    original_lstat = fs_module._lstat_or_missing
    real_rename = os.rename
    renamed = False
    destination_observations = 0

    def tracking_rename(first: object, second: object) -> None:
        nonlocal renamed
        real_rename(first, second)
        renamed = True

    def phased_destination_metadata(
        path: Path,
        *,
        field: str,
    ) -> os.stat_result | None:
        nonlocal destination_observations
        metadata = original_lstat(path, field=field)
        if not renamed or path != destination or metadata is None:
            return metadata
        destination_observations += 1
        unavailable = (
            destination_observations == 2
            if direction == "usable_to_unavailable"
            else destination_observations == 1
        )
        if surface == "identity":
            return _StatProxy(metadata, inode=(0 if unavailable else _UNCHANGED))  # type: ignore[return-value]
        return _StatProxy(  # type: ignore[return-value]
            metadata,
            inode=0,
            device=(None if unavailable else _UNCHANGED),
        )

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    monkeypatch.setattr(fs_module, "_lstat_or_missing", phased_destination_metadata)

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")

    _assert_error(
        raised,
        operation="verify_destination",
        category="concurrent_change",
    )
    assert destination_observations == 2
    assert destination.is_file()
    assert not source.exists()


@pytest.mark.parametrize("mutation", ["disappearance", "replacement"])
def test_destination_disappearance_or_replacement_between_final_inspections_is_concurrent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    payload = case.failure_payload
    original_inspect = promotion_module._inspect_promoted_destination
    mutated = False

    def mutate_after_first_inspection(
        selected: object,
        snapshot: object,
        *,
        previous: object,
    ) -> object:
        nonlocal mutated
        result = original_inspect(  # type: ignore[arg-type]
            selected,
            snapshot,
            previous=previous,
        )
        if previous is None and not mutated:
            destination.unlink()
            if mutation == "replacement":
                destination.write_bytes(payload)
                replacement_identity = _independent_identity(destination)
                if (
                    result.identity is None
                    or replacement_identity is None
                    or result.identity == replacement_identity
                ):
                    pytest.skip("host did not expose destination replacement identity")
            mutated = True
        return result

    monkeypatch.setattr(
        promotion_module,
        "_inspect_promoted_destination",
        mutate_after_first_inspection,
    )

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")

    _assert_error(
        raised,
        operation="verify_destination",
        category="concurrent_change",
    )
    assert mutated
    assert not source.exists()
    if mutation == "disappearance":
        assert not destination.exists()
    else:
        assert destination.read_bytes() == payload


def test_post_rename_source_unexpectedly_remaining_is_concurrent_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    payload = source.read_bytes()
    real_rename = os.rename

    def rename_and_recreate(first: object, second: object) -> None:
        real_rename(first, second)
        Path(first).write_bytes(payload)

    monkeypatch.setattr(promotion_module.os, "rename", rename_and_recreate)
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="verify_destination",
        category="concurrent_change",
    )
    assert source.read_bytes() == payload
    assert destination.read_bytes() == payload


def test_post_rename_verification_failure_never_deletes_or_renames_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    destination = case.paths.failure_path
    real_rename = os.rename
    renamed = False
    rename_calls = 0

    def tracking_rename(first: object, second: object) -> None:
        nonlocal rename_calls, renamed
        rename_calls += 1
        real_rename(first, second)
        renamed = True

    def forbidden_delete(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("post-rename rollback/delete attempted")

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    monkeypatch.setattr(os, "remove", forbidden_delete)
    monkeypatch.setattr(Path, "unlink", forbidden_delete)
    _patch_metadata(
        monkeypatch,
        destination,
        active=lambda: renamed,
        size=case.artifact.failure_bytes + 1,
    )
    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation="verify_destination",
        category="verification_mismatch",
    )
    assert rename_calls == 1
    assert destination.exists()
    assert not case.paths.staging_failure_path.exists()


@pytest.mark.parametrize(
    ("phase", "operation", "category"),
    [
        ("preflight", "preflight", "unsafe_filesystem"),
        ("source", "inspect_source", "io_failure"),
        ("rename", "promote", "io_failure"),
        ("verify", "verify_destination", "io_failure"),
    ],
)
def test_expected_dependency_and_os_errors_are_detached_and_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    operation: str,
    category: str,
) -> None:
    case = _make_case(tmp_path / _PRIVATE_MARKER)
    source = case.paths.staging_failure_path
    destination = case.paths.failure_path
    renamed = False
    real_rename = os.rename
    original_required = fs_module._inspect_required_entry

    if phase == "preflight":

        def fail_preflight(*_args: object, **_kwargs: object) -> object:
            raise PublicationPreflightError(field=_PRIVATE_MARKER, reason=_PRIVATE_MARKER)

        monkeypatch.setattr(
            promotion_module._preflight,
            "preflight_research_publication",
            fail_preflight,
        )
    elif phase == "source":

        def fail_source(path: Path, *, field: str, kind: object) -> object:
            if path == source:
                raise fs_module.PhysicalInspectionError(
                    field=_PRIVATE_MARKER,
                    reason="filesystem entry could not be inspected",
                )
            return original_required(path, field=field, kind=kind)  # type: ignore[arg-type]

        monkeypatch.setattr(fs_module, "_inspect_required_entry", fail_source)
    elif phase == "rename":

        def fail_rename(*_args: object, **_kwargs: object) -> object:
            raise OSError(5, _PRIVATE_MARKER, str(destination))

        monkeypatch.setattr(promotion_module.os, "rename", fail_rename)
    else:

        def tracking_rename(first: object, second: object) -> None:
            nonlocal renamed
            real_rename(first, second)
            renamed = True

        def fail_verify(path: Path, *, field: str, kind: object) -> object:
            if renamed and path == destination:
                raise fs_module.PhysicalInspectionError(
                    field=_PRIVATE_MARKER,
                    reason="filesystem entry could not be inspected",
                )
            return original_required(path, field=field, kind=kind)  # type: ignore[arg-type]

        monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
        monkeypatch.setattr(fs_module, "_inspect_required_entry", fail_verify)

    with pytest.raises(PromotionEntryError) as raised:
        _run(case, "failure")
    _assert_error(
        raised,
        operation=operation,
        category=category,
        markers=(str(source), str(destination)),
    )


@pytest.mark.parametrize("critical", [KeyboardInterrupt, SystemExit, MemoryError])
def test_critical_rename_failures_propagate_without_translation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    critical: type[BaseException],
) -> None:
    case = _make_case(tmp_path)
    marker = critical(_PRIVATE_MARKER)

    def fail(*_args: object, **_kwargs: object) -> object:
        raise marker

    monkeypatch.setattr(promotion_module.os, "rename", fail)
    with pytest.raises(critical) as raised:
        _run(case, "failure")
    assert raised.value is marker
    assert case.paths.staging_failure_path.exists()
    assert not case.paths.failure_path.exists()


def test_unexpected_programmer_defect_propagates_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    marker = TypeError(_PRIVATE_MARKER)

    def fail(*_args: object, **_kwargs: object) -> object:
        raise marker

    monkeypatch.setattr(promotion_module, "_inspect_promotion_state", fail)
    with pytest.raises(TypeError) as raised:
        _run(case, "failure")
    assert raised.value is marker
    assert case.paths.staging_failure_path.exists()
    assert not case.paths.failure_path.exists()


@pytest.mark.parametrize(
    ("operation", "category"),
    [
        (f"{_PRIVATE_MARKER}_operation", "invalid_contract"),
        ("validate_input", f"{_PRIVATE_MARKER}_category"),
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
def test_invalid_error_constructor_is_fixed_detached_and_marker_free(
    operation: object,
    category: object,
) -> None:
    with pytest.raises(ValueError) as raised:
        PromotionEntryError(
            operation=operation,  # type: ignore[arg-type]
            category=category,  # type: ignore[arg-type]
        )
    error = raised.value
    assert type(error) is ValueError
    assert str(error) == "invalid promotion entry error contract"
    assert vars(error) == {}
    assert not hasattr(error, "operation")
    assert not hasattr(error, "category")
    assert _PRIVATE_MARKER not in f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    ("operation", "category", "message"),
    [
        ("validate_input", "invalid_contract", "promotion input contract is invalid"),
        (
            "validate_input",
            "unsupported_platform",
            "no-clobber promotion requires Windows",
        ),
        (
            "preflight",
            "unsafe_filesystem",
            "publication preflight rejected the physical layout",
        ),
        ("inspect_source", "unsafe_filesystem", "staging source is not safe"),
        ("inspect_source", "io_failure", "staging source could not be inspected"),
        (
            "inspect_source",
            "verification_mismatch",
            "staging source does not match the artifact contract",
        ),
        (
            "inspect_source",
            "concurrent_change",
            "staging source changed during promotion",
        ),
        (
            "inspect_destination",
            "unsafe_filesystem",
            "promotion destination is not safe",
        ),
        (
            "inspect_destination",
            "entry_exists",
            "promotion destination already exists",
        ),
        (
            "inspect_destination",
            "io_failure",
            "promotion destination could not be inspected",
        ),
        (
            "inspect_destination",
            "concurrent_change",
            "promotion destination changed during promotion",
        ),
        ("promote", "entry_exists", "promotion destination appeared concurrently"),
        ("promote", "io_failure", "staging entry could not be promoted"),
        ("verify_destination", "unsafe_filesystem", "promoted entry is not safe"),
        (
            "verify_destination",
            "io_failure",
            "promoted entry could not be inspected",
        ),
        (
            "verify_destination",
            "verification_mismatch",
            "promoted entry does not match the artifact contract",
        ),
        (
            "verify_destination",
            "concurrent_change",
            "promoted entry changed during verification",
        ),
    ],
)
def test_valid_error_mapping_preserves_exact_contract(
    operation: str,
    category: str,
    message: str,
) -> None:
    error = PromotionEntryError(
        operation=operation,  # type: ignore[arg-type]
        category=category,  # type: ignore[arg-type]
    )
    assert error.operation == operation
    assert error.category == category
    assert str(error) == message


def test_error_mapping_is_immutable() -> None:
    with pytest.raises(TypeError):
        promotion_module._ERROR_MESSAGES[("promote", "io_failure")] = "changed"  # type: ignore[index]


@pytest.mark.parametrize(
    ("kind", "byte_count", "identity"),
    [
        (f"{_PRIVATE_MARKER}_kind", 0, None),
        (True, 0, None),
        ("failure", True, None),
        ("failure", -1, None),
        ("failure", 1.5, None),
        ("failure", 0, [1, 2]),
        ("failure", 0, (1,)),
        ("failure", 0, (1, 2, 3)),
        ("failure", 0, (True, 2)),
        ("failure", 0, (1, False)),
        ("failure", 0, (1, 0)),
        ("failure", 0, (1.0, 2)),
    ],
)
def test_promoted_state_direct_constructor_is_strict_and_sanitized(
    kind: object,
    byte_count: object,
    identity: object,
) -> None:
    with pytest.raises(ValueError) as raised:
        PromotedEntryState(
            kind=kind,  # type: ignore[arg-type]
            byte_count=byte_count,  # type: ignore[arg-type]
            physical_identity=identity,  # type: ignore[arg-type]
        )
    error = raised.value
    assert type(error) is ValueError
    assert str(error) == "invalid promoted entry state contract"
    assert vars(error) == {}
    assert _PRIVATE_MARKER not in f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize("identity", [None, (7, 11)])
def test_promoted_state_valid_values_are_frozen(identity: tuple[int, int] | None) -> None:
    state = PromotedEntryState(
        kind="failure",
        byte_count=0,
        physical_identity=identity,
    )
    assert state.physical_identity == identity
    with pytest.raises(FrozenInstanceError):
        state.byte_count = 1  # type: ignore[misc]


def test_python_optimized_mode_preserves_all_direct_validation() -> None:
    script = f"""
from packages.market_data.datasets._publication_promotion import (
    PromotedEntryState,
    PromotionEntryError,
    promote_staged_entry_no_clobber,
)
marker = {_PRIVATE_MARKER!r}
for call, expected in (
    (lambda: PromotionEntryError(operation=[marker], category='io_failure'),
     'invalid promotion entry error contract'),
    (lambda: PromotedEntryState(kind='failure', byte_count=True, physical_identity=None),
     'invalid promoted entry state contract'),
):
    try:
        call()
    except ValueError as error:
        if str(error) != expected or marker in repr(error):
            raise SystemExit(2)
        if error.__cause__ is not None or error.__context__ is not None:
            raise SystemExit(3)
    else:
        raise SystemExit(4)
try:
    promote_staged_entry_no_clobber(object(), artifact=object(), kind='failure')
except PromotionEntryError as error:
    if (error.operation, error.category) != ('validate_input', 'invalid_contract'):
        raise SystemExit(5)
else:
    raise SystemExit(6)
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_production_ast_allows_only_one_rename_and_no_forbidden_operations() -> None:
    source = Path(promotion_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden_calls = {
        "open",
        "read",
        "read_bytes",
        "read_text",
        "readall",
        "readlines",
        "write",
        "write_bytes",
        "write_text",
        "flush",
        "fsync",
        "close",
        "mkdir",
        "touch",
        "unlink",
        "remove",
        "rmdir",
        "delete",
        "truncate",
        "replace",
        "move",
        "eval",
        "exec",
    }
    calls: list[str] = []
    rename_calls: list[ast.Call] = []
    broad_handlers: list[str] = []
    raises_in_handlers = 0
    forbidden_imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in forbidden_calls:
                calls.append(node.func.id)
            if isinstance(node.func, ast.Attribute):
                if node.func.attr in forbidden_calls:
                    calls.append(node.func.attr)
                if node.func.attr == "rename":
                    rename_calls.append(node)
        if isinstance(node, ast.ExceptHandler):
            if node.type is None:
                broad_handlers.append("bare")
            elif isinstance(node.type, ast.Name) and node.type.id in {
                "Exception",
                "BaseException",
            }:
                broad_handlers.append(node.type.id)
            raises_in_handlers += sum(isinstance(child, ast.Raise) for child in ast.walk(node))
        if isinstance(node, ast.Import):
            forbidden_imports.extend(
                name.name
                for name in node.names
                if name.name in {"hashlib", "random", "shutil", "tempfile"}
            )
        if isinstance(node, ast.ImportFrom) and node.module in {
            "hashlib",
            "random",
            "shutil",
            "tempfile",
        }:
            forbidden_imports.append(node.module)

    assert calls == []
    assert len(rename_calls) == 1
    rename = rename_calls[0]
    assert isinstance(rename.func, ast.Attribute)
    assert isinstance(rename.func.value, ast.Name)
    assert rename.func.value.id == "os"
    assert len(rename.args) == 2
    assert broad_handlers == []
    assert raises_in_handlers == 0
    assert forbidden_imports == []
    assert not any(isinstance(node, ast.Assert) for node in ast.walk(tree))


def test_package_private_api_is_not_lazy_exported() -> None:
    private_names = {
        "PromotionEntryError",
        "PromotedEntryState",
        "promote_staged_entry_no_clobber",
    }
    assert private_names.isdisjoint(datasets_package.__all__)
    assert private_names.isdisjoint(datasets_package._EXPORTS)
    for name in private_names:
        with pytest.raises(AttributeError):
            getattr(datasets_package, name)


def test_runtime_promotion_never_reads_hashes_or_writes_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("content I/O attempted by promotion")

    monkeypatch.setattr(builtins, "open", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(Path, "read_bytes", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    monkeypatch.setattr(Path, "write_bytes", forbidden)
    monkeypatch.setattr(Path, "write_text", forbidden)
    state = _run(case, "failure")
    assert state.byte_count == case.artifact.failure_bytes
    assert not case.paths.staging_failure_path.exists()
    assert case.paths.failure_path.stat().st_size == case.artifact.failure_bytes
