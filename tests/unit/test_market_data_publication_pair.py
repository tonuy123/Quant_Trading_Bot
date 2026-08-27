"""DATA-005 C3B-2D-B/2 thin artifact-pair promotion tests."""

from __future__ import annotations

import ast
import importlib
import os
import subprocess
import sys
from collections.abc import Callable
from dataclasses import FrozenInstanceError, dataclass, fields
from hashlib import sha256
from pathlib import Path

import pytest

import packages.market_data.datasets as datasets_package
import packages.market_data.datasets._publication_pair as pair_module
import packages.market_data.datasets._publication_promotion as promotion_module
from packages.market_data.datasets._publication_pair import (
    ArtifactPairPromotionError,
    ArtifactPairPromotionResult,
    promote_staged_artifact_pair,
)
from packages.market_data.datasets._publication_promotion import (
    PromotedEntryState,
    PromotionEntryError,
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

_PRIVATE_MARKER = "C3B2D_PAIR_PRIVATE_MARKER_DO_NOT_LEAK"


@dataclass(frozen=True)
class _PairCase:
    layout: ResearchPublicationLayout
    artifact: ResearchFileArtifact
    paths: ResearchArtifactPaths
    raw_payload: bytes
    research_payload: bytes
    failure_payload: bytes


class _PromotedStateSubclass(PromotedEntryState):
    pass


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
    raw_payload: bytes = b"raw archive line one\nraw archive line two\n",
    research_payload: bytes = b"canonical research row\n",
    failure_payload: bytes = b"quarantined raw row\n",
) -> _PairCase:
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
    return _PairCase(
        layout=layout,
        artifact=artifact,
        paths=paths,
        raw_payload=raw_payload,
        research_payload=research_payload,
        failure_payload=failure_payload,
    )


def _states(
    artifact: ResearchFileArtifact,
) -> tuple[PromotedEntryState, PromotedEntryState]:
    return (
        PromotedEntryState(
            kind="failure",
            byte_count=artifact.failure_bytes,
            physical_identity=(7, 11),
        ),
        PromotedEntryState(
            kind="research",
            byte_count=artifact.research_bytes,
            physical_identity=(7, 12),
        ),
    )


def _assert_pair_error(
    error: ArtifactPairPromotionError,
    *,
    operation: str,
    category: str,
    progress: str,
    entry_operation: str | None = None,
    entry_category: str | None = None,
    markers: tuple[str, ...] = (),
) -> None:
    assert error.operation == operation
    assert error.category == category
    assert error.progress == progress
    assert error.entry_operation == entry_operation
    assert error.entry_category == entry_category
    assert error.__cause__ is None
    assert error.__context__ is None
    rendered = f"{error!s}\n{error!r}\n{vars(error)!r}"
    for marker in (_PRIVATE_MARKER, *markers):
        assert marker not in rendered
        assert all(
            marker not in repr(getattr(error, attribute))
            for attribute in (
                "operation",
                "category",
                "progress",
                "entry_operation",
                "entry_category",
            )
        )


def _assert_invalid_error_contract(call: Callable[[], object]) -> None:
    with pytest.raises(ValueError) as raised:
        call()
    error = raised.value
    assert type(error) is ValueError
    assert str(error) == "invalid artifact pair promotion error contract"
    assert vars(error) == {}
    assert not hasattr(error, "operation")
    assert not hasattr(error, "category")
    assert not hasattr(error, "progress")
    assert not hasattr(error, "entry_operation")
    assert not hasattr(error, "entry_category")
    assert _PRIVATE_MARKER not in f"{error!s}\n{error!r}\n{vars(error)!r}"
    assert error.__cause__ is None
    assert error.__context__ is None


def _invalid_observation(
    *,
    artifact: ResearchFileArtifact,
    kind: str,
    defect: str,
) -> object:
    expected_kind = "failure" if kind == "failure" else "research"
    expected_bytes = artifact.failure_bytes if kind == "failure" else artifact.research_bytes
    if defect == "wrong_type":
        return object()
    if defect == "wrong_kind":
        return PromotedEntryState(
            kind=("research" if expected_kind == "failure" else "failure"),
            byte_count=expected_bytes,
            physical_identity=None,
        )
    if defect == "wrong_bytes":
        return PromotedEntryState(
            kind=expected_kind,
            byte_count=expected_bytes + 1,
            physical_identity=None,
        )
    return _PromotedStateSubclass(
        kind=expected_kind,
        byte_count=expected_bytes,
        physical_identity=None,
    )


@pytest.mark.skipif(
    os.name != "nt",
    reason="requires real Windows no-clobber os.rename semantics",
)
@pytest.mark.parametrize(
    ("raw_payload", "research_payload", "failure_payload"),
    [
        (
            b"raw archive line one\nraw archive line two\n",
            b"canonical research row\n",
            b"quarantined raw row\n",
        ),
        (b"", b"", b""),
    ],
)
def test_real_windows_pair_success_preserves_exact_payloads(
    tmp_path: Path,
    raw_payload: bytes,
    research_payload: bytes,
    failure_payload: bytes,
) -> None:
    case = _make_case(
        tmp_path,
        raw_payload=raw_payload,
        research_payload=research_payload,
        failure_payload=failure_payload,
    )

    result = promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    assert type(result) is ArtifactPairPromotionResult
    assert result.failure.kind == "failure"
    assert result.failure.byte_count == len(failure_payload)
    assert result.research.kind == "research"
    assert result.research.byte_count == len(research_payload)
    assert not case.paths.staging_failure_path.exists()
    assert not case.paths.staging_research_path.exists()
    assert case.paths.failure_path.read_bytes() == failure_payload
    assert case.paths.research_path.read_bytes() == research_payload
    assert not case.layout.research_manifest_path.exists()
    assert not case.layout.staging_manifest_path.exists()


def test_dependency_call_order_count_and_exact_forwarding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    failure_state, research_state = _states(case.artifact)
    calls: list[tuple[object, object, str]] = []

    def fake_dependency(
        layout: object,
        *,
        artifact: object,
        kind: str,
    ) -> PromotedEntryState:
        calls.append((layout, artifact, kind))
        return failure_state if kind == "failure" else research_state

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        fake_dependency,
    )

    result = promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    assert result == ArtifactPairPromotionResult(
        failure=failure_state,
        research=research_state,
    )
    assert calls == [
        (case.layout, case.artifact, "failure"),
        (case.layout, case.artifact, "research"),
    ]


@pytest.mark.parametrize(
    (
        "phase",
        "child_operation",
        "child_category",
        "pair_operation",
        "progress",
    ),
    [
        (
            "failure",
            "inspect_source",
            "verification_mismatch",
            "promote_failure",
            "none_observed",
        ),
        (
            "research",
            "promote",
            "entry_exists",
            "promote_research",
            "failure_observed",
        ),
    ],
)
def test_child_error_is_detached_with_truthful_progress_and_no_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    child_operation: str,
    child_category: str,
    pair_operation: str,
    progress: str,
) -> None:
    case = _make_case(tmp_path)
    failure_state, _ = _states(case.artifact)
    child = PromotionEntryError(
        operation=child_operation,
        category=child_category,
    )
    calls: list[str] = []

    def fake_dependency(
        _layout: object,
        *,
        artifact: object,
        kind: str,
    ) -> PromotedEntryState:
        assert artifact is case.artifact
        calls.append(kind)
        if kind == phase:
            raise child
        return failure_state

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        fake_dependency,
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    _assert_pair_error(
        raised.value,
        operation=pair_operation,
        category="entry_failure",
        progress=progress,
        entry_operation=child_operation,
        entry_category=child_category,
    )
    assert raised.value is not child
    assert all(value is not child for value in vars(raised.value).values())
    assert calls == (["failure"] if phase == "failure" else ["failure", "research"])


def test_child_exception_object_and_sensitive_marker_are_not_retained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)

    class MarkedChildError(PromotionEntryError):
        pass

    child = MarkedChildError(operation="promote", category="io_failure")
    child.private_marker = _PRIVATE_MARKER

    def failing_dependency(
        _layout: object,
        *,
        artifact: object,
        kind: str,
    ) -> PromotedEntryState:
        assert artifact is case.artifact
        assert kind == "failure"
        raise child

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        failing_dependency,
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    _assert_pair_error(
        raised.value,
        operation="promote_failure",
        category="entry_failure",
        progress="none_observed",
        entry_operation="promote",
        entry_category="io_failure",
    )
    assert all(
        value is not child
        for value in (
            raised.value.operation,
            raised.value.category,
            raised.value.progress,
            raised.value.entry_operation,
            raised.value.entry_category,
            *raised.value.args,
            *vars(raised.value).values(),
        )
    )


@pytest.mark.parametrize(
    "defect",
    ["wrong_type", "wrong_kind", "wrong_bytes", "subclass"],
)
def test_invalid_failure_observation_stops_before_research(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    case = _make_case(tmp_path)
    invalid = _invalid_observation(
        artifact=case.artifact,
        kind="failure",
        defect=defect,
    )
    calls: list[str] = []

    def fake_dependency(
        _layout: object,
        *,
        artifact: object,
        kind: str,
    ) -> object:
        assert artifact is case.artifact
        calls.append(kind)
        return invalid

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        fake_dependency,
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    _assert_pair_error(
        raised.value,
        operation="validate_failure_observation",
        category="invalid_observation",
        progress="none_observed",
    )
    assert calls == ["failure"]


@pytest.mark.parametrize(
    "defect",
    ["wrong_type", "wrong_kind", "wrong_bytes", "subclass"],
)
def test_invalid_research_observation_reports_failure_observed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    case = _make_case(tmp_path)
    failure_state, _ = _states(case.artifact)
    invalid = _invalid_observation(
        artifact=case.artifact,
        kind="research",
        defect=defect,
    )
    calls: list[str] = []

    def fake_dependency(
        _layout: object,
        *,
        artifact: object,
        kind: str,
    ) -> object:
        assert artifact is case.artifact
        calls.append(kind)
        return failure_state if kind == "failure" else invalid

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        fake_dependency,
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    _assert_pair_error(
        raised.value,
        operation="validate_research_observation",
        category="invalid_observation",
        progress="failure_observed",
    )
    assert calls == ["failure", "research"]


def test_result_is_exact_frozen_pair_of_sequential_observations(tmp_path: Path) -> None:
    case = _make_case(tmp_path)
    failure_state, research_state = _states(case.artifact)
    result = ArtifactPairPromotionResult(
        failure=failure_state,
        research=research_state,
    )

    assert type(result) is ArtifactPairPromotionResult
    assert result.failure is failure_state
    assert result.research is research_state
    with pytest.raises(FrozenInstanceError):
        result.failure = research_state


@pytest.mark.parametrize(
    "defect",
    [
        "wrong_failure_type",
        "wrong_research_type",
        "swapped_kinds",
        "failure_subclass",
        "research_subclass",
    ],
)
def test_result_direct_constructor_rejects_invalid_contract(
    tmp_path: Path,
    defect: str,
) -> None:
    case = _make_case(tmp_path)
    failure_state, research_state = _states(case.artifact)
    failure: object = failure_state
    research: object = research_state
    if defect == "wrong_failure_type":
        failure = object()
    elif defect == "wrong_research_type":
        research = object()
    elif defect == "swapped_kinds":
        failure, research = research_state, failure_state
    elif defect == "failure_subclass":
        failure = _PromotedStateSubclass(
            kind="failure",
            byte_count=case.artifact.failure_bytes,
            physical_identity=None,
        )
    else:
        research = _PromotedStateSubclass(
            kind="research",
            byte_count=case.artifact.research_bytes,
            physical_identity=None,
        )

    with pytest.raises(ValueError) as raised:
        ArtifactPairPromotionResult(failure=failure, research=research)

    error = raised.value
    assert type(error) is ValueError
    assert str(error) == "invalid artifact pair promotion result contract"
    assert vars(error) == {}
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    (
        "operation",
        "category",
        "progress",
        "entry_operation",
        "entry_category",
        "message",
    ),
    [
        (
            "validate_input",
            "invalid_contract",
            "none_observed",
            None,
            None,
            "artifact pair promotion input contract is invalid",
        ),
        (
            "promote_failure",
            "entry_failure",
            "none_observed",
            "inspect_source",
            "io_failure",
            "failure sidecar promotion was not verified",
        ),
        (
            "validate_failure_observation",
            "invalid_observation",
            "none_observed",
            None,
            None,
            "failure sidecar promotion returned an invalid observation",
        ),
        (
            "promote_research",
            "entry_failure",
            "failure_observed",
            "promote",
            "entry_exists",
            "research artifact promotion was not verified",
        ),
        (
            "validate_research_observation",
            "invalid_observation",
            "failure_observed",
            None,
            None,
            "research artifact promotion returned an invalid observation",
        ),
    ],
)
def test_valid_error_matrix_preserves_exact_safe_contract(
    operation: str,
    category: str,
    progress: str,
    entry_operation: str | None,
    entry_category: str | None,
    message: str,
) -> None:
    error = ArtifactPairPromotionError(
        operation=operation,
        category=category,
        progress=progress,
        entry_operation=entry_operation,
        entry_category=entry_category,
    )

    assert str(error) == message
    _assert_pair_error(
        error,
        operation=operation,
        category=category,
        progress=progress,
        entry_operation=entry_operation,
        entry_category=entry_category,
    )


def test_error_message_mapping_is_immutable() -> None:
    with pytest.raises(TypeError):
        pair_module._ERROR_MESSAGES[("validate_input", "invalid_contract", "none_observed")] = (
            "changed"
        )


@pytest.mark.parametrize(
    ("operation", "category", "progress"),
    [
        ("validate_input", "invalid_contract", "failure_observed"),
        ("promote_failure", "entry_failure", "failure_observed"),
        ("promote_research", "entry_failure", "none_observed"),
        ("validate_failure_observation", "invalid_observation", "failure_observed"),
        ("validate_research_observation", "entry_failure", "failure_observed"),
    ],
)
def test_error_constructor_rejects_invalid_operation_category_progress_matrix(
    operation: str,
    category: str,
    progress: str,
) -> None:
    _assert_invalid_error_contract(
        lambda: ArtifactPairPromotionError(
            operation=operation,
            category=category,
            progress=progress,
            entry_operation=("promote" if category == "entry_failure" else None),
            entry_category=("io_failure" if category == "entry_failure" else None),
        )
    )


@pytest.mark.parametrize(
    "values",
    [
        {
            "operation": [_PRIVATE_MARKER],
            "category": "invalid_contract",
            "progress": "none_observed",
        },
        {
            "operation": "validate_input",
            "category": [_PRIVATE_MARKER],
            "progress": "none_observed",
        },
        {
            "operation": "validate_input",
            "category": "invalid_contract",
            "progress": [_PRIVATE_MARKER],
        },
        {
            "operation": True,
            "category": "invalid_contract",
            "progress": "none_observed",
        },
        {
            "operation": None,
            "category": "invalid_contract",
            "progress": "none_observed",
        },
        {
            "operation": object(),
            "category": "invalid_contract",
            "progress": "none_observed",
        },
    ],
)
def test_error_constructor_rejects_non_string_and_unhashable_fields(
    values: dict[str, object],
) -> None:
    _assert_invalid_error_contract(lambda: ArtifactPairPromotionError(**values))


@pytest.mark.parametrize(
    "entry_fields",
    [
        {"entry_operation": "promote", "entry_category": None},
        {"entry_operation": None, "entry_category": "io_failure"},
        {"entry_operation": [_PRIVATE_MARKER], "entry_category": "io_failure"},
        {"entry_operation": "promote", "entry_category": [_PRIVATE_MARKER]},
        {"entry_operation": "validate_input", "entry_category": "io_failure"},
        {"entry_operation": None, "entry_category": None},
    ],
)
def test_entry_error_fields_must_be_a_complete_valid_a2_pair(
    entry_fields: dict[str, object],
) -> None:
    _assert_invalid_error_contract(
        lambda: ArtifactPairPromotionError(
            operation="promote_failure",
            category="entry_failure",
            progress="none_observed",
            **entry_fields,
        )
    )


@pytest.mark.parametrize("invalid", [None, {}, object()])
def test_function_rejects_wrong_layout_type_before_dependency_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: object,
) -> None:
    case = _make_case(tmp_path)
    calls = 0

    def forbidden_dependency(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("dependency must not be called")

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        forbidden_dependency,
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(invalid, artifact=case.artifact)

    _assert_pair_error(
        raised.value,
        operation="validate_input",
        category="invalid_contract",
        progress="none_observed",
    )
    assert calls == 0


@pytest.mark.parametrize("invalid", [None, {}, object()])
def test_function_rejects_wrong_artifact_type_before_dependency_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: object,
) -> None:
    case = _make_case(tmp_path)
    calls = 0

    def forbidden_dependency(*_args: object, **_kwargs: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("dependency must not be called")

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        forbidden_dependency,
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(case.layout, artifact=invalid)

    _assert_pair_error(
        raised.value,
        operation="validate_input",
        category="invalid_contract",
        progress="none_observed",
    )
    assert calls == 0


def test_function_rejects_layout_subclass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)

    class LayoutSubclass(ResearchPublicationLayout):
        pass

    invalid = LayoutSubclass(
        raw_dir=case.layout.raw_dir,
        output_dir=case.layout.output_dir,
    )
    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        lambda *_args, **_kwargs: pytest.fail("dependency must not be called"),
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(invalid, artifact=case.artifact)

    _assert_pair_error(
        raised.value,
        operation="validate_input",
        category="invalid_contract",
        progress="none_observed",
    )


def test_function_rejects_artifact_subclass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)

    class ArtifactSubclass(ResearchFileArtifact):
        pass

    invalid = ArtifactSubclass(
        **{field.name: getattr(case.artifact, field.name) for field in fields(ResearchFileArtifact)}
    )
    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        lambda *_args, **_kwargs: pytest.fail("dependency must not be called"),
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(case.layout, artifact=invalid)

    _assert_pair_error(
        raised.value,
        operation="validate_input",
        category="invalid_contract",
        progress="none_observed",
    )


@pytest.mark.parametrize("phase", ["failure", "research"])
@pytest.mark.parametrize("error_type", [RuntimeError, TypeError, ValueError])
def test_unexpected_programmer_defects_propagate_unchanged_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    error_type: type[Exception],
) -> None:
    case = _make_case(tmp_path)
    failure_state, _ = _states(case.artifact)
    fault = error_type(_PRIVATE_MARKER)
    calls: list[str] = []

    def faulty_dependency(
        _layout: object,
        *,
        artifact: object,
        kind: str,
    ) -> PromotedEntryState:
        assert artifact is case.artifact
        calls.append(kind)
        if kind == phase:
            raise fault
        return failure_state

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        faulty_dependency,
    )

    with pytest.raises(error_type) as raised:
        promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    assert raised.value is fault
    assert calls == (["failure"] if phase == "failure" else ["failure", "research"])


@pytest.mark.parametrize("phase", ["failure", "research"])
@pytest.mark.parametrize("error_type", [KeyboardInterrupt, SystemExit, MemoryError])
def test_critical_failures_propagate_unchanged_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    error_type: type[BaseException],
) -> None:
    case = _make_case(tmp_path)
    failure_state, _ = _states(case.artifact)
    fault = error_type(_PRIVATE_MARKER)
    calls: list[str] = []

    def faulty_dependency(
        _layout: object,
        *,
        artifact: object,
        kind: str,
    ) -> PromotedEntryState:
        assert artifact is case.artifact
        calls.append(kind)
        if kind == phase:
            raise fault
        return failure_state

    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        faulty_dependency,
    )

    with pytest.raises(error_type) as raised:
        promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    assert raised.value is fault
    assert calls == (["failure"] if phase == "failure" else ["failure", "research"])


@pytest.mark.skipif(
    os.name != "nt",
    reason="requires real Windows no-clobber os.rename semantics",
)
def test_research_failure_keeps_observed_failure_final_without_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = _make_case(tmp_path)
    real_dependency = pair_module.promote_staged_entry_no_clobber
    real_rename = os.rename
    rename_calls: list[tuple[object, object]] = []
    calls: list[str] = []

    def tracking_rename(source: object, destination: object) -> None:
        rename_calls.append((source, destination))
        real_rename(source, destination)

    def fail_before_research_move(
        layout: ResearchPublicationLayout,
        *,
        artifact: ResearchFileArtifact,
        kind: str,
    ) -> PromotedEntryState:
        calls.append(kind)
        if kind == "failure":
            return real_dependency(layout, artifact=artifact, kind="failure")
        raise PromotionEntryError(operation="promote", category="io_failure")

    monkeypatch.setattr(promotion_module.os, "rename", tracking_rename)
    monkeypatch.setattr(
        pair_module,
        "promote_staged_entry_no_clobber",
        fail_before_research_move,
    )

    with pytest.raises(ArtifactPairPromotionError) as raised:
        promote_staged_artifact_pair(case.layout, artifact=case.artifact)

    _assert_pair_error(
        raised.value,
        operation="promote_research",
        category="entry_failure",
        progress="failure_observed",
        entry_operation="promote",
        entry_category="io_failure",
    )
    assert calls == ["failure", "research"]
    assert len(rename_calls) == 1
    assert not case.paths.staging_failure_path.exists()
    assert case.paths.failure_path.read_bytes() == case.failure_payload
    assert case.paths.staging_research_path.read_bytes() == case.research_payload
    assert not case.paths.research_path.exists()
    assert not case.layout.research_manifest_path.exists()
    assert not case.layout.staging_manifest_path.exists()


def test_python_optimized_mode_preserves_runtime_validation() -> None:
    script = """
from packages.market_data.datasets._publication_pair import (
    ArtifactPairPromotionError,
    ArtifactPairPromotionResult,
    promote_staged_artifact_pair,
)
from packages.market_data.datasets._publication_promotion import PromotedEntryState

checks = []
try:
    ArtifactPairPromotionError(
        operation=['private-marker'],
        category='invalid_contract',
        progress='none_observed',
    )
except ValueError as error:
    checks.append(
        str(error) == 'invalid artifact pair promotion error contract'
        and vars(error) == {}
        and error.__cause__ is None
        and error.__context__ is None
    )
failure = PromotedEntryState(kind='failure', byte_count=0, physical_identity=None)
research = PromotedEntryState(kind='research', byte_count=0, physical_identity=None)
try:
    ArtifactPairPromotionResult(failure=research, research=failure)
except ValueError as error:
    checks.append(str(error) == 'invalid artifact pair promotion result contract')
try:
    promote_staged_artifact_pair(None, artifact=None)
except ArtifactPairPromotionError as error:
    checks.append(
        (error.operation, error.category, error.progress)
        == ('validate_input', 'invalid_contract', 'none_observed')
        and error.__cause__ is None
        and error.__context__ is None
    )
if checks != [True, True, True]:
    raise SystemExit(1)
"""
    result = subprocess.run(
        [sys.executable, "-O", "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_package_private_module_import_smoke() -> None:
    module = importlib.import_module("packages.market_data.datasets._publication_pair")
    expected_names = {
        "PairPromotionProgress",
        "PairPromotionOperation",
        "PairPromotionCategory",
        "ArtifactPairPromotionError",
        "ArtifactPairPromotionResult",
        "promote_staged_artifact_pair",
    }
    assert all(hasattr(module, name) for name in expected_names)


def test_package_root_does_not_lazy_export_pair_api() -> None:
    private_names = {
        "PairPromotionProgress",
        "PairPromotionOperation",
        "PairPromotionCategory",
        "ArtifactPairPromotionError",
        "ArtifactPairPromotionResult",
        "promote_staged_artifact_pair",
    }
    assert private_names.isdisjoint(datasets_package.__all__)
    assert private_names.isdisjoint(datasets_package._EXPORTS)
    for name in private_names:
        with pytest.raises(AttributeError):
            getattr(datasets_package, name)


def test_production_ast_is_thin_sequential_and_filesystem_free() -> None:
    source = Path(pair_module.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    forbidden_imports = {
        "os",
        "pathlib",
        "hashlib",
        "shutil",
        "threading",
        "asyncio",
        "packages.market_data.datasets._publication_fs",
        "packages.market_data.datasets._publication_staging",
        "packages.market_data.datasets.publication_preflight",
    }
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
        "stat",
        "lstat",
        "resolve",
        "exists",
        "rename",
        "replace",
        "unlink",
        "remove",
        "delete",
        "truncate",
        "flush",
        "fsync",
        "lock",
        "acquire",
        "release",
    }
    imported: list[str] = []
    called: list[str] = []
    primitive_calls = 0
    broad_handlers: list[str] = []
    raises_in_handlers = 0
    raise_causes = 0
    dunder_mutations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.append(node.module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called.append(node.func.id)
                if node.func.id == "promote_staged_entry_no_clobber":
                    primitive_calls += 1
            elif isinstance(node.func, ast.Attribute):
                called.append(node.func.attr)
        elif isinstance(node, ast.ExceptHandler):
            if node.type is None:
                broad_handlers.append("bare")
            elif isinstance(node.type, ast.Name) and node.type.id in {
                "Exception",
                "BaseException",
            }:
                broad_handlers.append(node.type.id)
            raises_in_handlers += sum(isinstance(child, ast.Raise) for child in ast.walk(node))
        elif isinstance(node, ast.Raise) and node.cause is not None:
            raise_causes += 1
        elif (
            isinstance(node, ast.Attribute)
            and isinstance(node.ctx, ast.Store)
            and node.attr in {"__cause__", "__context__", "__traceback__"}
        ):
            dunder_mutations.append(node.attr)

    assert forbidden_imports.isdisjoint(imported)
    assert forbidden_calls.isdisjoint(called)
    assert primitive_calls == 2
    assert broad_handlers == []
    assert raises_in_handlers == 0
    assert raise_causes == 0
    assert dunder_mutations == []
    assert not any(isinstance(node, ast.Assert) for node in ast.walk(tree))
    assert not any(isinstance(node, (ast.For, ast.AsyncFor, ast.While)) for node in ast.walk(tree))
    assert not any(isinstance(node, ast.AsyncFunctionDef) for node in ast.walk(tree))
    assert "ResearchDatasetManifest" not in source
    assert "research_manifest_path" not in source
    assert "staging_manifest_path" not in source


def test_result_docstring_disclaims_ownership_atomicity_and_commit() -> None:
    docstring = ArtifactPairPromotionResult.__doc__ or ""
    assert "sequential" in docstring
    assert "not a simultaneous filesystem snapshot" in docstring
    assert "ownership" in docstring
    assert "pair-atomic" in docstring
    assert "dataset commit" in docstring
    assert "recovery token" in docstring
