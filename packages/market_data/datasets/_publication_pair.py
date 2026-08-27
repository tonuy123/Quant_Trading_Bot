"""Failure-first orchestration for one durable staged artifact pair.

This package-private slice composes the one-entry promotion primitive.  It
performs no filesystem work of its own and reports only sequential observations,
never pair atomicity, rename ownership, or dataset commitment.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal, NoReturn

from packages.market_data.datasets._publication_promotion import (
    PromotedEntryState,
    PromotionCategory,
    PromotionEntryError,
    PromotionOperation,
    promote_staged_entry_no_clobber,
)
from packages.market_data.datasets.conversion_manifest import ResearchFileArtifact
from packages.market_data.datasets.publication_layout import ResearchPublicationLayout

PairPromotionProgress = Literal["none_observed", "failure_observed"]
PairPromotionOperation = Literal[
    "validate_input",
    "promote_failure",
    "validate_failure_observation",
    "promote_research",
    "validate_research_observation",
]
PairPromotionCategory = Literal[
    "invalid_contract",
    "entry_failure",
    "invalid_observation",
]

_ErrorKey = tuple[PairPromotionOperation, PairPromotionCategory, PairPromotionProgress]
_ERROR_MESSAGES: MappingProxyType[_ErrorKey, str] = MappingProxyType(
    {
        ("validate_input", "invalid_contract", "none_observed"): (
            "artifact pair promotion input contract is invalid"
        ),
        ("promote_failure", "entry_failure", "none_observed"): (
            "failure sidecar promotion was not verified"
        ),
        ("validate_failure_observation", "invalid_observation", "none_observed"): (
            "failure sidecar promotion returned an invalid observation"
        ),
        ("promote_research", "entry_failure", "failure_observed"): (
            "research artifact promotion was not verified"
        ),
        ("validate_research_observation", "invalid_observation", "failure_observed"): (
            "research artifact promotion returned an invalid observation"
        ),
    }
)

_INVALID_ERROR_CONTRACT = "invalid artifact pair promotion error contract"
_INVALID_RESULT_CONTRACT = "invalid artifact pair promotion result contract"


def _valid_entry_error_pair(
    operation: PromotionOperation | None,
    category: PromotionCategory | None,
) -> bool:
    if type(operation) is not str or type(category) is not str:
        return False

    invalid_pair = False
    try:
        PromotionEntryError(operation=operation, category=category)
    except ValueError:
        invalid_pair = True
    return not invalid_pair


class ArtifactPairPromotionError(RuntimeError):
    """Sanitized pair-orchestration failure with truthful observed progress."""

    __slots__ = (
        "category",
        "entry_category",
        "entry_operation",
        "operation",
        "progress",
    )

    operation: PairPromotionOperation
    category: PairPromotionCategory
    progress: PairPromotionProgress
    entry_operation: PromotionOperation | None
    entry_category: PromotionCategory | None

    def __init__(
        self,
        *,
        operation: PairPromotionOperation,
        category: PairPromotionCategory,
        progress: PairPromotionProgress,
        entry_operation: PromotionOperation | None = None,
        entry_category: PromotionCategory | None = None,
    ) -> None:
        message: str | None = None
        if type(operation) is str and type(category) is str and type(progress) is str:
            message = _ERROR_MESSAGES.get((operation, category, progress))

        requires_entry_pair = message is not None and category == "entry_failure"
        entry_pair_is_absent = entry_operation is None and entry_category is None
        entry_pair_is_valid = requires_entry_pair and _valid_entry_error_pair(
            entry_operation, entry_category
        )
        if (
            message is None
            or (requires_entry_pair and not entry_pair_is_valid)
            or (not requires_entry_pair and not entry_pair_is_absent)
        ):
            raise ValueError(_INVALID_ERROR_CONTRACT)

        self.operation = operation
        self.category = category
        self.progress = progress
        self.entry_operation = entry_operation
        self.entry_category = entry_category
        super().__init__(message)


@dataclass(frozen=True, kw_only=True, slots=True)
class ArtifactPairPromotionResult:
    """Two sequential verified observations from failure-then-research promotion.

    The result is not a simultaneous filesystem snapshot, invocation-ownership
    proof, rename-winner token, lease, retry token, pair-atomic or dataset commit,
    or recovery token.
    """

    failure: PromotedEntryState
    research: PromotedEntryState

    def __post_init__(self) -> None:
        if (
            type(self.failure) is not PromotedEntryState
            or self.failure.kind != "failure"
            or type(self.research) is not PromotedEntryState
            or self.research.kind != "research"
        ):
            raise ValueError(_INVALID_RESULT_CONTRACT)


def _raise_pair_error(
    *,
    operation: PairPromotionOperation,
    category: PairPromotionCategory,
    progress: PairPromotionProgress,
    entry_operation: PromotionOperation | None = None,
    entry_category: PromotionCategory | None = None,
) -> NoReturn:
    raise ArtifactPairPromotionError(
        operation=operation,
        category=category,
        progress=progress,
        entry_operation=entry_operation,
        entry_category=entry_category,
    )


def _validated_observation(
    value: object,
    *,
    kind: Literal["failure", "research"],
    expected_bytes: int,
) -> PromotedEntryState | None:
    if (
        type(value) is not PromotedEntryState
        or value.kind != kind
        or value.byte_count != expected_bytes
    ):
        return None
    return value


def promote_staged_artifact_pair(
    layout: ResearchPublicationLayout,
    *,
    artifact: ResearchFileArtifact,
) -> ArtifactPairPromotionResult:
    """Promote failure then research and return sequential verified observations."""
    if type(layout) is not ResearchPublicationLayout or type(artifact) is not ResearchFileArtifact:
        _raise_pair_error(
            operation="validate_input",
            category="invalid_contract",
            progress="none_observed",
        )

    failure_child: tuple[PromotionOperation, PromotionCategory] | None = None
    try:
        failure_value = promote_staged_entry_no_clobber(
            layout,
            artifact=artifact,
            kind="failure",
        )
    except PromotionEntryError as error:
        failure_child = (error.operation, error.category)
    if failure_child is not None:
        _raise_pair_error(
            operation="promote_failure",
            category="entry_failure",
            progress="none_observed",
            entry_operation=failure_child[0],
            entry_category=failure_child[1],
        )

    failure = _validated_observation(
        failure_value,
        kind="failure",
        expected_bytes=artifact.failure_bytes,
    )
    if failure is None:
        _raise_pair_error(
            operation="validate_failure_observation",
            category="invalid_observation",
            progress="none_observed",
        )

    research_child: tuple[PromotionOperation, PromotionCategory] | None = None
    try:
        research_value = promote_staged_entry_no_clobber(
            layout,
            artifact=artifact,
            kind="research",
        )
    except PromotionEntryError as error:
        research_child = (error.operation, error.category)
    if research_child is not None:
        _raise_pair_error(
            operation="promote_research",
            category="entry_failure",
            progress="failure_observed",
            entry_operation=research_child[0],
            entry_category=research_child[1],
        )

    research = _validated_observation(
        research_value,
        kind="research",
        expected_bytes=artifact.research_bytes,
    )
    if research is None:
        _raise_pair_error(
            operation="validate_research_observation",
            category="invalid_observation",
            progress="failure_observed",
        )

    return ArtifactPairPromotionResult(failure=failure, research=research)
