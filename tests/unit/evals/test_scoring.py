"""Unit tests for sentinel.evals.scoring (4 pure-function scorers).

Uses a deterministic FakeEmbedder that maps known strings to fixed vectors,
so the scoring math is testable without spinning fastembed.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sentinel.evals.schema import GroundTruth
from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    IncidentContext,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef, SuggestedAction


class FakeEmbedder:
    """Deterministic test double for EmbeddingProvider.

    Maps known strings → unit vectors; unknown strings → orthogonal vectors
    derived from a hash so two distinct unknown strings score ~0 cosine.
    """

    def __init__(self, mapping: dict[str, list[float]] | None = None) -> None:
        self._mapping = mapping or {}

    async def embed(self, text: str) -> list[float]:
        if text in self._mapping:
            return self._mapping[text]
        # Deterministic "unknown" vector — different texts get different vectors
        # that have ~0 cosine with the known ones (which use the first slots).
        h = abs(hash(text)) % 1000
        # 8-dim vector with a 1 at slot determined by hash + offset past the
        # known basis vectors (slots 0-3 reserved for tests).
        v = [0.0] * 8
        v[4 + (h % 4)] = 1.0
        return v


# Basis vectors for known test strings: orthogonal unit vectors in slots 0..3.
# Slots 4-5 are reserved for the "orthogonal-pair" test below — pinning those
# strings into _BASIS eliminates the hash-collision flake that would otherwise
# affect FakeEmbedder's fallback path (~25% chance of slot collision under
# Python's default hash randomization).
# Intentional: "rollback the deploy" / "Revert the BGP configuration change" /
# "Roll back the deploy" all map to slot 0 — they're paraphrases that the
# production embedder would map close together, and the action-coverage test
# relies on the "perfect match" semantics.
_BASIS: dict[str, list[float]] = {
    "rollback the deploy": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "BGP misconfiguration caused route propagation failures": [
        0.0,
        1.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    ],
    "Revert the BGP configuration change": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "Roll back the deploy": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "BGP misconfig in route propagation": [
        0.0,
        0.99,
        0.141,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,  # ~0.99 cosine vs root_cause
    ],
    # Explicitly orthogonal pair for the clamp-to-zero test — pinning avoids
    # the FakeEmbedder hash collision flake.
    "completely unrelated topic A": [0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
    "totally different topic B": [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0],
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


_UNSET: list[SuggestedAction] = [
    SuggestedAction(
        description="rollback the deploy",
        command=None,
        risk="medium",
        rationale="revert",
        requires_human_approval=True,
    )
]


def _build_diagnosis(
    *,
    hypothesis: str = "BGP misconfig in route propagation",
    likely_category: str = "config",
    evidence: list[EvidenceRef] | None = None,
    suggested_actions: list[SuggestedAction] | None = None,
    confidence: float = 0.8,
) -> Diagnosis:
    return Diagnosis(
        hypothesis=hypothesis,
        confidence=confidence,
        reasoning="r",
        evidence=evidence or [EvidenceRef(kind="deploy", id="deploy:abc123", note="recent deploy")],
        # Use `is None` so an explicit empty list is honored (the "no suggested
        # actions" edge case in score_action_coverage).
        suggested_actions=_UNSET if suggested_actions is None else suggested_actions,
        likely_category=likely_category,  # CategoryType Literal — pydantic validates at runtime
    )


def _empty_context() -> IncidentContext:
    """Helper — context with one deploy and one log (typical eval seed shape)."""
    now = _utc_now()
    return IncidentContext(
        incident_id=uuid4(),
        assembled_at=now,
        recent_deploys=FetcherResult[DeployItem](
            status="ok",
            data=[
                DeployItem(
                    id="deploy:abc123",
                    service="edge",
                    sha="abc123",
                    deployed_at=now,
                )
            ],
            fetched_at=now,
        ),
        related_alerts=FetcherResult[RelatedAlertItem](status="ok", data=[], fetched_at=now),
        similar_incidents=FetcherResult[SimilarIncidentItem](status="ok", data=[], fetched_at=now),
        runbooks=FetcherResult[RunbookItem](status="ok", data=[], fetched_at=now),
        recent_logs=FetcherResult[LogLine](
            status="ok",
            data=[
                LogLine(
                    id="log:1",
                    timestamp=now,
                    level="error",
                    service="edge",
                    message="BGP reset",
                )
            ],
            fetched_at=now,
        ),
        active_alerts=FetcherResult[RelatedAlertItem](status="ok", data=[], fetched_at=now),
    )


# --- score_category ---


def test_score_category_hits_when_predicted_in_acceptable() -> None:
    from sentinel.evals.scoring import score_category

    gt = GroundTruth(
        category="config",
        acceptable_categories=["config", "deploy"],
        root_cause="x",
        correct_actions=[],
    )
    d_match = _build_diagnosis(likely_category="config")
    d_accept = _build_diagnosis(likely_category="deploy")
    d_miss = _build_diagnosis(likely_category="capacity")
    assert score_category(d_match, gt) == 1.0
    assert score_category(d_accept, gt) == 1.0
    assert score_category(d_miss, gt) == 0.0


# --- score_hypothesis ---


@pytest.mark.asyncio
async def test_score_hypothesis_returns_cosine_in_unit_interval() -> None:
    from sentinel.evals.scoring import score_hypothesis

    embedder = FakeEmbedder(_BASIS)
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config"],
        root_cause="BGP misconfiguration caused route propagation failures",
        correct_actions=[],
    )
    d = _build_diagnosis(hypothesis="BGP misconfig in route propagation")
    score = await score_hypothesis(d, gt, embedder)
    assert 0.0 <= score <= 1.0
    assert score > 0.95  # ~0.99 per the fixture basis


@pytest.mark.asyncio
async def test_score_hypothesis_clamps_to_zero_on_orthogonal() -> None:
    from sentinel.evals.scoring import score_hypothesis

    # Both strings are pinned to orthogonal vectors in _BASIS — deterministic 0.0
    # cosine. Previously this test used FakeEmbedder's hash-fallback path with a
    # 4-slot modulo, which had a ~25% chance of slot collision under Python's
    # default hash randomization → cosine 1.0 → flaky failure.
    embedder = FakeEmbedder(_BASIS)
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config"],
        root_cause="completely unrelated topic A",
        correct_actions=[],
    )
    d = _build_diagnosis(hypothesis="totally different topic B")
    score = await score_hypothesis(d, gt, embedder)
    assert score == 0.0


# --- score_action_coverage ---


@pytest.mark.asyncio
async def test_score_action_coverage_perfect_when_each_correct_has_match() -> None:
    from sentinel.evals.scoring import score_action_coverage

    embedder = FakeEmbedder(_BASIS)
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config"],
        root_cause="x",
        correct_actions=["Revert the BGP configuration change", "Roll back the deploy"],
    )
    # Both correct actions map to slot 0; suggested action also maps to slot 0
    d = _build_diagnosis(
        suggested_actions=[
            SuggestedAction(
                description="rollback the deploy",
                command=None,
                risk="medium",
                rationale="r",
                requires_human_approval=True,
            )
        ]
    )
    score = await score_action_coverage(d, gt, embedder)
    assert math.isclose(score, 1.0, abs_tol=0.01)


@pytest.mark.asyncio
async def test_score_action_coverage_returns_one_when_no_correct_actions() -> None:
    from sentinel.evals.scoring import score_action_coverage

    embedder = FakeEmbedder()
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config"],
        root_cause="x",
        correct_actions=[],
    )
    d = _build_diagnosis()
    score = await score_action_coverage(d, gt, embedder)
    assert score == 1.0


@pytest.mark.asyncio
async def test_score_action_coverage_returns_zero_when_no_suggested_actions() -> None:
    from sentinel.evals.scoring import score_action_coverage

    embedder = FakeEmbedder(_BASIS)
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config"],
        root_cause="x",
        correct_actions=["Revert the BGP configuration change"],
    )
    d = _build_diagnosis(suggested_actions=[])
    score = await score_action_coverage(d, gt, embedder)
    assert score == 0.0


# --- score_evidence_quality ---


def test_score_evidence_quality_full_when_all_ids_resolve() -> None:
    from sentinel.evals.scoring import score_evidence_quality

    ctx = _empty_context()
    d = _build_diagnosis(
        evidence=[
            EvidenceRef(kind="deploy", id="deploy:abc123", note="x"),
            EvidenceRef(kind="log", id="log:1", note="y"),
        ]
    )
    score = score_evidence_quality(d, ctx)
    assert score == 1.0


def test_score_evidence_quality_partial_when_some_invented() -> None:
    from sentinel.evals.scoring import score_evidence_quality

    ctx = _empty_context()
    d = _build_diagnosis(
        evidence=[
            EvidenceRef(kind="deploy", id="deploy:abc123", note="x"),
            EvidenceRef(kind="deploy", id="deploy:notreal", note="y"),
        ]
    )
    score = score_evidence_quality(d, ctx)
    assert score == 0.5


def test_score_evidence_quality_zero_when_no_evidence_resolves() -> None:
    from sentinel.evals.scoring import score_evidence_quality

    ctx = _empty_context()
    d = _build_diagnosis(evidence=[EvidenceRef(kind="deploy", id="deploy:notreal", note="x")])
    score = score_evidence_quality(d, ctx)
    assert score == 0.0
