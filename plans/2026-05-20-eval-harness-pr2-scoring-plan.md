# Eval Harness PR 2 — Scoring + Stats Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the pure-function scoring layer for the eval harness — Pydantic schemas for `GroundTruth` / `MetricSet` / `RegressionResult`, four scoring functions in `scoring.py`, and the paired-bootstrap regression gate in `stats.py`. No runner, no CI changes, no DB. Fully independent of PR 1 (the schema PR) — can land in parallel.

**Architecture:** Three new files under `sentinel/evals/` — `schema.py` (Pydantic models for the corpus ground truth + score outputs + regression verdicts), `scoring.py` (4 async functions that take a `Diagnosis`, a `GroundTruth`, and either an `EmbeddingProvider` or an `IncidentContext`), and `stats.py` (bootstrap CI + paired-difference regression gate). Scoring is async because the `EmbeddingProvider` Protocol from `sentinel/enrichment/protocols.py` is async (matches the production fastembed provider). Stats is pure sync NumPy.

**Tech Stack:** Python 3.12, Pydantic v2, NumPy (already transitively available via fastembed at ~2.4). Tests use pytest-asyncio (`asyncio_mode = "auto"` already set).

**Spec reference:** `plans/2026-05-20-eval-harness-design.md` §4 (Scoring) + §5 (Regression gate). This plan covers PR 2 only; PRs 1, 3, 4 each have their own plan cycle.

---

## Out of scope (deferred to later PRs)

- `CorpusCase`, `RunMetadata`, `ShotResult`, `CaseResult`, `RunResult` Pydantic models — PR 3 (runner wires them in)
- `corpus_loader.py`, `fetcher_override.py`, `runner.py`, `cassette.py`, `report.py`, `cli.py` — PR 3
- Wiring scoring into the runner — PR 3
- CI changes (`make evals`, `make evals-smoke`, cassette CI flow) — PR 4
- Baseline JSON file — PR 4 (the regression gate function is here; the calling code that reads the file is in the runner)

---

## File Structure

**New module skeleton:**
- Modify: `sentinel/evals/__init__.py` (currently empty — add exports)
- Create: `sentinel/evals/schema.py`
- Create: `sentinel/evals/scoring.py`
- Create: `sentinel/evals/stats.py`

**Tests:**
- Create: `tests/unit/evals/__init__.py` (empty package marker)
- Create: `tests/unit/evals/test_schema.py`
- Create: `tests/unit/evals/test_scoring.py`
- Create: `tests/unit/evals/test_stats.py`

---

## Task 0: Create feature branch

**Files:** none

- [ ] **Step 1: Confirm branch state**

Run: `git status && git branch --show-current`
Expected: clean tree; on branch `feat/eval-harness-pr2-scoring` (already created — verify).

If not on the branch:
```bash
git checkout main && git pull origin main && git checkout -b feat/eval-harness-pr2-scoring
```

---

## Task 1: Pydantic schemas for ground truth + score outputs + regression verdict

**Files:**
- Create: `sentinel/evals/schema.py`
- Create: `tests/unit/evals/__init__.py` (empty file with one line: `"""Eval-harness unit tests."""`)
- Create: `tests/unit/evals/test_schema.py`

### Step 1: Write failing schema tests

```python
# tests/unit/evals/test_schema.py
"""Unit tests for sentinel.evals.schema (GroundTruth, MetricSet, RegressionResult)."""

from __future__ import annotations

import pytest


def test_ground_truth_requires_category_in_acceptable() -> None:
    """The primary `category` must appear in `acceptable_categories`."""
    from sentinel.evals.schema import GroundTruth

    # Happy path
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config", "deploy"],
        root_cause="BGP misconfiguration",
        correct_actions=["Revert the change"],
    )
    assert gt.category == "config"

    # Primary category missing from acceptable set should raise
    with pytest.raises(ValueError, match="acceptable_categories"):
        GroundTruth(
            category="config",
            acceptable_categories=["deploy"],
            root_cause="x",
            correct_actions=["y"],
        )


def test_ground_truth_is_frozen() -> None:
    from sentinel.evals.schema import GroundTruth

    gt = GroundTruth(
        category="deploy",
        acceptable_categories=["deploy"],
        root_cause="x",
        correct_actions=["y"],
    )
    with pytest.raises(ValueError):  # pydantic frozen → ValidationError (subclass of ValueError)
        gt.category = "config"  # type: ignore[misc]


def test_ground_truth_rejects_empty_root_cause_or_actions() -> None:
    from sentinel.evals.schema import GroundTruth

    with pytest.raises(ValueError):
        GroundTruth(
            category="deploy",
            acceptable_categories=["deploy"],
            root_cause="",  # empty
            correct_actions=["y"],
        )
    # Empty correct_actions IS allowed (some incidents have no obvious remediation);
    # scoring handles it (returns 1.0 by spec).
    gt = GroundTruth(
        category="deploy",
        acceptable_categories=["deploy"],
        root_cause="x",
        correct_actions=[],
    )
    assert gt.correct_actions == []


def test_ground_truth_rejects_unknown_category() -> None:
    from sentinel.evals.schema import GroundTruth

    with pytest.raises(ValueError):
        GroundTruth(
            category="nonexistent",  # type: ignore[arg-type]
            acceptable_categories=["nonexistent"],  # type: ignore[list-item]
            root_cause="x",
            correct_actions=["y"],
        )


def test_metric_set_clamps_to_unit_interval() -> None:
    """All metrics in [0, 1]."""
    from sentinel.evals.schema import MetricSet

    ms = MetricSet(
        category_match=1.0,
        hypothesis_cosine=0.78,
        action_coverage=0.71,
        evidence_quality=0.94,
    )
    assert ms.category_match == 1.0

    with pytest.raises(ValueError):
        MetricSet(
            category_match=1.5,  # out of range
            hypothesis_cosine=0.5,
            action_coverage=0.5,
            evidence_quality=0.5,
        )


def test_metric_set_evidence_quality_can_be_none() -> None:
    """evidence_quality=None when no diagnosis was produced (schema_failed case).
    The other three metrics are 0 in that case; evidence_quality is N/A.
    """
    from sentinel.evals.schema import MetricSet

    ms = MetricSet(
        category_match=0.0,
        hypothesis_cosine=0.0,
        action_coverage=0.0,
        evidence_quality=None,
    )
    assert ms.evidence_quality is None


def test_regression_result_carries_ci_and_verdict() -> None:
    from sentinel.evals.schema import RegressionResult

    r = RegressionResult(
        metric_name="hypothesis_cosine",
        mean_diff=-0.08,
        ci_low=-0.12,
        ci_high=-0.02,
        is_regression=True,
        reason="CI excludes zero and mean drop exceeds 5% floor",
    )
    assert r.is_regression is True
    assert r.metric_name == "hypothesis_cosine"


def test_regression_verdict_aggregates_per_metric_results() -> None:
    from sentinel.evals.schema import RegressionResult, RegressionVerdict

    r1 = RegressionResult(
        metric_name="category_match",
        mean_diff=0.0,
        ci_low=-0.02,
        ci_high=0.02,
        is_regression=False,
        reason="within noise",
    )
    r2 = RegressionResult(
        metric_name="hypothesis_cosine",
        mean_diff=-0.08,
        ci_low=-0.12,
        ci_high=-0.02,
        is_regression=True,
        reason="CI excludes zero and mean drop exceeds 5% floor",
    )
    verdict = RegressionVerdict(per_metric=[r1, r2])
    assert verdict.has_regression is True
    assert verdict.regressed_metrics == ["hypothesis_cosine"]
    # All-clear verdict
    verdict_clean = RegressionVerdict(per_metric=[r1])
    assert verdict_clean.has_regression is False
    assert verdict_clean.regressed_metrics == []
```

### Step 2: Run — confirm it fails

Run: `.venv/bin/pytest tests/unit/evals/test_schema.py -v --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'sentinel.evals.schema'`.

### Step 3: Create the eval-tests package marker

Write `tests/unit/evals/__init__.py` with:

```python
"""Eval-harness unit tests."""
```

### Step 4: Create `sentinel/evals/schema.py`

```python
# sentinel/evals/schema.py
"""Pydantic schemas for the eval harness scoring path.

This module contains only the types the pure-function scoring + stats layer
needs. CorpusCase / RunMetadata / per-shot result types live in a separate
schema module landing with PR 3 (the runner).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from sentinel.schemas.enums import CategoryType


class GroundTruth(BaseModel):
    """Curator-supplied truth for one postmortem case.

    `category` is the single primary label; `acceptable_categories` is the
    defensible set (so e.g. a config bug shipped via deploy can score either
    'config' or 'deploy' without a penalty).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    category: CategoryType
    acceptable_categories: list[CategoryType] = Field(min_length=1)
    root_cause: str = Field(min_length=1)
    # Empty correct_actions is allowed — scoring returns 1.0 for action_coverage
    # in that case (no actions to be covered = vacuously perfect).
    correct_actions: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _category_in_acceptable(self) -> GroundTruth:
        if self.category not in self.acceptable_categories:
            raise ValueError(
                f"category={self.category!r} must appear in acceptable_categories="
                f"{self.acceptable_categories!r}"
            )
        return self


class MetricSet(BaseModel):
    """Four eval metrics, all in [0, 1]. evidence_quality is None when the
    diagnosis could not be produced or parsed (case_status=schema_failed); the
    other three are 0 in that case."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    category_match: float = Field(ge=0.0, le=1.0)
    hypothesis_cosine: float = Field(ge=0.0, le=1.0)
    action_coverage: float = Field(ge=0.0, le=1.0)
    evidence_quality: float | None = Field(default=None, ge=0.0, le=1.0)


class RegressionResult(BaseModel):
    """One metric's regression verdict from the paired-bootstrap gate.

    is_regression = True iff:
      - CI excludes zero (statistical signal that the new run is worse), AND
      - mean(per-case diff) is worse than the practical floor (default 5%)

    The "worse direction" is metric-dependent: most metrics are better-when-
    higher, so worse = mean_diff < -practical_floor. The gate caller flips the
    sign for hallucinated_evidence_rate (lower is better).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    metric_name: str = Field(min_length=1)
    mean_diff: float
    ci_low: float
    ci_high: float
    is_regression: bool
    reason: str = Field(min_length=1)


class RegressionVerdict(BaseModel):
    """Aggregate of per-metric regression results — what the gate returns."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    per_metric: list[RegressionResult]

    @property
    def has_regression(self) -> bool:
        return any(r.is_regression for r in self.per_metric)

    @property
    def regressed_metrics(self) -> list[str]:
        return [r.metric_name for r in self.per_metric if r.is_regression]
```

### Step 5: Wire the module export

Replace the contents of `sentinel/evals/__init__.py` with:

```python
# sentinel/evals/__init__.py
"""Eval harness — corpus-driven scoring of diagnosis quality."""

from sentinel.evals.schema import (
    GroundTruth,
    MetricSet,
    RegressionResult,
    RegressionVerdict,
)

__all__ = [
    "GroundTruth",
    "MetricSet",
    "RegressionResult",
    "RegressionVerdict",
]
```

### Step 6: Re-run

Run: `.venv/bin/pytest tests/unit/evals/test_schema.py -v --no-cov`
Expected: all 8 tests PASS.

### Step 7: mypy strict

Run: `mypy --strict sentinel/evals/ tests/unit/evals/`
Expected: no errors.

### Step 8: Commit

```bash
git add sentinel/evals/__init__.py sentinel/evals/schema.py tests/unit/evals/__init__.py tests/unit/evals/test_schema.py
git commit -m "$(cat <<'EOF'
feat(evals): GroundTruth + MetricSet + RegressionResult Pydantic schemas

First slice of Work Area K PR 2. Pure schemas — no runner, no scoring logic
yet. GroundTruth validates primary category against acceptable_categories;
MetricSet allows None evidence_quality for the schema-failed case;
RegressionVerdict aggregates per-metric RegressionResult into a single
has_regression property.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: scoring.py — four pure async functions

**Files:**
- Create: `sentinel/evals/scoring.py`
- Create: `tests/unit/evals/test_scoring.py`

`scoring.py` exports four functions matching the design spec §4. All are async because they use the `EmbeddingProvider` Protocol from `sentinel.enrichment.protocols`, which is async (the production fastembed implementation is async).

### Step 1: Write failing scoring tests

```python
# tests/unit/evals/test_scoring.py
"""Unit tests for sentinel.evals.scoring (4 pure-function scorers).

Uses a deterministic FakeEmbedder that maps known strings to fixed vectors,
so the scoring math is testable without spinning fastembed.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime

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
_BASIS: dict[str, list[float]] = {
    "rollback the deploy": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "BGP misconfiguration caused route propagation failures": [
        0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
    ],
    "Revert the BGP configuration change": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "Roll back the deploy": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "BGP misconfig in route propagation": [
        0.0, 0.99, 0.141, 0.0, 0.0, 0.0, 0.0, 0.0,  # ~0.99 cosine vs root_cause
    ],
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


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
        evidence=evidence or [
            EvidenceRef(kind="deploy", id="deploy:abc123", note="recent deploy")
        ],
        suggested_actions=suggested_actions or [
            SuggestedAction(
                description="rollback the deploy",
                command=None,
                risk="medium",
                rationale="revert",
                requires_human_approval=True,
            )
        ],
        likely_category=likely_category,  # type: ignore[arg-type]  # CategoryType is a Literal
    )


def _empty_context() -> IncidentContext:
    """Helper — context with one deploy and one log (typical eval seed shape)."""
    now = _utc_now()
    return IncidentContext(
        incident_id=None,  # type: ignore[arg-type]  # not validated here; some test calls override
        recent_deploys=FetcherResult(
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
        related_alerts=FetcherResult(status="ok", data=[], fetched_at=now),
        similar_incidents=FetcherResult(status="ok", data=[], fetched_at=now),
        runbooks=FetcherResult(status="ok", data=[], fetched_at=now),
        recent_logs=FetcherResult(
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

    embedder = FakeEmbedder()  # unknown strings → orthogonal-ish unit vectors
    gt = GroundTruth(
        category="config",
        acceptable_categories=["config"],
        root_cause="completely unrelated topic A",
        correct_actions=[],
    )
    d = _build_diagnosis(hypothesis="totally different topic B")
    score = await score_hypothesis(d, gt, embedder)
    assert 0.0 <= score <= 0.5


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
    d = _build_diagnosis(
        evidence=[EvidenceRef(kind="deploy", id="deploy:notreal", note="x")]
    )
    score = score_evidence_quality(d, ctx)
    assert score == 0.0
```

### Step 2: Run — confirm it fails

Run: `.venv/bin/pytest tests/unit/evals/test_scoring.py -v --no-cov`
Expected: FAIL — ImportError on `sentinel.evals.scoring`.

### Step 3: Create `sentinel/evals/scoring.py`

```python
# sentinel/evals/scoring.py
"""Pure scoring functions for the eval harness — 4 metrics per case.

All metrics return floats in [0, 1]. Functions that use embeddings are async
because EmbeddingProvider (from sentinel.enrichment.protocols) is async to
match the production fastembed implementation.

Aggregation across cases (mean + bootstrap CI) lives in stats.py — these
functions operate on a single (Diagnosis, GroundTruth, IncidentContext)
triple.
"""

from __future__ import annotations

from statistics import fmean

from sentinel.enrichment.protocols import EmbeddingProvider
from sentinel.evals.schema import GroundTruth
from sentinel.schemas.context import IncidentContext
from sentinel.schemas.diagnosis import Diagnosis


def score_category(d: Diagnosis, gt: GroundTruth) -> float:
    """1.0 if predicted likely_category is in the acceptable set, else 0.0."""
    return 1.0 if d.likely_category in gt.acceptable_categories else 0.0


async def score_hypothesis(
    d: Diagnosis, gt: GroundTruth, embed: EmbeddingProvider
) -> float:
    """Cosine similarity of hypothesis vs root_cause embeddings, clamped to [0, 1].

    Reported as the raw continuous cosine rather than a thresholded pass/fail
    — preserves signal for the regression gate (a drift from 0.82 → 0.71 is
    meaningful and would be hidden by binary scoring).
    """
    hyp_vec = await embed.embed(d.hypothesis)
    rc_vec = await embed.embed(gt.root_cause)
    return max(0.0, _cosine(hyp_vec, rc_vec))


async def score_action_coverage(
    d: Diagnosis, gt: GroundTruth, embed: EmbeddingProvider
) -> float:
    """For each correct_action: max cosine vs any suggested_action.description.
    Final score is mean across correct_actions.

    Edge cases per spec:
      - No correct_actions (some incidents have no obvious remediation) → 1.0
      - No suggested_actions but correct_actions exist → 0.0
    """
    if not gt.correct_actions:
        return 1.0
    if not d.suggested_actions:
        return 0.0

    action_vecs = [await embed.embed(a.description) for a in d.suggested_actions]
    per_correct: list[float] = []
    for ca in gt.correct_actions:
        ca_vec = await embed.embed(ca)
        per_correct.append(max(_cosine(ca_vec, av) for av in action_vecs))
    return max(0.0, min(1.0, fmean(per_correct)))


def score_evidence_quality(d: Diagnosis, ctx: IncidentContext) -> float:
    """Fraction of EvidenceRef.id values that resolve to a context item.

    This is the strict ID-resolution gate — the production hallucination
    metric (in diagnosis/validation.py) handles the "cited but unsupported"
    case via confidence capping. PR 2 only scores the deterministic ID match.

    Returns 0.0 if the diagnosis has no evidence at all (the schema enforces
    at least one ref on a parsed Diagnosis, so this branch is defensive).
    """
    if not d.evidence:
        return 0.0
    valid_ids = _collect_context_ids(ctx)
    resolved = sum(1 for e in d.evidence if e.id in valid_ids)
    return resolved / len(d.evidence)


# --- helpers ---


def _cosine(a: list[float], b: list[float]) -> float:
    """Plain cosine similarity without numpy — these are small vectors (~1024 dim
    in production, 8 dim in tests) and avoiding a numpy import in scoring.py
    keeps the module's dependency surface lean (stats.py uses numpy for the
    bootstrap, but scoring is leaner)."""
    if len(a) != len(b):
        raise ValueError(f"vector length mismatch: {len(a)} vs {len(b)}")
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (na * nb)


def _collect_context_ids(ctx: IncidentContext) -> set[str]:
    """Walk the full IncidentContext and gather every item.id — the contract
    the diagnosis prompt + validation gate also rely on."""
    ids: set[str] = set()
    for item in ctx.recent_deploys.data:
        ids.add(item.id)
    for item in ctx.related_alerts.data:
        ids.add(item.id)
    for item in ctx.similar_incidents.data:
        ids.add(item.id)
    for item in ctx.runbooks.data:
        ids.add(item.id)
    for item in ctx.recent_logs.data:
        ids.add(item.id)
    return ids
```

### Step 4: Re-run

Run: `.venv/bin/pytest tests/unit/evals/test_scoring.py -v --no-cov`
Expected: all 9 tests PASS.

If `IncidentContext.incident_id` is required and the `_empty_context()` helper passes `None`, the test will fail. In that case, check `sentinel/schemas/context.py:86+` for the actual `IncidentContext` field shape and adjust the helper — pass a `uuid4()` if required, drop the field if not present.

### Step 5: mypy strict

Run: `mypy --strict sentinel/evals/ tests/unit/evals/test_scoring.py`
Expected: no errors. If mypy complains about `Diagnosis(likely_category=string)` requiring a Literal, the `# type: ignore[arg-type]` in `_build_diagnosis` should suffice.

### Step 6: Wire `scoring` exports into `sentinel/evals/__init__.py`

Append to the imports + `__all__` (alphabetical):

```python
from sentinel.evals.scoring import (
    score_action_coverage,
    score_category,
    score_evidence_quality,
    score_hypothesis,
)
```

And in `__all__`:

```python
"score_action_coverage",
"score_category",
"score_evidence_quality",
"score_hypothesis",
```

### Step 7: Commit

```bash
git add sentinel/evals/scoring.py sentinel/evals/__init__.py tests/unit/evals/test_scoring.py
git commit -m "$(cat <<'EOF'
feat(evals): scoring.py — 4 pure-function scorers (async on embed)

score_category (binary), score_hypothesis (continuous cosine, not thresholded),
score_action_coverage (mean of max-per-correct), score_evidence_quality
(fraction of resolved IDs). FakeEmbedder helper in tests makes the math
deterministic without spinning fastembed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: stats.py — bootstrap CI + paired-difference regression gate

**Files:**
- Create: `sentinel/evals/stats.py`
- Create: `tests/unit/evals/test_stats.py`

### Step 1: Write failing stats tests

```python
# tests/unit/evals/test_stats.py
"""Unit tests for sentinel.evals.stats (paired-bootstrap regression gate)."""

from __future__ import annotations

import pytest


def test_bootstrap_ci_returns_tuple_in_value_order() -> None:
    """ci_low <= mean <= ci_high; deterministic when seeded."""
    from sentinel.evals.stats import bootstrap_ci

    # 20 values clustered around 0.8 — CI should be tight around the mean
    values = [0.78, 0.80, 0.82, 0.79, 0.81, 0.80, 0.79, 0.80, 0.81, 0.79,
              0.80, 0.81, 0.82, 0.78, 0.80, 0.79, 0.81, 0.80, 0.82, 0.79]
    lo, hi = bootstrap_ci(values, n_resamples=2000, seed=42)
    assert lo < 0.80 < hi
    assert hi - lo < 0.05  # tight cluster → tight CI


def test_bootstrap_ci_is_seeded_deterministic() -> None:
    from sentinel.evals.stats import bootstrap_ci

    values = [0.5, 0.6, 0.7, 0.8, 0.9, 0.4, 0.5, 0.6]
    lo1, hi1 = bootstrap_ci(values, n_resamples=1000, seed=7)
    lo2, hi2 = bootstrap_ci(values, n_resamples=1000, seed=7)
    assert lo1 == lo2 and hi1 == hi2


def test_bootstrap_ci_empty_raises() -> None:
    from sentinel.evals.stats import bootstrap_ci

    with pytest.raises(ValueError, match="empty"):
        bootstrap_ci([], n_resamples=1000, seed=1)


def test_regression_for_metric_clean_when_runs_match() -> None:
    """No drift → no regression, regardless of variance."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.8, 0.85, 0.78, 0.82, 0.79, 0.81, 0.80, 0.83, 0.77, 0.80]
    current = [0.8, 0.84, 0.78, 0.82, 0.79, 0.81, 0.80, 0.83, 0.77, 0.81]
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
    )
    assert r.is_regression is False
    assert r.metric_name == "hypothesis_cosine"


def test_regression_for_metric_detects_clear_drop() -> None:
    """A 15% drop across all cases triggers the gate."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.85] * 10
    current = [0.70] * 10  # -0.15 per case
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
    )
    assert r.is_regression is True
    assert r.mean_diff < -0.05


def test_regression_for_metric_ignores_tiny_drop_below_practical_floor() -> None:
    """A 2% drop is below the practical floor — not a regression even if CI
    excludes zero. This avoids 'statistically significant but practically
    meaningless' false alarms."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.85] * 30
    current = [0.83] * 30  # -0.02 per case, very tight CI excludes zero
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
        practical_floor=0.05,
    )
    assert r.is_regression is False


def test_regression_for_metric_ignores_noise_even_when_mean_drops_a_lot() -> None:
    """A 10% drop in mean but huge variance — CI includes zero → no regression
    (paired-bootstrap correctly attributes the drop to noise)."""
    from sentinel.evals.stats import regression_for_metric

    # baseline and current paired so per-case diffs are noisy around 0
    baseline = [0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1]
    current = [0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9, 0.1, 0.9]
    r = regression_for_metric(
        metric_name="hypothesis_cosine",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
    )
    # Mean diff is 0; CI brackets zero → not a regression
    assert r.is_regression is False


def test_regression_for_metric_inverted_metric_fires_on_increase() -> None:
    """For inverted metrics (lower-is-better like hallucinated_evidence_rate),
    a 15% INCREASE is a regression."""
    from sentinel.evals.stats import regression_for_metric

    baseline = [0.05] * 10
    current = [0.20] * 10  # +0.15 per case
    r = regression_for_metric(
        metric_name="hallucinated_evidence_rate",
        current_per_case=current,
        baseline_per_case=baseline,
        n_resamples=2000,
        seed=42,
        higher_is_better=False,
    )
    assert r.is_regression is True
    assert r.mean_diff > 0.05


def test_regression_for_metric_length_mismatch_raises() -> None:
    from sentinel.evals.stats import regression_for_metric

    with pytest.raises(ValueError, match="length"):
        regression_for_metric(
            metric_name="x",
            current_per_case=[0.1, 0.2, 0.3],
            baseline_per_case=[0.1, 0.2],
            n_resamples=100,
            seed=1,
        )
```

### Step 2: Run — confirm it fails

Run: `.venv/bin/pytest tests/unit/evals/test_stats.py -v --no-cov`
Expected: FAIL — ImportError on `sentinel.evals.stats`.

### Step 3: Create `sentinel/evals/stats.py`

```python
# sentinel/evals/stats.py
"""Statistical helpers for the eval regression gate.

Paired-difference bootstrap rather than naive flat-threshold: with n=10 cases
× 4 metrics, a flat ">5% drop" gate produces constant false positives from
LLM noise alone. See spec §5 + Cameron Wolfe's stats-for-LLM-evals analysis
referenced in plans/2026-05-20-eval-harness-design.md.

Pure NumPy, sync. No DB, no LLM, no async — just math.
"""

from __future__ import annotations

import numpy as np

from sentinel.evals.schema import RegressionResult


def bootstrap_ci(
    values: list[float],
    *,
    n_resamples: int,
    seed: int,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Percentile bootstrap CI for the mean of `values`.

    Args:
        values: per-case sample (e.g. per-case difference scores)
        n_resamples: number of bootstrap resamples (industry default: 10k+)
        seed: deterministic RNG seed (eval gate must be reproducible)
        alpha: significance level (0.05 → 95% CI)

    Returns:
        (ci_low, ci_high) such that the true mean is in the interval with
        100*(1-alpha)% confidence under the bootstrap assumptions.
    """
    if not values:
        raise ValueError("bootstrap_ci called with empty values")

    rng = np.random.default_rng(seed)
    arr = np.asarray(values, dtype=np.float64)
    # Resample with replacement n_resamples times; each resample has the same
    # length as the original sample. Take the mean of each resample.
    resample_means = rng.choice(arr, size=(n_resamples, arr.size), replace=True).mean(
        axis=1
    )
    lo = float(np.quantile(resample_means, alpha / 2))
    hi = float(np.quantile(resample_means, 1 - alpha / 2))
    return lo, hi


def regression_for_metric(
    *,
    metric_name: str,
    current_per_case: list[float],
    baseline_per_case: list[float],
    n_resamples: int = 10_000,
    seed: int,
    practical_floor: float = 0.05,
    higher_is_better: bool = True,
) -> RegressionResult:
    """Paired-bootstrap regression gate for one metric.

    Computes per-case difference d_i = current[i] - baseline[i], bootstraps a
    95% CI on the mean difference, and returns is_regression=True iff BOTH:
      - CI excludes zero in the "worse" direction (statistical signal), AND
      - mean(d_i) is worse than the practical_floor (default 5%)

    For higher-is-better metrics: "worse" = mean_d < -practical_floor AND ci_high < 0.
    For lower-is-better (e.g. hallucinated_evidence_rate): the sign flips.

    Both conditions are required so that:
      - random noise with no real drift doesn't trip the gate (CI requirement)
      - statistically significant but practically tiny drops don't either
        (floor requirement)
    """
    if len(current_per_case) != len(baseline_per_case):
        raise ValueError(
            f"length mismatch: current={len(current_per_case)} vs "
            f"baseline={len(baseline_per_case)}"
        )

    diffs = [c - b for c, b in zip(current_per_case, baseline_per_case, strict=True)]
    mean_d = float(sum(diffs) / len(diffs))
    ci_low, ci_high = bootstrap_ci(diffs, n_resamples=n_resamples, seed=seed)

    if higher_is_better:
        ci_excludes_zero_worse = ci_high < 0  # entirely below zero = drop
        beyond_floor = mean_d < -practical_floor
    else:
        ci_excludes_zero_worse = ci_low > 0  # entirely above zero = rise
        beyond_floor = mean_d > practical_floor

    is_regression = ci_excludes_zero_worse and beyond_floor

    if is_regression:
        direction = "drop" if higher_is_better else "rise"
        reason = (
            f"95% CI excludes zero in the {direction} direction "
            f"(CI=[{ci_low:.4f}, {ci_high:.4f}]) and mean_diff={mean_d:.4f} "
            f"exceeds practical_floor={practical_floor}"
        )
    elif not ci_excludes_zero_worse and not beyond_floor:
        reason = (
            f"within noise: CI=[{ci_low:.4f}, {ci_high:.4f}] brackets zero "
            f"and mean_diff={mean_d:.4f} is within practical_floor={practical_floor}"
        )
    elif not ci_excludes_zero_worse:
        reason = (
            f"CI=[{ci_low:.4f}, {ci_high:.4f}] does not exclude zero — drift "
            f"indistinguishable from noise"
        )
    else:
        reason = (
            f"mean_diff={mean_d:.4f} is within practical_floor={practical_floor} — "
            f"statistically significant but practically immaterial"
        )

    return RegressionResult(
        metric_name=metric_name,
        mean_diff=mean_d,
        ci_low=ci_low,
        ci_high=ci_high,
        is_regression=is_regression,
        reason=reason,
    )
```

### Step 4: Re-run

Run: `.venv/bin/pytest tests/unit/evals/test_stats.py -v --no-cov`
Expected: all 9 tests PASS.

### Step 5: mypy strict

Run: `mypy --strict sentinel/evals/ tests/unit/evals/`
Expected: no errors. If mypy complains about `numpy` (it's not in the mypy `module = [...]` ignore list), add `"numpy.*"` to the existing `[[tool.mypy.overrides]]` ignore-missing-imports block in `pyproject.toml` (look for the line that includes `aiokafka.*`, `asyncpg.*` etc.) — match the existing pattern.

### Step 6: Wire `stats` exports into `sentinel/evals/__init__.py`

Add to imports + `__all__` (alphabetical):

```python
from sentinel.evals.stats import bootstrap_ci, regression_for_metric
```

And in `__all__`:

```python
"bootstrap_ci",
"regression_for_metric",
```

### Step 7: Commit

```bash
git add sentinel/evals/stats.py sentinel/evals/__init__.py tests/unit/evals/test_stats.py pyproject.toml
git commit -m "$(cat <<'EOF'
feat(evals): stats.py — paired-bootstrap regression gate

bootstrap_ci computes a percentile CI on the mean of a sample (seeded for
reproducibility). regression_for_metric does paired-difference bootstrap +
practical-floor check, supporting both higher-is-better and lower-is-better
metrics (the latter for hallucinated_evidence_rate). Reason field captures
the gate verdict in operator-readable form for the eventual report.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

(Only include `pyproject.toml` in the commit if you had to add `numpy.*` to the mypy override block.)

---

## Task 4: Full lint + typecheck + test sweep

- [ ] **Step 1: lint** — `make lint` — expected clean
- [ ] **Step 2: typecheck** — `make typecheck` — expected clean
- [ ] **Step 3: unit suite** — `make test` — expected all green, coverage ≥ 80%
- [ ] **Step 4: integration suite** — `make test-integration` — PR 2 adds no integration tests, but confirm we didn't regress anything

If a pre-existing test breaks due to a side effect, investigate before patching.

---

## Task 5: Mandatory code review

Per `MEMORY.md` → `sentinel-review-before-commit.md`.

- [ ] **Step 1: Invoke `superpowers:requesting-code-review`** with:
  - Scope: PR 2 of Work Area K (scoring + stats + schemas)
  - Spec: `plans/2026-05-20-eval-harness-design.md` §4, §5
  - Plan: `plans/2026-05-20-eval-harness-pr2-scoring-plan.md`
  - Specific concerns to validate:
    - Scoring functions are pure (no I/O beyond the injected EmbeddingProvider)
    - `score_action_coverage` handles both edge cases (empty correct_actions → 1.0; empty suggested → 0.0)
    - Bootstrap CI is seeded → deterministic; no nondet leaks into the regression gate
    - Both gate conditions required (CI + floor) — neither alone trips a regression
    - inverted-metric branch correctly handles hallucinated_evidence_rate semantics
    - FakeEmbedder test fixture covers the realistic shape (orthogonal vectors for unknown strings)

- [ ] **Step 2: Address feedback**

- [ ] **Step 3: Re-run sweep after changes**

---

## Task 6: Push + open PR

- [ ] **Step 1: Verify clean tree** — `git status`
- [ ] **Step 2: Verify commit log** — `git log --oneline main..HEAD` — expect ~4 commits (schema, scoring, stats, plus any review-fix commits)
- [ ] **Step 3: Push** — `git push -u origin feat/eval-harness-pr2-scoring`
- [ ] **Step 4: Open PR** — `gh pr create` with title `feat(evals): scoring + stats (PR 2/4 of Work Area K)`

PR description should:
- Link the design doc and this plan
- Note PR 2 is independent of PR 1 — pure functions, no DB
- Highlight the two regression-gate conditions (CI + floor) and the paired-bootstrap rationale
- Confirm: `make lint typecheck test` green; `make test-integration` no regressions
