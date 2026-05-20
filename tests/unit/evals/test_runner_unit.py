"""Unit tests for sentinel.evals.runner — orchestration helpers.

These tests cover the helper functions and aggregation math in isolation.
End-to-end coverage (real FastAPI app, real Postgres) lands with the
integration test in Task 7.

Four tests, per the PR 3b plan:

1. ``test_failure_mode_ingest_failed`` — webhook returns 500 → case_status
   "ingest_failed", metrics zeroed, evidence_quality = None.
2. ``test_failure_mode_timeout`` — diagnosis poll always returns None → after
   the configured poll_timeout_s, case_status "timeout".
3. ``test_per_case_stability_computation`` — pure-function check of the
   per-case mean + stddev helper.
4. ``test_run_result_aggregation`` — pure-function check of the run-level
   aggregator (cases with status != "ok" drag the mean down).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.evals.runner import (
    EvalCaseResultRecord,
    RunnerDeps,
    _aggregate_run,
    _per_case_stats,
    run_corpus,
)
from sentinel.evals.schema import (
    AlertSeed,
    ContextSeed,
    CorpusCase,
    DeploySeed,
    GroundTruth,
    LogSeed,
    MetricSet,
)
from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction

# --- Fakes ----------------------------------------------------------------- #


class FakeEmbedder:
    """Same-shape stand-in for production EmbeddingProvider — every text maps
    to a single fixed unit vector. Tests using this embedder don't exercise
    score_hypothesis / score_action_coverage at non-trivial values.
    """

    async def embed(self, text: str) -> list[float]:
        return [1.0, 0.0, 0.0, 0.0]


@dataclass
class FakeClientResponse:
    status_code: int
    body: dict[str, Any]

    def json(self) -> dict[str, Any]:
        return self.body


class FakeClient:
    """Stand-in for httpx.AsyncClient. Returns canned (status, json) tuples
    keyed by URL path, falling back to a single default response."""

    def __init__(self, *, status_code: int = 202, body: dict[str, Any] | None = None) -> None:
        self._status_code = status_code
        self._body = body or {}
        self.requests: list[tuple[str, bytes, dict[str, str]]] = []

    async def post(
        self,
        url: str,
        *,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        self.requests.append((url, content or b"", headers or {}))
        return httpx.Response(
            status_code=self._status_code,
            json=self._body,
            request=httpx.Request("POST", f"http://test{url}"),
        )


class FakeDiagnosisRepo:
    """Stand-in for the DiagnosisRepository.get_by_incident_id call.

    If ``diagnosis`` is None, every poll returns None — exercises the timeout
    path. Otherwise, returns it on the first poll.
    """

    def __init__(self, *, diagnosis: PersistedDiagnosis | None = None) -> None:
        self._diagnosis = diagnosis
        self.calls: int = 0

    async def get_by_incident_id(self, incident_id: UUID) -> PersistedDiagnosis | None:
        self.calls += 1
        return self._diagnosis


class FakeEvalRunRepo:
    """Records persist_shot calls in order so tests can assert on them."""

    def __init__(self) -> None:
        self.shots: list[EvalCaseResultRecord] = []

    async def persist_shot(self, shot: EvalCaseResultRecord) -> None:
        self.shots.append(shot)


# --- Fixtures -------------------------------------------------------------- #


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _build_case(case_id: str = "test-case") -> CorpusCase:
    now = _utc_now()
    return CorpusCase(
        id=case_id,
        corpus_version=1,
        source_url="https://example.com/postmortem",
        sources_consulted=["https://example.com/postmortem"],
        notes="",
        alert=AlertSeed(
            source="generic",
            service="edge",
            severity="SEV1",
            title="BGP route propagation failure",
            timestamp=now,
            raw_payload={
                "id": "ext-1",
                "service": "edge",
                "severity": "SEV1",
                "title": "BGP route propagation failure",
            },
        ),
        context_seed=ContextSeed(
            deploys=[
                DeploySeed(
                    id="deploy:abc123",
                    service="edge",
                    sha="abc123",
                    deployed_at=now,
                )
            ],
            recent_logs=[
                LogSeed(
                    id="log:1",
                    timestamp=now,
                    level="error",
                    service="edge",
                    message="bgp reset",
                ),
            ],
        ),
        ground_truth=GroundTruth(
            category="config",
            acceptable_categories=["config"],
            root_cause="BGP misconfiguration",
            correct_actions=[],
        ),
    )


def _build_persisted_diagnosis() -> PersistedDiagnosis:
    return PersistedDiagnosis(
        hypothesis="BGP misconfig",
        confidence=Decimal("0.8"),
        reasoning="r",
        evidence=[EvidenceRef(kind="deploy", id="deploy:abc123", note="recent deploy")],
        suggested_actions=[
            SuggestedAction(
                description="rollback the deploy",
                command=None,
                risk="medium",
                rationale="revert",
                requires_human_approval=True,
            )
        ],
        likely_category="config",
        hallucinated_evidence=False,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        latency_ms=1234,
        token_usage={"input": 100, "output": 50},
    )


@dataclass
class _DepsBundle:
    """Bundle around a RunnerDeps + the fakes it wraps, so tests can assert on
    the fakes after calling run_corpus."""

    deps: RunnerDeps
    client: FakeClient
    diagnosis_repo: FakeDiagnosisRepo
    eval_run_repo: FakeEvalRunRepo
    truncate_calls: list[None] = field(default_factory=list)


def _build_deps(
    *,
    client_status: int = 202,
    client_body: dict[str, Any] | None = None,
    diagnosis: PersistedDiagnosis | None = None,
    poll_timeout_s: float = 0.2,
    poll_interval_s: float = 0.02,
) -> _DepsBundle:
    client = FakeClient(
        status_code=client_status,
        body=client_body if client_body is not None else {"incident_id": str(uuid4())},
    )
    diag_repo = FakeDiagnosisRepo(diagnosis=diagnosis)
    eval_repo = FakeEvalRunRepo()
    bundle_truncate_calls: list[None] = []

    async def truncate() -> None:
        bundle_truncate_calls.append(None)

    deps = RunnerDeps(
        client=client,
        diagnosis_repo=diag_repo,
        eval_run_repo=eval_repo,
        embed=FakeEmbedder(),
        cassette_transport=None,
        run_id=uuid4(),
        prompt_version="v1",
        model_id="claude-sonnet-4-5",
        truncate_between_cases=truncate,
        webhook_secret=SecretStr("test-secret"),
        poll_timeout_s=poll_timeout_s,
        poll_interval_s=poll_interval_s,
    )
    bundle = _DepsBundle(
        deps=deps,
        client=client,
        diagnosis_repo=diag_repo,
        eval_run_repo=eval_repo,
    )
    bundle.truncate_calls = bundle_truncate_calls
    return bundle


# --- Failure-mode tests ---------------------------------------------------- #


@pytest.mark.asyncio
async def test_failure_mode_ingest_failed() -> None:
    """Webhook returns 500 → case_status "ingest_failed", metrics zeroed."""
    case = _build_case()
    bundle = _build_deps(client_status=500, client_body={"detail": "boom"})

    result = await run_corpus(cases=[case], shots_per_case=1, runner_deps=bundle.deps)

    assert len(bundle.eval_run_repo.shots) == 1
    shot = bundle.eval_run_repo.shots[0]
    assert shot.case_status == "ingest_failed"
    assert shot.incident_id is None
    assert shot.diagnosis is None
    assert shot.metrics["category_match"] == 0.0
    assert shot.metrics["hypothesis_cosine"] == 0.0
    assert shot.metrics["action_coverage"] == 0.0
    assert shot.metrics["evidence_quality"] is None
    assert shot.error_detail is not None

    # Run-level aggregation: ingest_failed contributes 0 to drag the mean down.
    agg = result.aggregate_metrics
    assert agg.category_match == 0.0
    assert agg.hypothesis_cosine == 0.0
    assert agg.action_coverage == 0.0
    # evidence_quality has no non-None contributors → stays None.
    assert agg.evidence_quality is None


@pytest.mark.asyncio
async def test_failure_mode_timeout() -> None:
    """Diagnosis poll never returns → case_status = "timeout"."""
    incident_id = uuid4()
    case = _build_case()
    bundle = _build_deps(
        client_body={"incident_id": str(incident_id)},
        diagnosis=None,  # poll always returns None → forces timeout
        poll_timeout_s=0.1,
        poll_interval_s=0.02,
    )

    result = await run_corpus(cases=[case], shots_per_case=1, runner_deps=bundle.deps)

    assert len(bundle.eval_run_repo.shots) == 1
    shot = bundle.eval_run_repo.shots[0]
    assert shot.case_status == "timeout"
    assert shot.incident_id == incident_id
    assert shot.diagnosis is None
    assert shot.metrics["category_match"] == 0.0
    assert shot.metrics["evidence_quality"] is None
    # The poll should have been called at least once before the timeout.
    assert bundle.diagnosis_repo.calls >= 1
    # Per-case stability with a single shot is 0 stddev — verify the run
    # at least returns a populated structure.
    assert case.id in result.per_case_metrics
    assert case.id in result.stability


# --- Aggregation tests ----------------------------------------------------- #


def test_per_case_stability_computation() -> None:
    """Mean and sample stddev across shots for a single case."""
    shots = [
        MetricSet(
            category_match=1.0,
            hypothesis_cosine=0.8,
            action_coverage=0.5,
            evidence_quality=1.0,
        ),
        MetricSet(
            category_match=1.0,
            hypothesis_cosine=0.6,
            action_coverage=0.7,
            evidence_quality=0.5,
        ),
        MetricSet(
            category_match=0.0,
            hypothesis_cosine=0.7,
            action_coverage=0.6,
            evidence_quality=None,  # excluded from the evidence_quality mean
        ),
    ]
    mean, stddev = _per_case_stats(shots)

    # Mean
    assert mean.category_match == pytest.approx(2 / 3)
    assert mean.hypothesis_cosine == pytest.approx(0.7)
    assert mean.action_coverage == pytest.approx(0.6)
    # evidence_quality: mean over the 2 non-None values = (1.0 + 0.5) / 2 = 0.75
    assert mean.evidence_quality == pytest.approx(0.75)

    # Sample stddev (n-1 denominator)
    # category_match values [1, 1, 0]: mean 2/3, var = ((1/3)^2 + (1/3)^2 + (2/3)^2)/2
    # = (1/9 + 1/9 + 4/9)/2 = (6/9)/2 = 1/3 → stddev = sqrt(1/3) ≈ 0.5774
    assert stddev["category_match"] == pytest.approx((1 / 3) ** 0.5)
    # hypothesis_cosine [0.8, 0.6, 0.7]: mean 0.7, var = (0.01 + 0.01 + 0)/2 = 0.01
    # → stddev = 0.1
    assert stddev["hypothesis_cosine"] == pytest.approx(0.1)
    # action_coverage [0.5, 0.7, 0.6]: mean 0.6, same shape → stddev = 0.1
    assert stddev["action_coverage"] == pytest.approx(0.1)
    # evidence_quality [1.0, 0.5]: mean 0.75, var = (0.0625 + 0.0625)/1 = 0.125
    # → stddev = sqrt(0.125) ≈ 0.3536
    assert stddev["evidence_quality"] == pytest.approx(0.125**0.5)


def test_per_case_stability_single_shot_returns_zero_stddev() -> None:
    """One shot → stddev is 0 for every metric (avoid n-1 ZeroDivisionError)."""
    shots = [
        MetricSet(
            category_match=1.0,
            hypothesis_cosine=0.8,
            action_coverage=0.5,
            evidence_quality=1.0,
        )
    ]
    mean, stddev = _per_case_stats(shots)
    assert mean.category_match == 1.0
    assert stddev["category_match"] == 0.0
    assert stddev["hypothesis_cosine"] == 0.0
    assert stddev["action_coverage"] == 0.0
    assert stddev["evidence_quality"] == 0.0


def test_run_result_aggregation() -> None:
    """Run-level aggregate is mean of per-case means; non-ok cases are 0s."""
    per_case = {
        "case-a": MetricSet(
            category_match=1.0,
            hypothesis_cosine=0.8,
            action_coverage=0.6,
            evidence_quality=1.0,
        ),
        "case-b": MetricSet(
            category_match=0.0,
            hypothesis_cosine=0.0,
            action_coverage=0.0,
            evidence_quality=None,  # non-ok case → excluded from evidence mean
        ),
        "case-c": MetricSet(
            category_match=1.0,
            hypothesis_cosine=0.6,
            action_coverage=0.4,
            evidence_quality=0.5,
        ),
    }
    agg = _aggregate_run(per_case)
    # category_match: (1 + 0 + 1) / 3 = 2/3
    assert agg.category_match == pytest.approx(2 / 3)
    # hypothesis_cosine: (0.8 + 0 + 0.6) / 3
    assert agg.hypothesis_cosine == pytest.approx((0.8 + 0.6) / 3)
    # action_coverage: (0.6 + 0 + 0.4) / 3
    assert agg.action_coverage == pytest.approx((0.6 + 0.4) / 3)
    # evidence_quality: mean over the 2 non-None values = (1.0 + 0.5) / 2 = 0.75
    assert agg.evidence_quality == pytest.approx(0.75)


def test_run_result_aggregation_empty_cases() -> None:
    """No cases → MetricSet all zeros (no division-by-zero crash)."""
    agg = _aggregate_run({})
    assert agg.category_match == 0.0
    assert agg.hypothesis_cosine == 0.0
    assert agg.action_coverage == 0.0
    assert agg.evidence_quality is None
