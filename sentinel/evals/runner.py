"""Single-case + multi-shot orchestrator for the eval harness.

The runner fires synthetic webhooks at the in-process FastAPI app, polls for
the resulting diagnosis, scores it against the corpus ground truth, and
persists one ``EvalCaseResultRecord`` per (case, shot). After all cases, it
returns a ``RunResult`` with per-case means, per-case stddevs, and a
run-level aggregate.

Design points worth flagging:

- **Cassette context per shot.** Before each shot the runner stamps the
  cassette transport with ``(prompt_version, model_id, case_id, shot_index)``
  so VCR-style replay deterministically resolves the Anthropic response. A
  ``CassetteMiss`` propagates out (the CLI in T6 catches it and marks the run
  failed) rather than being silently swallowed.

- **Case status semantics.** ``ingest_failed``, ``timeout``, and ``ok`` are
  the three terminal states for one shot. Non-ok shots persist a record with
  zeroed numeric metrics (so the aggregate is dragged down) and
  ``evidence_quality = None`` (so it is excluded from the evidence_quality
  mean — non-ok cases never produced a diagnosis whose evidence could be
  checked).

- **Score helper reconstructs IncidentContext from the corpus seed.** This
  matches what the corpus fetchers fed into enrichment in the first place,
  and avoids round-tripping through the DB just to recover what the runner
  already has in memory.

- **Per-case stddev uses sample (n-1) stddev** — the standard reporting
  convention for shot-level stability. With ``n=1`` the formula is
  ill-defined; we return 0 for every metric in that case (single-shot runs
  carry no stability signal).

Failure modes — each path produces a valid ``EvalCaseResultRecord``:
  * Webhook 5xx → ``case_status="ingest_failed"``, ``incident_id=None``,
    ``error_detail`` set, metrics zero, evidence_quality None.
  * Webhook != 202 (4xx) → same as ingest_failed (caller mis-signed or the
    app rejected the synthetic payload — both are runner-level setup bugs).
  * Diagnosis poll timeout → ``case_status="timeout"``, incident_id known,
    error_detail set, metrics zero, evidence_quality None.
  * Diagnosis present → ``case_status="ok"``, full metrics, error_detail
    None.
"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.enrichment.protocols import EmbeddingProvider
from sentinel.evals.cassette import CassetteContext, CassetteTransport
from sentinel.evals.registry import REGISTRY
from sentinel.evals.schema import CorpusCase, MetricSet
from sentinel.evals.scoring import (
    score_action_coverage,
    score_category,
    score_evidence_quality,
    score_hypothesis,
)
from sentinel.ingestion.fingerprint import fingerprint as compute_fingerprint
from sentinel.ingestion.fingerprint import normalize_title
from sentinel.integrations.base import compute_hmac_sha256
from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    IncidentContext,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)

log = logging.getLogger(__name__)

CaseStatus = Literal["ok", "ingest_failed", "timeout"]


# --- Protocols for dependency injection ----------------------------------- #


class DiagnosisLookup(Protocol):
    """Subset of the DiagnosisRepository contract the runner needs.

    Defined here (rather than reused from persistence.repositories) because
    the production ``DiagnosisRepository`` Protocol doesn't yet have a
    ``get_by_incident_id`` lookup — adding one to the production interface
    is out of scope for PR 3b. The Postgres implementation will satisfy this
    Protocol structurally once the method lands.
    """

    async def get_by_incident_id(self, incident_id: UUID) -> PersistedDiagnosis | None: ...


class EvalShotPersister(Protocol):
    """Subset of the EvalRunRepository contract the runner needs.

    Same rationale as DiagnosisLookup — ``persist_shot`` is defined here so
    the runner is decoupled from the (still-evolving) production repo.
    """

    async def persist_shot(self, shot: EvalCaseResultRecord) -> None: ...


class HttpPoster(Protocol):
    """Subset of httpx.AsyncClient the runner uses — accepts FakeClient in
    tests without inheriting from AsyncClient (which has a complex signature).

    The runner POSTs raw bytes (``content=`` plus an explicit Content-Type
    header) instead of the ``json=`` kwarg so the body the server sees is
    exactly the body the runner HMAC-signed; httpx's internal JSON serializer
    could otherwise differ (separators, ensure_ascii, key ordering) and break
    signature verification.
    """

    async def post(
        self,
        url: str,
        *,
        content: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response: ...


# --- Public schema --------------------------------------------------------- #


class EvalCaseResultRecord(BaseModel):
    """One row in the per-shot result table — persisted to ``eval_case_results``.

    ``metrics`` is a flat dict (rather than a MetricSet) because the persistence
    layer stores it as JSONB; the field order/typing is enforced by the runner
    that constructs it, not by the DB.
    """

    model_config = ConfigDict(frozen=True)

    run_id: UUID
    case_id: str = Field(min_length=1)
    shot_index: int = Field(ge=0)
    case_status: CaseStatus
    metrics: dict[str, float | None]
    diagnosis: dict[str, Any] | None = None
    incident_id: UUID | None = None
    incident_fingerprint: str
    incident_title: str
    incident_severity: str
    token_usage: dict[str, Any] | None = None
    latency_ms: int | None = None
    error_detail: str | None = None


@dataclass(frozen=True, slots=True)
class RunnerDeps:
    """Frozen container for everything the runner needs to operate.

    The CLI (T6) constructs this from ``app.state`` after the FastAPI lifespan
    starts. Tests construct it directly with fakes.

    ``poll_timeout_s`` and ``poll_interval_s`` default to production-ish values
    (60s / 250ms) but are overridable in tests so the timeout-mode test doesn't
    take a minute.
    """

    client: HttpPoster
    diagnosis_repo: DiagnosisLookup
    eval_run_repo: EvalShotPersister
    embed: EmbeddingProvider
    cassette_transport: CassetteTransport | None
    run_id: UUID
    prompt_version: str
    model_id: str
    truncate_between_cases: Callable[[], Awaitable[None]]
    webhook_secret: SecretStr
    poll_timeout_s: float = 60.0
    poll_interval_s: float = 0.25


@dataclass(frozen=True, slots=True)
class RunResult:
    """What ``run_corpus`` returns — the report writer renders this."""

    run_id: UUID
    per_case_metrics: dict[str, MetricSet]
    aggregate_metrics: MetricSet
    # shots_per_case is recorded on the result so the report can decide
    # whether the stability column is meaningful (n=1 → no variance to
    # report; report.py omits the column in that case).
    shots_per_case: int = 1
    stability: dict[str, dict[str, float]] = field(default_factory=dict)


# --- Public entry point --------------------------------------------------- #


async def run_corpus(
    *,
    cases: list[CorpusCase],
    shots_per_case: int,
    runner_deps: RunnerDeps,
) -> RunResult:
    """Drive the full corpus through the diagnosis pipeline, ``shots_per_case``
    times per case, persisting one shot record per (case, shot) and returning
    an aggregated RunResult.

    Cases are processed sequentially (the diagnosis path is single-shot per
    spec — no concurrent in-flight cases). Within a case the registry is set
    once and only mutated between cases, so an inner concurrency could in
    principle parallelize shots — but that's deferred until we have evidence
    the run wall-clock matters.
    """
    if shots_per_case < 1:
        raise ValueError(f"shots_per_case must be >= 1, got {shots_per_case}")

    per_case_shots: dict[str, list[MetricSet]] = {case.id: [] for case in cases}

    for case in cases:
        # Truncate once per case (not per shot). Each shot uses a unique
        # external_id (case.id + "-shot-" + shot_index) so shots don't
        # collide on dedup or recurred-incident logic — meaning per-case
        # truncate is enough to avoid cross-case state contamination, and
        # per-shot truncate would only create races with the async
        # enricher/diagnoser consumers reading earlier shots' events.
        await runner_deps.truncate_between_cases()
        REGISTRY.set(case)
        try:
            for shot_index in range(shots_per_case):
                if runner_deps.cassette_transport is not None:
                    runner_deps.cassette_transport.set_context(
                        CassetteContext(
                            prompt_version=runner_deps.prompt_version,
                            model_id=runner_deps.model_id,
                            case_id=case.id,
                            shot_index=shot_index,
                        )
                    )
                incident_id, case_status, diagnosis = await _fire_and_poll(
                    case, runner_deps, shot_index=shot_index
                )
                error_detail: str | None = None
                if case_status == "ok" and diagnosis is not None:
                    metrics = await _score(diagnosis, case, runner_deps.embed)
                else:
                    metrics = MetricSet(
                        category_match=0.0,
                        hypothesis_cosine=0.0,
                        action_coverage=0.0,
                        evidence_quality=None,
                    )
                    error_detail = (
                        "webhook ingest did not return 202"
                        if case_status == "ingest_failed"
                        else f"diagnosis poll timed out after {runner_deps.poll_timeout_s}s"
                    )

                shot = EvalCaseResultRecord(
                    run_id=runner_deps.run_id,
                    case_id=case.id,
                    shot_index=shot_index,
                    case_status=case_status,
                    metrics={
                        "category_match": metrics.category_match,
                        "hypothesis_cosine": metrics.hypothesis_cosine,
                        "action_coverage": metrics.action_coverage,
                        "evidence_quality": metrics.evidence_quality,
                    },
                    diagnosis=_persisted_to_dict(diagnosis) if diagnosis is not None else None,
                    incident_id=incident_id,
                    incident_fingerprint=compute_fingerprint(
                        case.alert.service,
                        normalize_title(case.alert.title),
                        case.alert.severity,
                    ),
                    incident_title=case.alert.title,
                    incident_severity=case.alert.severity,
                    token_usage=dict(diagnosis.token_usage) if diagnosis is not None else None,
                    latency_ms=diagnosis.latency_ms if diagnosis is not None else None,
                    error_detail=error_detail,
                )
                await runner_deps.eval_run_repo.persist_shot(shot)
                per_case_shots[case.id].append(metrics)
        finally:
            # Clear the registry between cases so a stray late callback (e.g.
            # delayed Kafka redelivery) doesn't pick up a stale case.
            REGISTRY.clear()

    per_case_metrics: dict[str, MetricSet] = {}
    stability: dict[str, dict[str, float]] = {}
    for case_id, shots in per_case_shots.items():
        mean, stddev = _per_case_stats(shots)
        per_case_metrics[case_id] = mean
        stability[case_id] = stddev

    return RunResult(
        run_id=runner_deps.run_id,
        per_case_metrics=per_case_metrics,
        aggregate_metrics=_aggregate_run(per_case_metrics),
        shots_per_case=shots_per_case,
        stability=stability,
    )


# --- Helpers --------------------------------------------------------------- #


async def _fire_and_poll(
    case: CorpusCase,
    deps: RunnerDeps,
    *,
    shot_index: int,
) -> tuple[UUID | None, CaseStatus, PersistedDiagnosis | None]:
    """POST the synthetic webhook, then poll for the diagnosis.

    Returns ``(incident_id, case_status, diagnosis)`` where:
      * ``ingest_failed`` → incident_id is None, diagnosis is None
      * ``timeout`` → incident_id is known, diagnosis is None
      * ``ok`` → both populated
    """
    # Merge the corpus alert fields (id/service/title/severity) into the
    # webhook body — the GenericAdapter (and the other source adapters) require
    # these as top-level fields in the POSTed payload, but the corpus YAML
    # carries them as a separate `alert.*` block (cleaner curation), with
    # `raw_payload` reserved for supplementary monitoring data. Without the
    # merge the adapter rejects with 422 ("missing required fields").
    # external_id per shot — each shot is its own incident from the system's
    # view, so we don't fight Redis dedup, the recurred-incident path, or the
    # enricher's incident lookup. The fingerprint
    # (sha256(service||title||severity)) is still constant per case, but the
    # external_id makes each row distinct.
    payload: dict[str, object] = {
        "id": f"{case.id}-shot-{shot_index}",
        "service": case.alert.service,
        "severity": case.alert.severity,
        "title": case.alert.title,
        **case.alert.raw_payload,
    }
    # Serialize ourselves and POST the exact bytes we sign — the alternative
    # (json=payload) lets httpx pick separators that may differ from what
    # the adapter recomputes the HMAC over, causing 401 with no diagnostic.
    body_bytes = json.dumps(payload).encode()
    hex_sig = compute_hmac_sha256(body_bytes, deps.webhook_secret.get_secret_value().encode())
    headers = _signature_headers_for_source(case.alert.source, hex_sig)
    headers["Content-Type"] = "application/json"

    try:
        response = await deps.client.post(
            f"/webhooks/{case.alert.source}",
            content=body_bytes,
            headers=headers,
        )
    except Exception:
        log.exception("eval_webhook_post_failed", extra={"case_id": case.id})
        return (None, "ingest_failed", None)

    if response.status_code != 202:
        log.warning(
            "eval_webhook_non_202",
            extra={
                "case_id": case.id,
                "status_code": response.status_code,
                "body": response.text[:500],
            },
        )
        return (None, "ingest_failed", None)

    body = response.json()
    incident_id_raw = body.get("incident_id")
    if not incident_id_raw:
        log.warning(
            "eval_webhook_missing_incident_id",
            extra={"case_id": case.id, "body": body},
        )
        return (None, "ingest_failed", None)
    incident_id = UUID(str(incident_id_raw))

    diagnosis = await _poll_for_diagnosis(
        deps.diagnosis_repo,
        incident_id,
        timeout_s=deps.poll_timeout_s,
        interval_s=deps.poll_interval_s,
    )
    if diagnosis is None:
        return (incident_id, "timeout", None)
    return (incident_id, "ok", diagnosis)


async def _poll_for_diagnosis(
    repo: DiagnosisLookup,
    incident_id: UUID,
    *,
    timeout_s: float,
    interval_s: float,
) -> PersistedDiagnosis | None:
    """Poll ``get_by_incident_id`` every ``interval_s`` for up to ``timeout_s``."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        diagnosis = await repo.get_by_incident_id(incident_id)
        if diagnosis is not None:
            return diagnosis
        if loop.time() >= deadline:
            return None
        await asyncio.sleep(min(interval_s, max(0.0, deadline - loop.time())))


def _signature_headers_for_source(source: str, hex_sig: str) -> dict[str, str]:
    """Per-source signature header. Matches each adapter's verify_signature."""
    if source == "generic":
        return {"X-Sentinel-Signature": f"sha256={hex_sig}"}
    if source == "sentry":
        return {"Sentry-Hook-Signature": hex_sig}
    if source == "pagerduty":
        # PagerDuty uses X-PagerDuty-Signature with v1= prefix; check adapter.
        return {"X-PagerDuty-Signature": f"v1={hex_sig}"}
    if source == "datadog":
        return {"X-Datadog-Signature": hex_sig}
    raise ValueError(f"unknown webhook source for signature header: {source!r}")


async def _score(
    diagnosis: PersistedDiagnosis,
    case: CorpusCase,
    embed: EmbeddingProvider,
) -> MetricSet:
    """Call the 4 scorers and pack into a MetricSet.

    Reconstructs the IncidentContext from the corpus seed (same translation
    the corpus fetchers do at enrichment time). This keeps the runner from
    depending on yet another repo method just to recover what it already
    has in memory.
    """
    # PersistedDiagnosis → Diagnosis-shaped score input. Use
    # ``model_construct`` (skips validation) because the persisted record can
    # legitimately have an empty ``evidence`` list: the production agent runs
    # every ref through ``verify_evidence`` and stores only verified refs
    # (`evidence=verdict.verified` in diagnosis/agent.py). A diagnosis whose
    # LLM cited only-invented refs lands in the DB with `evidence=[]` and
    # `hallucinated_evidence=True`. The Diagnosis schema requires
    # ``min_length=1`` for fresh LLM outputs but the persisted form is a
    # post-validation record. Constructing without validation lets the scorer
    # operate on what the diagnosis ACTUALLY produced (and award 0 for
    # evidence_quality, which is correct).
    from sentinel.schemas.diagnosis import Diagnosis

    scoring_diagnosis = Diagnosis.model_construct(
        hypothesis=diagnosis.hypothesis,
        confidence=float(diagnosis.confidence),
        reasoning=diagnosis.reasoning,
        evidence=diagnosis.evidence,
        suggested_actions=diagnosis.suggested_actions,
        likely_category=diagnosis.likely_category,
    )

    gt = case.ground_truth
    category = score_category(scoring_diagnosis, gt)
    hypothesis = await score_hypothesis(scoring_diagnosis, gt, embed)
    actions = await score_action_coverage(scoring_diagnosis, gt, embed)
    ctx = _seed_to_incident_context(case)
    evidence = score_evidence_quality(scoring_diagnosis, ctx)

    return MetricSet(
        category_match=category,
        hypothesis_cosine=hypothesis,
        action_coverage=actions,
        evidence_quality=evidence,
    )


def _seed_to_incident_context(case: CorpusCase) -> IncidentContext:
    """Translate ``case.context_seed`` → ``IncidentContext`` using the same
    field mapping as the corpus fetchers.

    Factored out so the runner doesn't replicate the 6 fetcher classes inline
    just to compute evidence_quality. The fetchers themselves still exist
    because they hook into the enrichment pipeline; this helper produces the
    flat context the scoring step needs.
    """
    from datetime import UTC, datetime
    from uuid import uuid4

    now = datetime.now(UTC)
    seed = case.context_seed

    # Translation mirrors the per-fetcher logic in sentinel/evals/fetcher_override.py
    # (similar root_cause / remediation default to "", runbook cosine defaults
    # to 0.0). Kept inline rather than reusing the Fetcher classes because the
    # fetchers go through the ActiveCaseRegistry — here we already have the
    # case in hand and don't want a registry round-trip.
    return IncidentContext(
        incident_id=uuid4(),
        assembled_at=now,
        recent_deploys=FetcherResult[DeployItem](
            status="ok",
            data=[
                DeployItem(
                    id=d.id,
                    service=d.service,
                    sha=d.sha,
                    pr_number=d.pr_number,
                    pr_title=d.pr_title,
                    pr_diff_summary=d.pr_diff_summary,
                    deployed_at=d.deployed_at,
                    deployed_by=d.deployed_by,
                )
                for d in seed.deploys
            ],
            fetched_at=now,
        ),
        related_alerts=FetcherResult[RelatedAlertItem](
            status="ok",
            data=[
                RelatedAlertItem(
                    id=a.id,
                    service=a.service,
                    severity=a.severity,
                    title=a.title,
                    opened_at=a.opened_at,
                )
                for a in seed.related_alerts
            ],
            fetched_at=now,
        ),
        similar_incidents=FetcherResult[SimilarIncidentItem](
            status="ok",
            data=[
                SimilarIncidentItem(
                    id=s.id,
                    title=s.title,
                    root_cause=s.root_cause or "",
                    remediation=s.remediation or "",
                    cosine_similarity=s.cosine_similarity,
                )
                for s in seed.similar_incidents
            ],
            fetched_at=now,
        ),
        runbooks=FetcherResult[RunbookItem](
            status="ok",
            data=[
                RunbookItem(
                    id=r.id,
                    title=r.title,
                    content=r.content,
                    cosine_similarity=0.0,
                )
                for r in seed.runbooks
            ],
            fetched_at=now,
        ),
        recent_logs=FetcherResult[LogLine](
            status="ok",
            data=[
                LogLine(
                    id=lg.id,
                    timestamp=lg.timestamp,
                    level=lg.level,
                    service=lg.service,
                    message=lg.message,
                )
                for lg in seed.recent_logs
            ],
            fetched_at=now,
        ),
        active_alerts=FetcherResult[RelatedAlertItem](
            status="ok",
            data=[
                RelatedAlertItem(
                    id=a.id,
                    service=a.service,
                    severity=a.severity,
                    title=a.title,
                    opened_at=a.opened_at,
                )
                for a in seed.active_alerts
            ],
            fetched_at=now,
        ),
    )


def _persisted_to_dict(diagnosis: PersistedDiagnosis) -> dict[str, Any]:
    """JSON-serializable view of a PersistedDiagnosis (frozen dataclass; no
    pydantic ``.model_dump`` available)."""
    return {
        "hypothesis": diagnosis.hypothesis,
        "confidence": float(diagnosis.confidence),
        "reasoning": diagnosis.reasoning,
        "evidence": [e.model_dump(mode="json") for e in diagnosis.evidence],
        "suggested_actions": [a.model_dump(mode="json") for a in diagnosis.suggested_actions],
        "likely_category": diagnosis.likely_category,
        "hallucinated_evidence": diagnosis.hallucinated_evidence,
        "model": diagnosis.model,
        "prompt_version": diagnosis.prompt_version,
        "latency_ms": diagnosis.latency_ms,
        "token_usage": dict(diagnosis.token_usage),
    }


# --- Aggregation ----------------------------------------------------------- #


def _per_case_stats(
    shots: list[MetricSet],
) -> tuple[MetricSet, dict[str, float]]:
    """Mean MetricSet + per-metric sample stddev across shots of one case.

    With n=1, stddev is 0 for every metric (no variance signal). For
    evidence_quality, None values are excluded from both mean and stddev —
    a shot that errored out before producing a diagnosis can't contribute to
    the evidence-quality distribution.
    """
    if not shots:
        zero = MetricSet(
            category_match=0.0,
            hypothesis_cosine=0.0,
            action_coverage=0.0,
            evidence_quality=None,
        )
        return zero, {
            "category_match": 0.0,
            "hypothesis_cosine": 0.0,
            "action_coverage": 0.0,
            "evidence_quality": 0.0,
        }

    cat = [s.category_match for s in shots]
    hyp = [s.hypothesis_cosine for s in shots]
    act = [s.action_coverage for s in shots]
    ev = [s.evidence_quality for s in shots if s.evidence_quality is not None]

    mean = MetricSet(
        category_match=statistics.fmean(cat),
        hypothesis_cosine=statistics.fmean(hyp),
        action_coverage=statistics.fmean(act),
        evidence_quality=statistics.fmean(ev) if ev else None,
    )
    stddev = {
        "category_match": _safe_stddev(cat),
        "hypothesis_cosine": _safe_stddev(hyp),
        "action_coverage": _safe_stddev(act),
        "evidence_quality": _safe_stddev(ev),
    }
    return mean, stddev


def _safe_stddev(values: list[float]) -> float:
    """Sample stddev (n-1) when n >= 2; 0.0 otherwise. ``statistics.stdev``
    raises on n<2, which is noisy in the n=1 single-shot path."""
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values)


def _aggregate_run(per_case: Mapping[str, MetricSet]) -> MetricSet:
    """Mean per-case mean. Cases with status != ok contribute zeros to the
    numeric metrics (per_case_metrics carried 0s for failed shots); their
    evidence_quality is None and is therefore excluded from the
    evidence_quality average (so a run of all-failures returns
    evidence_quality=None rather than 0)."""
    if not per_case:
        return MetricSet(
            category_match=0.0,
            hypothesis_cosine=0.0,
            action_coverage=0.0,
            evidence_quality=None,
        )
    cat = [m.category_match for m in per_case.values()]
    hyp = [m.hypothesis_cosine for m in per_case.values()]
    act = [m.action_coverage for m in per_case.values()]
    ev = [m.evidence_quality for m in per_case.values() if m.evidence_quality is not None]
    return MetricSet(
        category_match=statistics.fmean(cat),
        hypothesis_cosine=statistics.fmean(hyp),
        action_coverage=statistics.fmean(act),
        evidence_quality=statistics.fmean(ev) if ev else None,
    )


__all__ = [
    "CaseStatus",
    "DiagnosisLookup",
    "EvalCaseResultRecord",
    "EvalShotPersister",
    "HttpPoster",
    "RunResult",
    "RunnerDeps",
    "run_corpus",
]
