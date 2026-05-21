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


async def score_hypothesis(d: Diagnosis, gt: GroundTruth, embed: EmbeddingProvider) -> float:
    """Cosine similarity of hypothesis vs root_cause embeddings, clamped to [0, 1].

    Reported as the raw continuous cosine rather than a thresholded pass/fail
    — preserves signal for the regression gate (a drift from 0.82 → 0.71 is
    meaningful and would be hidden by binary scoring).
    """
    hyp_vec = await embed.embed(d.hypothesis)
    rc_vec = await embed.embed(gt.root_cause)
    return max(0.0, _cosine(hyp_vec, rc_vec))


async def score_action_coverage(d: Diagnosis, gt: GroundTruth, embed: EmbeddingProvider) -> float:
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
    """Fraction of EvidenceRef.id values that resolve to a context item under
    their declared kind.

    This is the strict ID-resolution gate — the production hallucination
    metric (in diagnosis/validation.py) handles the "cited but unsupported"
    case via confidence capping. The scorer only counts the deterministic
    (kind, id) match.

    Returns 0.0 if the diagnosis has no evidence at all (the schema enforces
    at least one ref on a parsed Diagnosis, so this branch is defensive).

    Uses the same lenient (kind, id) match as ``verify_evidence`` (see ADR
    0007): a ref resolves if either its bare id matches the bucket under its
    declared kind, or its kind-prefixed form matches. The LLM consistently
    emits bare ids ("abc123") while the context items store the prefixed
    form ("deploy:abc123"); without the lenient match every cassette-recorded
    diagnosis would score 0. The bucket is kind-scoped (not a flat set) so
    wrong-kind references — e.g. ``deploy:0`` claimed for a log line — still
    register as invented.
    """
    if not d.evidence:
        return 0.0
    buckets = _collect_context_ids_by_kind(ctx)
    resolved = 0
    for e in d.evidence:
        bucket = buckets.get(e.kind, set())
        if e.id in bucket or f"{e.kind}:{e.id}" in bucket:
            resolved += 1
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
    return float(dot / (na * nb))


def _collect_context_ids_by_kind(ctx: IncidentContext) -> dict[str, set[str]]:
    """Bucket of {EvidenceKind: {id, ...}} matching the layout that
    ``verify_evidence`` uses. The scorer reuses this so the lenient match
    cannot silently cross kind boundaries — see ADR 0007.

    ``related_alert`` aggregates both ``related_alerts`` and ``active_alerts``
    (same as the validator), since the schema's EvidenceKind enum exposes
    them as one kind.
    """
    return {
        "deploy": {d.id for d in ctx.recent_deploys.data},
        "similar_incident": {s.id for s in ctx.similar_incidents.data},
        "runbook": {r.id for r in ctx.runbooks.data},
        "log": {lg.id for lg in ctx.recent_logs.data},
        "related_alert": (
            {r.id for r in ctx.related_alerts.data} | {a.id for a in ctx.active_alerts.data}
        ),
    }
