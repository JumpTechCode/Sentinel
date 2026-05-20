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
    return float(dot / (na * nb))


def _collect_context_ids(ctx: IncidentContext) -> set[str]:
    """Walk the full IncidentContext and gather every item.id — the contract
    the diagnosis prompt + validation gate also rely on."""
    ids: set[str] = set()
    for deploy in ctx.recent_deploys.data:
        ids.add(deploy.id)
    for alert in ctx.related_alerts.data:
        ids.add(alert.id)
    for similar in ctx.similar_incidents.data:
        ids.add(similar.id)
    for runbook in ctx.runbooks.data:
        ids.add(runbook.id)
    for log in ctx.recent_logs.data:
        ids.add(log.id)
    for active in ctx.active_alerts.data:
        ids.add(active.id)
    return ids
