# sentinel/diagnosis/validation.py
"""Evidence-citation gate — the headline quality signal.

For every EvidenceRef in the diagnosis, check that its `id` appears in the
context **under its declared `kind`**. Wrong-kind references count as invented.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from sentinel.schemas.context import IncidentContext
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef


@dataclass(frozen=True, slots=True)
class EvidenceVerdict:
    verified: list[EvidenceRef] = field(default_factory=list)
    invented: list[EvidenceRef] = field(default_factory=list)

    @property
    def hallucinated(self) -> bool:
        return bool(self.invented)


def _index_context(ctx: IncidentContext) -> dict[str, set[str]]:
    """Bucket of {kind: {id, ...}} for fast membership tests.

    Note: `related_alert` aggregates both `related_alerts` AND `active_alerts`.
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


def verify_evidence(diagnosis: Diagnosis, context: IncidentContext) -> EvidenceVerdict:
    """Verify every EvidenceRef in *diagnosis* against *context*.

    Returns an EvidenceVerdict with two partitions:
    - `verified`: refs whose id appears in the context under their declared kind.
    - `invented`: refs whose id is absent or present under a different kind.

    A non-empty `invented` list sets `hallucinated = True`.
    """
    index = _index_context(context)
    verified: list[EvidenceRef] = []
    invented: list[EvidenceRef] = []
    for ref in diagnosis.evidence:
        bucket = index.get(ref.kind, set())
        # Accept either bare id ("abc123") or kind-prefixed id ("deploy:abc123").
        # The prompt renders bracket citations as `[deploy:abc123]`; the LLM
        # sometimes copies the bracket form verbatim into the id field and
        # sometimes splits on the colon. Both are equivalent claims about the
        # same context item, so both count as verified — what matters is that
        # the (kind, identity) pair resolves to a context item under the
        # declared kind. Wrong-kind references still fall through to invented.
        prefixed = f"{ref.kind}:{ref.id}"
        if ref.id in bucket or prefixed in bucket:
            verified.append(ref)
        else:
            invented.append(ref)
    return EvidenceVerdict(verified=verified, invented=invented)
