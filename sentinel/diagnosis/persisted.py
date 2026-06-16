# sentinel/diagnosis/persisted.py
"""Boundary record between the agent and the repository.

`Diagnosis` (the Pydantic schema) enforces `min_length=1` on evidence — correct
for the wire-level contract with the LLM. The persisted record drops that
constraint because a 100%-hallucinated diagnosis still needs to be written
(with the verified-evidence list empty and the hallucinated_evidence flag set).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sentinel.schemas.api import DiagnosisView

from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction
from sentinel.schemas.enums import CategoryType


@dataclass(frozen=True, slots=True)
class PersistedDiagnosis:
    hypothesis: str
    confidence: Decimal
    reasoning: str
    evidence: list[EvidenceRef]
    suggested_actions: list[SuggestedAction]
    likely_category: CategoryType
    hallucinated_evidence: bool
    model: str
    prompt_version: str
    latency_ms: int
    token_usage: dict[str, Any]


def to_view(record: PersistedDiagnosis) -> DiagnosisView:
    """Convert a persisted diagnosis into the relaxed API read model.

    Local import keeps `persisted.py` free of an API-schema import at module
    load (mirrors the local-import style in the repository layer).
    """
    from sentinel.schemas.api import DiagnosisView

    return DiagnosisView(
        hypothesis=record.hypothesis,
        confidence=float(record.confidence),
        reasoning=record.reasoning,
        evidence=record.evidence,
        suggested_actions=record.suggested_actions,
        likely_category=record.likely_category,
        hallucinated_evidence=record.hallucinated_evidence,
        model=record.model,
        prompt_version=record.prompt_version,
    )
