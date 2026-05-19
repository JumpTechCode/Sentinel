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
from typing import Any

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
