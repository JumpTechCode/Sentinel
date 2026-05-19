# sentinel/schemas/diagnosis.py
"""Diagnosis structured output schema — what the LLM must produce.

The confidence rubric is encoded in the system prompt; this module enforces the
syntactic envelope (range, min_length, enum values). Semantic validation
(evidence-citation gate) lives in `sentinel/diagnosis/validation.py`.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from sentinel.schemas.enums import CategoryType, EvidenceKindType


class EvidenceRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: EvidenceKindType
    id: str = Field(min_length=1)
    note: str = Field(min_length=1)


class SuggestedAction(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    description: str = Field(min_length=1)
    command: str | None = None
    risk: Literal["low", "medium", "high"]
    rationale: str = Field(min_length=1)
    requires_human_approval: bool = True


class Diagnosis(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    hypothesis: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(min_length=1)
    evidence: list[EvidenceRef] = Field(min_length=1)
    suggested_actions: list[SuggestedAction] = Field(default_factory=list)
    likely_category: CategoryType
