# sentinel/schemas/api.py
"""Request and response models for the HTTP API surface.

These are imported by `sentinel/api/routes/*` (lands in Work Area I). Defining
them here keeps the wire contract decoupled from FastAPI plumbing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from sentinel.schemas.context import IncidentContext
from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction
from sentinel.schemas.enums import (
    CategoryType,
    IncidentStatusType,
    SeverityType,
    SourceType,
)

# --- Webhooks / incidents --------------------------------------------------- #


class WebhookAcceptedResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    status: Literal["accepted", "recurred", "duplicate"]
    incident_id: UUID | None = None


class CreateIncidentRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    source: SourceType
    external_id: str = Field(min_length=1)
    service: str = Field(min_length=1)
    severity: SeverityType
    title: str = Field(min_length=1)
    raw_payload: dict[str, Any]


class ResolveIncidentRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    root_cause: str = Field(min_length=1)
    remediation: str = Field(min_length=1)
    category: CategoryType
    diagnosis_was_correct: bool | None = None
    notes: str | None = None
    resolved_by: str | None = None


class ResolveIncidentResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    incident_id: UUID
    status: Literal["resolved"]
    resolved_at: datetime
    event_id: UUID  # staged `incident.resolved` outbox event_id


class IncidentListItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: UUID
    service: str
    severity: SeverityType
    status: IncidentStatusType
    title: str
    opened_at: datetime
    resolved_at: datetime | None = None


class IncidentListResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    items: list[IncidentListItem]
    total: int
    limit: int
    offset: int


class DiagnosisView(BaseModel):
    """Read model for a PERSISTED diagnosis on API responses.

    Mirrors `Diagnosis`'s constraints and relaxes ONLY `evidence` to allow an
    empty list: a fully-hallucinated diagnosis is stored with verified-evidence
    `[]` and `hallucinated_evidence=True`, which the strict wire `Diagnosis`
    (evidence min_length=1) cannot represent. `hypothesis`/`reasoning`/
    `confidence` keep their bounds — persisted records never violate them, so
    the constraints surface data corruption early rather than passing it through.
    `latency_ms`/`token_usage` are intentionally omitted: they are internal
    metrics exposed via `/metrics`, not part of the diagnosis read shape.
    """

    model_config = ConfigDict(frozen=True)
    hypothesis: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = Field(min_length=1)
    evidence: list[EvidenceRef]
    suggested_actions: list[SuggestedAction]
    likely_category: CategoryType
    hallucinated_evidence: bool
    model: str
    prompt_version: str


class IncidentDetailResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: UUID
    source: SourceType
    external_id: str
    service: str
    severity: SeverityType
    status: IncidentStatusType
    title: str
    fingerprint: str
    opened_at: datetime
    resolved_at: datetime | None = None
    context: IncidentContext | None = None
    diagnoses: list[DiagnosisView] = Field(default_factory=list)


class DiagnoseResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    incident_id: UUID
    diagnosis: DiagnosisView
    persisted: Literal["new", "duplicate"] = Field(
        description=(
            "Dedup outcome of the underlying save: 'new' = a diagnosis row was "
            "written; 'duplicate' = an identical (incident, prompt, model) "
            "diagnosis already existed and was returned unchanged."
        )
    )


# --- Eval runs -------------------------------------------------------------- #


class EvalRunSummary(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: UUID
    started_at: datetime
    completed_at: datetime | None
    model: str
    prompt_version: str
    corpus_version: str
    summary: dict[str, float] | None  # aggregated scorer outputs


# --- Health ----------------------------------------------------------------- #


class HealthResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    status: Literal["ok", "degraded"]
    checks: dict[str, str]
