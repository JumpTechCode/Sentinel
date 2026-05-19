# sentinel/schemas/context.py
"""Pre-assembled incident context consumed by the diagnosis agent."""

from __future__ import annotations

from datetime import datetime
from typing import Generic, Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from sentinel.schemas.enums import SeverityType

T = TypeVar("T")

FetcherStatus = Literal["ok", "degraded", "failed"]


class FetcherResult(BaseModel, Generic[T]):
    """Result envelope for every parallel fetcher.

    `degraded` means partial data; `failed` means no data (and `error` is required).
    """

    model_config = ConfigDict(frozen=True)

    status: FetcherStatus
    data: list[T] = Field(default_factory=list)
    error: str | None = None
    fetched_at: datetime

    @model_validator(mode="after")
    def _failed_requires_error(self) -> FetcherResult[T]:
        if self.status == "failed" and not self.error:
            raise ValueError("failed FetcherResult requires a non-empty error message")
        return self


class DeployItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `deploy:<sha>`
    service: str
    sha: str
    pr_number: int | None = None
    pr_title: str | None = None
    pr_diff_summary: str | None = None
    deployed_at: datetime
    deployed_by: str | None = None


class RelatedAlertItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `related:<uuid>`
    service: str
    severity: SeverityType
    title: str
    opened_at: datetime


class SimilarIncidentItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `similar:<uuid>`
    title: str
    root_cause: str
    remediation: str
    cosine_similarity: float = Field(ge=-1.0, le=1.0)


class RunbookItem(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `runbook:<uuid>`
    title: str
    content: str
    cosine_similarity: float = Field(ge=-1.0, le=1.0)


class LogLine(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str  # `log:<idx>`
    timestamp: datetime
    level: str
    service: str
    message: str


class IncidentContext(BaseModel):
    """All six fetcher sections, each as its own FetcherResult."""

    model_config = ConfigDict(frozen=True)

    incident_id: UUID
    assembled_at: datetime

    recent_deploys: FetcherResult[DeployItem]
    related_alerts: FetcherResult[RelatedAlertItem]
    similar_incidents: FetcherResult[SimilarIncidentItem]
    runbooks: FetcherResult[RunbookItem]
    recent_logs: FetcherResult[LogLine]
    active_alerts: FetcherResult[RelatedAlertItem]
