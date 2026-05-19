# sentinel/enrichment/defaults.py
"""No-op adapters that return degraded FetcherResults.

Returned when a real implementation (e.g. pgvector from Work Area H, or a
log adapter) is not yet wired. The pipeline always assembles a context;
sections without an implementation are explicitly degraded rather than
silently missing.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sentinel.schemas.context import (
    FetcherResult,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)

_NOT_CONFIGURED = "not_configured"


class NotConfiguredSimilarIncidents:
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        exclude_incident_id: UUID | None,
    ) -> FetcherResult[SimilarIncidentItem]:
        return FetcherResult(
            status="degraded",
            data=[],
            error=_NOT_CONFIGURED,
            fetched_at=datetime.now(UTC),
        )


class NotConfiguredRunbookRetrieval:
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        min_cosine: float,
    ) -> FetcherResult[RunbookItem]:
        return FetcherResult(
            status="degraded",
            data=[],
            error=_NOT_CONFIGURED,
            fetched_at=datetime.now(UTC),
        )


class NotConfiguredLogSearch:
    async def recent_errors(
        self,
        *,
        service: str,
        since: datetime,
        limit: int,
    ) -> FetcherResult[LogLine]:
        return FetcherResult(
            status="degraded",
            data=[],
            error=_NOT_CONFIGURED,
            fetched_at=datetime.now(UTC),
        )


class NotConfiguredActiveAlerts:
    async def active_for_service(
        self,
        *,
        service: str,
        exclude_external_id: str,
    ) -> FetcherResult[RelatedAlertItem]:
        return FetcherResult(
            status="degraded",
            data=[],
            error=_NOT_CONFIGURED,
            fetched_at=datetime.now(UTC),
        )
