# sentinel/enrichment/protocols.py
"""Protocols for enrichment dependencies that may not be built yet.

Real implementations land in:
- memory/ (Work Area H): EmbeddingProvider, SimilarIncidentRetrieval, RunbookRetrieval
- integrations/ (per source): LogSearchAdapter, ActiveAlertsAdapter

Until they're wired in, the default no-op implementations in
sentinel/enrichment/defaults.py supply degraded results.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol
from uuid import UUID

from sentinel.schemas.context import (
    FetcherResult,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)


class EmbeddingProvider(Protocol):
    async def embed(self, text: str) -> list[float]: ...


class SimilarIncidentRetrieval(Protocol):
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        exclude_incident_id: UUID | None,
    ) -> FetcherResult[SimilarIncidentItem]: ...


class RunbookRetrieval(Protocol):
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        min_cosine: float,
    ) -> FetcherResult[RunbookItem]: ...


class LogSearchAdapter(Protocol):
    async def recent_errors(
        self,
        *,
        service: str,
        since: datetime,
        limit: int,
    ) -> FetcherResult[LogLine]: ...


class ActiveAlertsAdapter(Protocol):
    async def active_for_service(
        self,
        *,
        service: str,
        exclude_external_id: str,
    ) -> FetcherResult[RelatedAlertItem]: ...
