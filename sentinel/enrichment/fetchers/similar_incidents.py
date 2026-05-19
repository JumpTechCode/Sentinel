# sentinel/enrichment/fetchers/similar_incidents.py
"""Top-k similar past incidents from pgvector memory (or degraded if unwired)."""

from __future__ import annotations

from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, SimilarIncidentItem


class SimilarIncidentsFetcher:
    name = "similar_incidents"
    timeout_s = 5.0

    async def fetch(
        self,
        incident: Any,
        deps: EnrichmentDeps,
    ) -> FetcherResult[SimilarIncidentItem]:
        return await deps.similar_incidents.top_k(
            query_text=f"{incident.service} {incident.title}",
            k=5,
            exclude_incident_id=incident.id,
        )
