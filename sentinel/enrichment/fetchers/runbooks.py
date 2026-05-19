# sentinel/enrichment/fetchers/runbooks.py
"""Top-k runbook matches by title similarity (or degraded if unwired)."""

from __future__ import annotations

from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, RunbookItem


class RunbooksFetcher:
    name = "runbooks"
    timeout_s = 5.0

    async def fetch(
        self,
        incident: Any,
        deps: EnrichmentDeps,
    ) -> FetcherResult[RunbookItem]:
        return await deps.runbooks.top_k(
            query_text=incident.title,
            k=3,
            min_cosine=0.6,
        )
