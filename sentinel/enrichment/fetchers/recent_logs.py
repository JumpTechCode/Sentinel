# sentinel/enrichment/fetchers/recent_logs.py
"""Recent error log lines for the incident's service (or degraded if unwired)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, LogLine


class RecentLogsFetcher:
    name = "recent_logs"
    timeout_s = 5.0

    async def fetch(
        self,
        incident: Any,
        deps: EnrichmentDeps,
    ) -> FetcherResult[LogLine]:
        since = datetime.now(UTC) - timedelta(minutes=5)
        return await deps.log_search.recent_errors(
            service=incident.service,
            since=since,
            limit=50,
        )
