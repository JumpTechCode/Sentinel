# sentinel/enrichment/fetchers/related_alerts.py
"""Other incidents on the same service in the last 24h, excluding this one."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, RelatedAlertItem


class RelatedAlertsFetcher:
    name = "related_alerts"
    timeout_s = 5.0

    async def fetch(
        self,
        incident: Any,
        deps: EnrichmentDeps,
    ) -> FetcherResult[RelatedAlertItem]:
        since = datetime.now(UTC) - timedelta(hours=24)
        rows = await deps.incident_repo.recent_for_service(
            service=incident.service,
            since=since,
            exclude_incident_id=incident.id,
        )
        return FetcherResult(
            status="ok",
            data=list(rows),
            fetched_at=datetime.now(UTC),
        )
