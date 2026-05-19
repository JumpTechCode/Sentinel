# sentinel/enrichment/fetchers/deploys.py
"""Recent deploys on the incident's service in the last 24h."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import DeployItem, FetcherResult


class DeploysFetcher:
    name = "deploys"
    timeout_s = 5.0

    async def fetch(
        self,
        incident: Any,
        deps: EnrichmentDeps,
    ) -> FetcherResult[DeployItem]:
        since = datetime.now(UTC) - timedelta(hours=24)
        rows = await deps.deploy_repo.recent_for_service_window(
            service=incident.service,
            since=since,
        )
        return FetcherResult(
            status="ok",
            data=list(rows),
            fetched_at=datetime.now(UTC),
        )
