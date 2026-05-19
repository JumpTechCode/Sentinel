# sentinel/enrichment/fetchers/active_alerts.py
"""Currently-firing alerts on the incident's service (or degraded if unwired)."""

from __future__ import annotations

from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, RelatedAlertItem


class ActiveAlertsFetcher:
    name = "active_alerts"
    timeout_s = 5.0

    async def fetch(
        self,
        incident: Any,
        deps: EnrichmentDeps,
    ) -> FetcherResult[RelatedAlertItem]:
        return await deps.active_alerts.active_for_service(
            service=incident.service,
            exclude_external_id=incident.external_id,
        )
