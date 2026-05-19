# sentinel/enrichment/fetchers/__init__.py
"""Built-in fetchers + a factory wiring them together with default Protocols."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sentinel.enrichment.fetchers.active_alerts import ActiveAlertsFetcher
from sentinel.enrichment.fetchers.deploys import DeploysFetcher
from sentinel.enrichment.fetchers.recent_logs import RecentLogsFetcher
from sentinel.enrichment.fetchers.related_alerts import RelatedAlertsFetcher
from sentinel.enrichment.fetchers.runbooks import RunbooksFetcher
from sentinel.enrichment.fetchers.similar_incidents import SimilarIncidentsFetcher

if TYPE_CHECKING:
    from sentinel.enrichment.orchestrator import Fetcher


def default_fetchers() -> tuple[Fetcher, ...]:
    """Returns the six built-in fetchers in orchestrator section order.

    Order matches `_SECTION_NAMES` in `sentinel.enrichment.orchestrator`:
    deploys, related_alerts, similar_incidents, runbooks, recent_logs, active_alerts.
    Stubs delegate to their Protocol on `EnrichmentDeps`; when the deps wire in
    NotConfigured* defaults the fetcher returns a `degraded` FetcherResult. Once
    real adapters land (Work Area H), the same call sites return real data.
    """
    return (
        DeploysFetcher(),
        RelatedAlertsFetcher(),
        SimilarIncidentsFetcher(),
        RunbooksFetcher(),
        RecentLogsFetcher(),
        ActiveAlertsFetcher(),
    )
