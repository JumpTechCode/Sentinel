# sentinel/enrichment/deps.py
"""Dependency bundle passed to each fetcher.

Constructed once at app startup in app.py lifespan. Fetchers never
construct DB sessions or HTTP clients; they consume what's in here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentinel.enrichment.circuit_breaker import CircuitBreaker
    from sentinel.enrichment.orchestrator import Fetcher
    from sentinel.enrichment.protocols import (
        ActiveAlertsAdapter,
        LogSearchAdapter,
        RunbookRetrieval,
        SimilarIncidentRetrieval,
    )
    from sentinel.persistence.repositories import (
        DeployRepository,
        IncidentRepository,
    )


@dataclass(frozen=True, slots=True)
class EnrichmentDeps:
    fetchers: tuple[Fetcher, ...]
    breakers: dict[str, CircuitBreaker]
    incident_repo: IncidentRepository
    deploy_repo: DeployRepository
    similar_incidents: SimilarIncidentRetrieval
    runbooks: RunbookRetrieval
    log_search: LogSearchAdapter
    active_alerts: ActiveAlertsAdapter
