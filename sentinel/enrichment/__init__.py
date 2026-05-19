# sentinel/enrichment/__init__.py
"""Enrichment public API."""

from sentinel.enrichment.circuit_breaker import CircuitBreaker, CircuitOpenError
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.fetchers import default_fetchers
from sentinel.enrichment.metrics_wiring import make_breaker
from sentinel.enrichment.orchestrator import assemble
from sentinel.enrichment.protocols import (
    ActiveAlertsAdapter,
    EmbeddingProvider,
    LogSearchAdapter,
    RunbookRetrieval,
    SimilarIncidentRetrieval,
)

__all__ = [
    "ActiveAlertsAdapter",
    "CircuitBreaker",
    "CircuitOpenError",
    "EmbeddingProvider",
    "EnrichmentDeps",
    "LogSearchAdapter",
    "RunbookRetrieval",
    "SimilarIncidentRetrieval",
    "assemble",
    "default_fetchers",
    "make_breaker",
]
