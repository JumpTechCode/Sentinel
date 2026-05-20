"""Corpus-replay drop-ins for the 6 production Fetcher implementations.

In eval mode the orchestrator runs the same parallel-fetch logic, but each
fetcher reads from an ActiveCaseRegistry (set by the runner before firing
each synthetic webhook) instead of hitting the DB / external systems.

Translation: the corpus YAML uses ``*Seed`` types (sentinel/evals/schema.py) —
strict subsets carrying only curator-supplied fields. These fetchers
translate seed → production context type (sentinel/schemas/context.py) at
fetch time, supplying defaults for fetcher-computed fields (e.g.
``fetched_at``) and for required production fields the seed leaves
optional (see Deviations note below).

Deviations from a one-to-one shape match (vs. the plan):
- ``SimilarIncidentItem`` requires ``root_cause: str`` and
  ``remediation: str`` in production; the seed makes both ``Optional[str]``
  (curator may not always have a known root cause). Translation defaults
  missing values to the empty string.
- ``SimilarIncidentItem`` has no ``resolved_at`` field in production; the
  seed carries one for curator context but it is dropped at translation.
- ``RunbookItem`` requires ``cosine_similarity: float`` in production; the
  seed omits it (runbook retrieval scoring is not part of the corpus-author
  contract). Translation defaults to ``0.0``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sentinel.evals.schema import CorpusCase
from sentinel.schemas.context import (
    DeployItem,
    FetcherResult,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)

_FETCHER_TIMEOUT_S = 0.5  # arbitrary; CorpusFetchers never block on I/O


class ActiveCaseRegistry:
    """Per-process holder for the currently-active corpus case.

    The runner sets the active case before POSTing each synthetic webhook;
    the fetchers read it during the enrichment fan-out. NOT thread-safe —
    the eval runner is single-threaded by design (the orchestrator fans out
    via asyncio, not threads).
    """

    def __init__(self) -> None:
        self._case: CorpusCase | None = None

    def get(self) -> CorpusCase | None:
        return self._case

    def set(self, case: CorpusCase) -> None:
        self._case = case

    def clear(self) -> None:
        self._case = None


def _now() -> datetime:
    return datetime.now(UTC)


def _require_active(registry: ActiveCaseRegistry) -> CorpusCase:
    case = registry.get()
    if case is None:
        raise RuntimeError(
            "no active corpus case — the eval runner must set the registry "
            "before triggering enrichment"
        )
    return case


# --- Six concrete fetchers, one per production Fetcher ---


class CorpusDeploysFetcher:
    name = "deploys"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[DeployItem]:
        case = _require_active(self._registry)
        items = [
            DeployItem(
                id=d.id,
                service=d.service,
                sha=d.sha,
                pr_number=d.pr_number,
                pr_title=d.pr_title,
                pr_diff_summary=d.pr_diff_summary,
                deployed_at=d.deployed_at,
                deployed_by=d.deployed_by,
            )
            for d in case.context_seed.deploys
        ]
        return FetcherResult[DeployItem](status="ok", data=items, fetched_at=_now())


class CorpusRelatedAlertsFetcher:
    name = "related_alerts"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[RelatedAlertItem]:
        case = _require_active(self._registry)
        items = [
            RelatedAlertItem(
                id=a.id,
                service=a.service,
                severity=a.severity,
                title=a.title,
                opened_at=a.opened_at,
            )
            for a in case.context_seed.related_alerts
        ]
        return FetcherResult[RelatedAlertItem](status="ok", data=items, fetched_at=_now())


class CorpusSimilarIncidentsFetcher:
    name = "similar_incidents"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[SimilarIncidentItem]:
        case = _require_active(self._registry)
        items = [
            SimilarIncidentItem(
                id=s.id,
                title=s.title,
                # Production requires non-Optional str; default to "" when the
                # curator has no known root cause / remediation.
                root_cause=s.root_cause or "",
                remediation=s.remediation or "",
                cosine_similarity=s.cosine_similarity,
            )
            for s in case.context_seed.similar_incidents
        ]
        return FetcherResult[SimilarIncidentItem](status="ok", data=items, fetched_at=_now())


class CorpusRunbooksFetcher:
    name = "runbooks"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[RunbookItem]:
        case = _require_active(self._registry)
        items = [
            RunbookItem(
                id=r.id,
                title=r.title,
                content=r.content,
                # Production requires cosine_similarity; corpus seed omits it
                # because runbook-retrieval ranking is not a corpus-author
                # concern. Default to 0.0 (neutral).
                cosine_similarity=0.0,
            )
            for r in case.context_seed.runbooks
        ]
        return FetcherResult[RunbookItem](status="ok", data=items, fetched_at=_now())


class CorpusRecentLogsFetcher:
    name = "recent_logs"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[LogLine]:
        case = _require_active(self._registry)
        items = [
            LogLine(
                id=lg.id,
                timestamp=lg.timestamp,
                level=lg.level,
                service=lg.service,
                message=lg.message,
            )
            for lg in case.context_seed.recent_logs
        ]
        return FetcherResult[LogLine](status="ok", data=items, fetched_at=_now())


class CorpusActiveAlertsFetcher:
    name = "active_alerts"
    timeout_s = _FETCHER_TIMEOUT_S

    def __init__(self, registry: ActiveCaseRegistry) -> None:
        self._registry = registry

    async def fetch(self, incident: Any, deps: Any) -> FetcherResult[RelatedAlertItem]:
        case = _require_active(self._registry)
        items = [
            RelatedAlertItem(
                id=a.id,
                service=a.service,
                severity=a.severity,
                title=a.title,
                opened_at=a.opened_at,
            )
            for a in case.context_seed.active_alerts
        ]
        return FetcherResult[RelatedAlertItem](status="ok", data=items, fetched_at=_now())


def corpus_fetchers(
    registry: ActiveCaseRegistry,
) -> tuple[
    CorpusDeploysFetcher,
    CorpusRelatedAlertsFetcher,
    CorpusSimilarIncidentsFetcher,
    CorpusRunbooksFetcher,
    CorpusRecentLogsFetcher,
    CorpusActiveAlertsFetcher,
]:
    """Build the standard tuple of all 6 corpus fetchers — drop-in replacement
    for ``sentinel.enrichment.defaults.default_fetchers()`` in eval mode."""
    return (
        CorpusDeploysFetcher(registry),
        CorpusRelatedAlertsFetcher(registry),
        CorpusSimilarIncidentsFetcher(registry),
        CorpusRunbooksFetcher(registry),
        CorpusRecentLogsFetcher(registry),
        CorpusActiveAlertsFetcher(registry),
    )
