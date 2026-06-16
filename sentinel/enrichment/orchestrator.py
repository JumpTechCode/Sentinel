# sentinel/enrichment/orchestrator.py
"""Parallel fetcher orchestrator.

Fetchers run concurrently via asyncio.gather(return_exceptions=True). Each
is wrapped in a per-fetcher CircuitBreaker and an asyncio.wait_for timeout.
Fetcher misbehavior (raising, returning a non-FetcherResult) is coerced to
a FetcherResult(status="failed", ...) — assemble() never raises.

The IncidentContext schema expects six named section fields; fetchers are
mapped onto sections by their .name attribute, not by list position.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any, Protocol

from sentinel.enrichment.circuit_breaker import CircuitOpenError
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.observability.metrics import (
    enrichment_assemble_duration_seconds,
    enrichment_duration_seconds,
    enrichment_failures_total,
    enrichment_section_status_total,
)
from sentinel.observability.tracing import span_for_fetcher
from sentinel.schemas.context import FetcherResult, IncidentContext

log = logging.getLogger(__name__)

_SECTION_NAMES = (
    "deploys",
    "related_alerts",
    "similar_incidents",
    "runbooks",
    "recent_logs",
    "active_alerts",
)

_SECTION_TO_FIELD = {
    "deploys": "recent_deploys",
    "related_alerts": "related_alerts",
    "similar_incidents": "similar_incidents",
    "runbooks": "runbooks",
    "recent_logs": "recent_logs",
    "active_alerts": "active_alerts",
}


class Fetcher(Protocol):
    name: str
    timeout_s: float

    async def fetch(self, incident: Any, deps: EnrichmentDeps) -> FetcherResult[Any]: ...


def _failed_result(error: str) -> FetcherResult[Any]:
    return FetcherResult(
        status="failed",
        data=[],
        error=error,
        fetched_at=datetime.now(UTC),
    )


async def _run(
    fetcher: Fetcher,
    incident: Any,
    deps: EnrichmentDeps,
) -> FetcherResult[Any]:
    """Run one fetcher wrapped in a per-fetcher tracing span.

    The fetch/timeout/breaker/coerce logic lives in `_run_guarded` (which never
    raises); this wrapper only opens the span so enrichment is actually traced.
    gh#64: `span_for_fetcher` previously had zero call sites.
    """
    with span_for_fetcher(fetcher.name):
        return await _run_guarded(fetcher, incident, deps)


async def _run_guarded(
    fetcher: Fetcher,
    incident: Any,
    deps: EnrichmentDeps,
) -> FetcherResult[Any]:
    started = time.monotonic()
    breaker = deps.breakers.get(fetcher.name)
    result: Any
    try:
        if breaker is None:
            result = await asyncio.wait_for(
                fetcher.fetch(incident, deps),
                timeout=fetcher.timeout_s,
            )
        else:
            result = await breaker.call(
                lambda: asyncio.wait_for(
                    fetcher.fetch(incident, deps),
                    timeout=fetcher.timeout_s,
                )
            )
    except TimeoutError:
        log.warning(
            "fetcher_timed_out",
            extra={"fetcher": fetcher.name, "incident_id": str(incident.id)},
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name,
            reason="timeout",
        ).inc()
        enrichment_section_status_total.labels(
            fetcher=fetcher.name,
            status="failed",
        ).inc()
        return _failed_result("timeout")
    except CircuitOpenError:
        log.debug(
            "breaker_short_circuit",
            extra={"fetcher": fetcher.name},
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name,
            reason="circuit_open",
        ).inc()
        enrichment_section_status_total.labels(
            fetcher=fetcher.name,
            status="failed",
        ).inc()
        return _failed_result("circuit_open")
    except Exception as e:
        log.warning(
            "fetcher_errored",
            extra={
                "fetcher": fetcher.name,
                "incident_id": str(incident.id),
                "exc_type": type(e).__name__,
            },
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name,
            reason="error",
        ).inc()
        enrichment_section_status_total.labels(
            fetcher=fetcher.name,
            status="failed",
        ).inc()
        return _failed_result(f"{type(e).__name__}: {e}")
    finally:
        enrichment_duration_seconds.labels(fetcher=fetcher.name).observe(
            time.monotonic() - started,
        )

    if not isinstance(result, FetcherResult):
        log.warning(
            "fetcher_returned_non_result",
            extra={"fetcher": fetcher.name, "type": type(result).__name__},
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name,
            reason="bad_return",
        ).inc()
        enrichment_section_status_total.labels(
            fetcher=fetcher.name,
            status="failed",
        ).inc()
        return _failed_result("fetcher returned non-FetcherResult")

    enrichment_section_status_total.labels(
        fetcher=fetcher.name,
        status=result.status,
    ).inc()
    return result


async def assemble(incident: Any, deps: EnrichmentDeps) -> IncidentContext:
    started = time.monotonic()
    coros = [_run(f, incident, deps) for f in deps.fetchers]
    raw = await asyncio.gather(*coros, return_exceptions=True)

    by_name: dict[str, FetcherResult[Any]] = {}
    for fetcher, value in zip(deps.fetchers, raw, strict=True):
        if isinstance(value, FetcherResult):
            by_name[fetcher.name] = value
        else:
            # asyncio.gather(return_exceptions=True) yields either the coro's
            # return value (a FetcherResult — _run never raises) or a
            # BaseException. _run is defensive enough that this branch is a
            # backstop for surprises (e.g. CancelledError).
            log.warning(
                "fetcher_orchestrator_exception",
                extra={
                    "fetcher": fetcher.name,
                    "exc_type": type(value).__name__,
                },
            )
            by_name[fetcher.name] = _failed_result(
                f"{type(value).__name__}: {value}",
            )

    placeholder: FetcherResult[Any] = FetcherResult(
        status="degraded",
        data=[],
        error="not_configured",
        fetched_at=datetime.now(UTC),
    )
    section_results = {_SECTION_TO_FIELD[n]: by_name.get(n, placeholder) for n in _SECTION_NAMES}
    ctx = IncidentContext(
        incident_id=incident.id,
        assembled_at=datetime.now(UTC),
        **section_results,
    )
    enrichment_assemble_duration_seconds.observe(time.monotonic() - started)
    return ctx
