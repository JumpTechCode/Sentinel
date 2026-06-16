# tests/unit/enrichment/test_orchestrator.py
"""assemble() coerces exceptions, enforces timeouts, runs fetchers in parallel."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from sentinel.enrichment.circuit_breaker import CircuitBreaker
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.orchestrator import assemble
from sentinel.schemas.context import FetcherResult, IncidentContext


@dataclass(frozen=True)
class _Incident:
    id: UUID
    service: str
    external_id: str
    title: str
    severity: str


def _ok_result() -> FetcherResult[Any]:
    return FetcherResult(status="ok", data=[], fetched_at=datetime.now(UTC))


class _OkFetcher:
    timeout_s = 5.0

    def __init__(self, name: str) -> None:
        self.name = name

    async def fetch(self, incident: Any, deps: EnrichmentDeps) -> FetcherResult[Any]:
        return _ok_result()


def _make_deps(fetchers: list[Any]) -> EnrichmentDeps:
    return EnrichmentDeps(
        fetchers=tuple(fetchers),
        breakers={f.name: CircuitBreaker(f.name) for f in fetchers},
        incident_repo=None,  # type: ignore[arg-type]
        deploy_repo=None,  # type: ignore[arg-type]
        similar_incidents=None,  # type: ignore[arg-type]
        runbooks=None,  # type: ignore[arg-type]
        log_search=None,  # type: ignore[arg-type]
        active_alerts=None,  # type: ignore[arg-type]
    )


def _make_incident() -> _Incident:
    return _Incident(
        id=uuid4(),
        service="api",
        external_id="ext-1",
        title="boom",
        severity="SEV2",
    )


def _six_ok() -> list[Any]:
    return [
        _OkFetcher("deploys"),
        _OkFetcher("related_alerts"),
        _OkFetcher("similar_incidents"),
        _OkFetcher("runbooks"),
        _OkFetcher("recent_logs"),
        _OkFetcher("active_alerts"),
    ]


@pytest.mark.asyncio
async def test_assemble_returns_incident_context() -> None:
    incident = _make_incident()
    fetchers = _six_ok()
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert isinstance(ctx, IncidentContext)
    assert ctx.incident_id == incident.id
    assert ctx.recent_deploys.status == "ok"


@pytest.mark.asyncio
async def test_assemble_emits_a_span_per_fetcher(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each fetcher run is wrapped in a tracing span.

    gh#64: `span_for_fetcher` existed but had zero call sites, so enrichment
    emitted no spans. Inject an in-memory exporter (the SDK forbids overriding a
    set provider, so patch the tracer helper) and assert one span per fetcher.
    """
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(
        "sentinel.observability.tracing._tracer",
        lambda: provider.get_tracer("sentinel"),
    )

    deps = _make_deps(_six_ok())
    await assemble(_make_incident(), deps)

    names = {s.name for s in exporter.get_finished_spans()}
    assert {f"fetcher.{n}" for n in ("deploys", "related_alerts", "runbooks")} <= names


@pytest.mark.asyncio
async def test_raising_fetcher_becomes_failed_result() -> None:
    incident = _make_incident()

    class _Raises:
        name = "deploys"
        timeout_s = 5.0

        async def fetch(self, incident: Any, deps: EnrichmentDeps) -> FetcherResult[Any]:
            raise RuntimeError("kaboom")

    fetchers: list[Any] = [_Raises()] + _six_ok()[1:]
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert ctx.recent_deploys.status == "failed"
    assert "RuntimeError" in (ctx.recent_deploys.error or "")
    # Other sections remain ok.
    assert ctx.related_alerts.status == "ok"


@pytest.mark.asyncio
async def test_slow_fetcher_times_out_others_complete() -> None:
    incident = _make_incident()

    class _Slow:
        name = "deploys"
        timeout_s = 0.05

        async def fetch(self, incident: Any, deps: EnrichmentDeps) -> FetcherResult[Any]:
            await asyncio.sleep(0.5)
            return _ok_result()

    fetchers: list[Any] = [_Slow()] + _six_ok()[1:]
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert ctx.recent_deploys.status == "failed"
    assert "timeout" in (ctx.recent_deploys.error or "").lower()
    assert ctx.related_alerts.status == "ok"


@pytest.mark.asyncio
async def test_non_fetcherresult_return_is_coerced_to_failed() -> None:
    incident = _make_incident()

    class _Bad:
        name = "deploys"
        timeout_s = 5.0

        async def fetch(self, incident: Any, deps: EnrichmentDeps) -> FetcherResult[Any]:
            return "not a FetcherResult"  # type: ignore[return-value]

    fetchers: list[Any] = [_Bad()] + _six_ok()[1:]
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert ctx.recent_deploys.status == "failed"
