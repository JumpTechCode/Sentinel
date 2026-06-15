# tests/chaos/test_breaker_chaos.py
"""Chaos B2 — enrichment circuit breaker under sustained fault injection.

The breaker state machine is unit-tested in isolation
(``tests/unit/enrichment/test_circuit_breaker.py``). This suite proves the
*integration*: a breaker wired into ``assemble()`` via ``EnrichmentDeps.breakers``
trips open under sustained fetcher failure, short-circuits subsequent calls,
keeps the rest of the context healthy (graceful degradation — ``assemble`` never
raises), and recovers after cooldown. It also confirms the production gauge
wiring (``sentinel_circuit_breaker_state``) that ``make_breaker`` installs.

No Docker, no Anthropic: enrichment fetchers are in-process stubs, so the
faithful chaos injection is a fault-fetcher, not toxiproxy (see ADR 0010).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest
from prometheus_client import REGISTRY
from sentinel.enrichment.circuit_breaker import CircuitBreaker
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.metrics_wiring import make_breaker
from sentinel.enrichment.orchestrator import assemble
from sentinel.schemas.context import FetcherResult, IncidentContext

pytestmark = pytest.mark.chaos


class _Clock:
    """Controllable monotonic clock — mirrors test_circuit_breaker.py."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


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


class _FaultFetcher:
    """Fetcher whose ``fetch`` raises until flipped healthy; counts invocations.

    The call counter is the short-circuit probe: once the breaker is open,
    ``assemble`` must not invoke ``fetch`` at all.
    """

    timeout_s = 5.0

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0
        self.healthy = False

    async def fetch(self, incident: Any, deps: EnrichmentDeps) -> FetcherResult[Any]:
        self.calls += 1
        if not self.healthy:
            raise RuntimeError("injected fault")
        return _ok_result()


def _make_incident() -> _Incident:
    return _Incident(
        id=uuid4(),
        service="api",
        external_id="ext-1",
        title="boom",
        severity="SEV2",
    )


def _make_deps(fetchers: list[Any], breakers: dict[str, CircuitBreaker]) -> EnrichmentDeps:
    # Repo/adapter deps are None on purpose: the stub fetchers in this suite never
    # read them. If a fetcher here ever consults `deps`, give it a real stub instead.
    return EnrichmentDeps(
        fetchers=tuple(fetchers),
        breakers=breakers,
        incident_repo=None,  # type: ignore[arg-type]
        deploy_repo=None,  # type: ignore[arg-type]
        similar_incidents=None,  # type: ignore[arg-type]
        runbooks=None,  # type: ignore[arg-type]
        log_search=None,  # type: ignore[arg-type]
        active_alerts=None,  # type: ignore[arg-type]
    )


def _five_ok() -> list[Any]:
    # All sections except "deploys" — that one is the fault-injection target.
    return [
        _OkFetcher("related_alerts"),
        _OkFetcher("similar_incidents"),
        _OkFetcher("runbooks"),
        _OkFetcher("recent_logs"),
        _OkFetcher("active_alerts"),
    ]


def _wire(fault: _FaultFetcher, breaker: CircuitBreaker) -> tuple[_Incident, EnrichmentDeps]:
    fetchers: list[Any] = [fault, *_five_ok()]
    breakers = {f.name: CircuitBreaker(f.name) for f in fetchers}
    breakers[fault.name] = breaker
    return _make_incident(), _make_deps(fetchers, breakers)


def _circuit_open_failures(fetcher: str) -> float:
    """Current sentinel_enrichment_failures_total{reason="circuit_open"} for a fetcher."""
    return (
        REGISTRY.get_sample_value(
            "sentinel_enrichment_failures_total",
            {"fetcher": fetcher, "reason": "circuit_open"},
        )
        or 0.0
    )


@pytest.mark.asyncio
async def test_sustained_fault_opens_breaker_and_degrades_gracefully() -> None:
    """`threshold` consecutive failures open the breaker; assemble never raises."""
    clock = _Clock()
    transitions: list[tuple[str, str]] = []
    breaker = CircuitBreaker(
        "deploys",
        threshold=3,
        window_s=60.0,
        cooldown_s=30.0,
        time_fn=clock,
        on_state_change=lambda old, new: transitions.append((old, new)),
    )
    fault = _FaultFetcher("deploys")
    incident, deps = _wire(fault, breaker)

    for _ in range(3):
        ctx = await assemble(incident, deps)
        # Faulted section degrades; the rest of the context stays healthy.
        assert isinstance(ctx, IncidentContext)
        assert ctx.recent_deploys.status == "failed"
        assert ctx.related_alerts.status == "ok"
        assert ctx.active_alerts.status == "ok"
        clock.advance(1.0)

    assert fault.calls == 3  # fetched (and failed) on every pre-open call
    assert breaker.state == "open"
    assert ("closed", "open") in transitions


@pytest.mark.asyncio
async def test_open_breaker_short_circuits_fetcher() -> None:
    """Once open, assemble short-circuits — the fetcher is never invoked again."""
    clock = _Clock()
    breaker = CircuitBreaker("deploys", threshold=3, window_s=60.0, cooldown_s=30.0, time_fn=clock)
    fault = _FaultFetcher("deploys")
    incident, deps = _wire(fault, breaker)

    for _ in range(3):
        await assemble(incident, deps)
    assert breaker.state == "open"
    calls_at_open = fault.calls
    open_failures_before = _circuit_open_failures("deploys")

    # Clock not advanced past cooldown → breaker stays open.
    ctx = await assemble(incident, deps)
    assert fault.calls == calls_at_open  # short-circuited: fetch() not called
    assert ctx.recent_deploys.status == "failed"
    assert "circuit_open" in (ctx.recent_deploys.error or "")
    assert ctx.related_alerts.status == "ok"  # degradation is isolated
    # The short-circuit emits enrichment_failures_total{reason="circuit_open"} (delta-based
    # to tolerate cross-test accumulation on the global registry).
    assert _circuit_open_failures("deploys") >= open_failures_before + 1


@pytest.mark.asyncio
async def test_breaker_recovers_to_closed_after_cooldown() -> None:
    """After cooldown, a healthy fetch closes the breaker and restores the section."""
    clock = _Clock()
    transitions: list[tuple[str, str]] = []
    breaker = CircuitBreaker(
        "deploys",
        threshold=3,
        window_s=60.0,
        cooldown_s=30.0,
        time_fn=clock,
        on_state_change=lambda old, new: transitions.append((old, new)),
    )
    fault = _FaultFetcher("deploys")
    incident, deps = _wire(fault, breaker)

    for _ in range(3):
        await assemble(incident, deps)
    # Read into a str local so this assert doesn't narrow the `state` property and
    # poison the post-recovery check below (mypy treats argless-property reads as
    # the same narrowed expression).
    opened_state: str = breaker.state
    assert opened_state == "open"

    # Dependency recovers; advance past cooldown so the next call is a half-open trial.
    fault.healthy = True
    clock.advance(31.0)
    ctx = await assemble(incident, deps)

    assert breaker.state == "closed"
    assert ctx.recent_deploys.status == "ok"
    assert transitions[-2] == ("open", "half_open")
    assert transitions[-1] == ("half_open", "closed")


@pytest.mark.asyncio
async def test_make_breaker_drives_state_gauge() -> None:
    """The production breaker factory moves sentinel_circuit_breaker_state on open.

    Uses a unique integration label so the global gauge read is deterministic
    regardless of test ordering.
    """
    name = "chaos-gauge-probe"
    breaker = make_breaker(name)

    def _gauge() -> float | None:
        return REGISTRY.get_sample_value("sentinel_circuit_breaker_state", {"integration": name})

    assert _gauge() == 0.0  # make_breaker initializes to closed

    async def boom() -> None:
        raise RuntimeError("boom")

    for _ in range(5):  # default threshold
        with pytest.raises(RuntimeError):
            await breaker.call(boom)

    assert breaker.state == "open"
    assert _gauge() == 1.0  # 0=closed, 1=open, 2=half_open
