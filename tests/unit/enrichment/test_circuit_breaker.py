# tests/unit/enrichment/test_circuit_breaker.py
"""CircuitBreaker state machine and rolling-window semantics."""

from __future__ import annotations

import asyncio

import pytest
from sentinel.enrichment.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
)


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.mark.asyncio
async def test_starts_closed() -> None:
    cb = CircuitBreaker("x", time_fn=_Clock())
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_opens_after_five_failures_in_window() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=5, window_s=60.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    for _ in range(5):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
        clock.advance(1.0)
    assert cb.state == "open"


@pytest.mark.asyncio
async def test_does_not_open_when_failures_age_out() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=5, window_s=60.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    for _ in range(4):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
        clock.advance(20.0)  # spread 4 failures across 60s
    # 5th failure at t=80; first failure at t=0 is now > window_s old → pruned.
    with pytest.raises(RuntimeError):
        await cb.call(boom)
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_open_state_short_circuits() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, cooldown_s=30.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    assert cb.state == "open"
    with pytest.raises(CircuitOpenError):
        await cb.call(boom)


@pytest.mark.asyncio
async def test_half_open_after_cooldown_then_close_on_success() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, cooldown_s=30.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    async def ok() -> int:
        return 1

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    assert cb.state == "open"
    clock.advance(31.0)
    assert await cb.call(ok) == 1
    assert cb.state == "closed"  # type: ignore[comparison-overlap]


@pytest.mark.asyncio
async def test_half_open_failure_reopens() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, cooldown_s=30.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    clock.advance(31.0)
    with pytest.raises(RuntimeError):
        await cb.call(boom)
    assert cb.state == "open"


@pytest.mark.asyncio
async def test_cancellation_does_not_count_as_failure() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, time_fn=clock)

    async def cancelled() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await cb.call(cancelled)
    with pytest.raises(asyncio.CancelledError):
        await cb.call(cancelled)
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_half_open_single_flights_concurrent_probes() -> None:
    """In half_open, exactly ONE caller probes the unhealthy dependency; callers
    arriving while the probe is in flight short-circuit with CircuitOpenError
    instead of all hammering the dependency (issue #65)."""
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=1, window_s=60.0, cooldown_s=30.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await cb.call(boom)  # threshold=1 → opens immediately
    assert cb.state == "open"
    clock.advance(31.0)  # cooldown elapsed → next caller may probe

    probe_in_flight = asyncio.Event()
    release = asyncio.Event()
    probe_calls = 0

    async def slow_probe() -> str:
        nonlocal probe_calls
        probe_calls += 1
        probe_in_flight.set()
        await release.wait()
        return "ok"

    async def must_not_run() -> str:  # pragma: no cover - asserts it never runs
        raise AssertionError("concurrent caller should have been short-circuited")

    probe_task = asyncio.create_task(cb.call(slow_probe))
    await asyncio.wait_for(probe_in_flight.wait(), timeout=1.0)

    # While the single probe is in flight, concurrent callers must be rejected.
    results = await asyncio.gather(
        cb.call(must_not_run), cb.call(must_not_run), return_exceptions=True
    )
    assert all(isinstance(r, CircuitOpenError) for r in results), results

    release.set()
    assert await probe_task == "ok"
    assert probe_calls == 1
    assert cb.state == "closed"  # type: ignore[comparison-overlap]


@pytest.mark.asyncio
async def test_half_open_probe_reopens_then_allows_next_probe_after_cooldown() -> None:
    """Guards the *failure-path* flag release: a failed half-open probe re-opens
    the breaker AND clears the in-flight flag, so a fresh probe is admitted after
    the next cooldown. (Drop the flag-clear from the failure branch and the final
    call short-circuits with CircuitOpenError instead of probing.)"""
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=1, window_s=60.0, cooldown_s=30.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    async def ok() -> int:
        return 1

    with pytest.raises(RuntimeError):
        await cb.call(boom)
    clock.advance(31.0)
    # Single-flight probe fails → reopen (and in-flight flag must be released).
    with pytest.raises(RuntimeError):
        await cb.call(boom)
    assert cb.state == "open"
    # Next cooldown → a fresh probe is allowed (proves the flag was cleared).
    clock.advance(31.0)
    assert await cb.call(ok) == 1
    assert cb.state == "closed"  # type: ignore[comparison-overlap]


@pytest.mark.asyncio
async def test_half_open_cancelled_probe_releases_slot() -> None:
    """Guards the cancellation-path flag release: if the single half-open probe
    is cancelled (e.g. the caller's task is torn down), the in-flight slot must be
    freed so a later caller can probe — never wedged half-open forever (issue #65).
    """
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=1, window_s=60.0, cooldown_s=30.0, time_fn=clock)

    async def boom() -> None:
        raise RuntimeError("boom")

    async def ok() -> int:
        return 1

    with pytest.raises(RuntimeError):
        await cb.call(boom)
    clock.advance(31.0)

    probe_in_flight = asyncio.Event()

    async def blocking_probe() -> None:
        probe_in_flight.set()
        await asyncio.Event().wait()  # block until cancelled

    probe_task = asyncio.create_task(cb.call(blocking_probe))
    await asyncio.wait_for(probe_in_flight.wait(), timeout=1.0)
    probe_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe_task

    # State is still half_open (cancellation isn't a failure); only the flag gates
    # the next caller. If it leaked True this would raise CircuitOpenError.
    assert cb.state == "half_open"
    assert await cb.call(ok) == 1
    assert cb.state == "closed"  # type: ignore[comparison-overlap]


@pytest.mark.asyncio
async def test_state_change_callback_fires_on_transitions() -> None:
    clock = _Clock()
    events: list[tuple[str, str]] = []
    cb = CircuitBreaker(
        "x",
        threshold=2,
        window_s=60.0,
        cooldown_s=30.0,
        time_fn=clock,
        on_state_change=lambda old, new: events.append((old, new)),
    )

    async def boom() -> None:
        raise RuntimeError("boom")

    async def ok() -> int:
        return 1

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    assert events[-1] == ("closed", "open")
    clock.advance(31.0)
    await cb.call(ok)
    assert events[-2] == ("open", "half_open")
    assert events[-1] == ("half_open", "closed")
