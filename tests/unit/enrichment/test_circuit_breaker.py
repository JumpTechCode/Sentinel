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
