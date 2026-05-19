# sentinel/enrichment/circuit_breaker.py
"""Per-fetcher circuit breaker.

State machine: closed → open (after `threshold` failures within `window_s`)
→ half_open (after `cooldown_s` from opening) → closed (on a successful
trial) or back to open (on a failed trial).

Failures use a rolling time-deque, not a counter — counter-only fails the
"rolling" requirement; see program-of-work F-risks.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Literal, TypeVar

T = TypeVar("T")

State = Literal["closed", "open", "half_open"]


class CircuitOpenError(Exception):
    """Raised when the breaker short-circuits a call."""


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        *,
        threshold: int = 5,
        window_s: float = 60.0,
        cooldown_s: float = 30.0,
        time_fn: Callable[[], float] = time.monotonic,
        on_state_change: Callable[[str, str], None] | None = None,
    ) -> None:
        self._name = name
        self._threshold = threshold
        self._window_s = window_s
        self._cooldown_s = cooldown_s
        self._time = time_fn
        self._on_state_change = on_state_change
        self._failures: deque[float] = deque()
        self._state: State = "closed"
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> State:
        return self._state

    def _transition(self, new: State) -> None:
        old = self._state
        if old == new:
            return
        self._state = new
        if new == "open":
            self._opened_at = self._time()
        elif new == "closed":
            self._failures.clear()
            self._opened_at = None
        if self._on_state_change is not None:
            self._on_state_change(old, new)

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._failures and self._failures[0] <= cutoff:
            self._failures.popleft()

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            now = self._time()
            if self._state == "open":
                opened_at = self._opened_at
                if opened_at is None:  # pragma: no cover - invariant
                    raise CircuitOpenError(f"breaker {self._name} open")
                if now - opened_at >= self._cooldown_s:
                    self._transition("half_open")
                else:
                    raise CircuitOpenError(f"breaker {self._name} open")
        try:
            result = await fn()
        except asyncio.CancelledError:
            raise
        except BaseException:
            async with self._lock:
                now2 = self._time()
                if self._state == "half_open":
                    self._failures.append(now2)
                    self._transition("open")
                else:
                    self._failures.append(now2)
                    self._prune(now2)
                    if len(self._failures) >= self._threshold:
                        self._transition("open")
            raise
        else:
            async with self._lock:
                if self._state == "half_open":
                    self._transition("closed")
            return result
