# sentinel/enrichment/metrics_wiring.py
"""Build a CircuitBreaker that updates the Prometheus state gauge.

Keeping this wiring out of CircuitBreaker itself preserves testability:
unit tests can construct breakers without touching prometheus globals.
"""

from __future__ import annotations

from sentinel.enrichment.circuit_breaker import CircuitBreaker
from sentinel.observability.metrics import circuit_breaker_state

_STATE_TO_INT = {"closed": 0, "open": 1, "half_open": 2}


def make_breaker(name: str) -> CircuitBreaker:
    def _on_change(old: str, new: str) -> None:
        circuit_breaker_state.labels(integration=name).set(_STATE_TO_INT[new])

    cb = CircuitBreaker(name, on_state_change=_on_change)
    # Initialize the gauge to "closed" so dashboards don't show a missing series.
    circuit_breaker_state.labels(integration=name).set(_STATE_TO_INT["closed"])
    return cb
