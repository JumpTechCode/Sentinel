# sentinel/observability/__init__.py
"""Observability primitives — logging, metrics, tracing, cost.

Skeleton lands here; downstream work areas import these and call them as code
that needs instrumentation lands.
"""

from sentinel.observability.cost import usd_cost
from sentinel.observability.logging import (
    bind_request_context,
    clear_request_context,
    configure_logging,
    get_llm_audit_logger,
)
from sentinel.observability.tracing import (
    configure_tracing,
    span_for_fetcher,
    span_for_llm,
)

__all__ = [
    "bind_request_context",
    "clear_request_context",
    "configure_logging",
    "configure_tracing",
    "get_llm_audit_logger",
    "span_for_fetcher",
    "span_for_llm",
    "usd_cost",
]
