# sentinel/observability/__init__.py
"""Observability primitives — logging, metrics, tracing, cost.

Skeleton lands here; downstream work areas import these and call them as code
that needs instrumentation lands.
"""

from sentinel.config.settings import Settings
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


def configure_observability(settings: Settings) -> None:
    """Process-level observability init — structured logging + tracing.

    The single entry point the app lifespan calls, so the "observability is
    wired" contract has one place to set up and one to test. gh#64: the lifespan
    previously called only ``configure_tracing``, leaving structlog inert and
    runtime logging on bare stdlib.
    """
    configure_logging(settings)
    configure_tracing(settings)


__all__ = [
    "bind_request_context",
    "clear_request_context",
    "configure_logging",
    "configure_observability",
    "configure_tracing",
    "get_llm_audit_logger",
    "span_for_fetcher",
    "span_for_llm",
    "usd_cost",
]
