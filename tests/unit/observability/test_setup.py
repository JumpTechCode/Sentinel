# tests/unit/observability/test_setup.py
"""`configure_observability` is the single process-level init entry point.

It must wire BOTH structured logging and tracing — the gh#64 regression was that
the lifespan called only `configure_tracing`, so structlog never activated and
runtime logging fell back to bare stdlib.
"""

from __future__ import annotations

import pytest
from sentinel.config.settings import Settings


def _settings() -> Settings:
    return Settings(
        postgres_dsn="postgresql+asyncpg://u:p@localhost/d",
        redis_url="redis://localhost:6379/0",
        kafka_brokers="localhost:9092",
        anthropic_api_key="x",
    )


def test_configure_observability_wires_logging_and_tracing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from sentinel.observability import configure_observability

    calls: list[str] = []
    monkeypatch.setattr(
        "sentinel.observability.configure_logging", lambda s: calls.append("logging")
    )
    monkeypatch.setattr(
        "sentinel.observability.configure_tracing", lambda s: calls.append("tracing")
    )

    configure_observability(_settings())

    assert calls == ["logging", "tracing"]


def test_configure_observability_runs_for_real_without_error() -> None:
    """The real wiring must execute end-to-end (no endpoint configured)."""
    from sentinel.observability import configure_observability

    # Should not raise; logging is idempotent and tracing no-ops without an endpoint.
    configure_observability(_settings())
