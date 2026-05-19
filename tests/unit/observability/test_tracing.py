# tests/unit/observability/test_tracing.py
"""Tracing helpers are safe when no endpoint is configured."""

from __future__ import annotations

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sentinel.config.settings import Settings
from sentinel.observability.tracing import (
    configure_tracing,
    span_for_fetcher,
    span_for_llm,
)


def _settings(otel_endpoint: str | None = None) -> Settings:
    return Settings(
        postgres_dsn="postgresql+asyncpg://u:p@localhost/d",
        redis_url="redis://localhost:6379/0",
        kafka_brokers="localhost:9092",
        anthropic_api_key="x",
        otel_endpoint=otel_endpoint,
    )


def test_no_endpoint_is_noop() -> None:
    configure_tracing(_settings(otel_endpoint=None))
    with span_for_fetcher("deploys"):
        pass
    with span_for_llm("claude-sonnet-4-5"):
        pass


def test_with_inmemory_exporter_records_spans(monkeypatch: pytest.MonkeyPatch) -> None:
    # Wire an in-memory exporter by patching the _tracer() helper directly.
    # OTel SDK prevents overriding a provider once set, so we bypass set_tracer_provider
    # and inject a fresh TracerProvider with our exporter via monkeypatch.
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    monkeypatch.setattr(
        "sentinel.observability.tracing._tracer",
        lambda: provider.get_tracer(_TRACER_NAME := "sentinel"),
    )

    with span_for_fetcher("logs"):
        pass
    with span_for_llm("claude-sonnet-4-5"):
        pass

    spans = exporter.get_finished_spans()
    by_name = {s.name: s for s in spans}
    assert "fetcher.logs" in by_name
    assert "llm.claude-sonnet-4-5" in by_name
    assert by_name["fetcher.logs"].attributes is not None
    assert by_name["fetcher.logs"].attributes["sentinel.fetcher"] == "logs"
    assert by_name["llm.claude-sonnet-4-5"].attributes is not None
    assert by_name["llm.claude-sonnet-4-5"].attributes["sentinel.llm.model"] == "claude-sonnet-4-5"
