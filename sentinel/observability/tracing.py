# sentinel/observability/tracing.py
"""OTel SDK init + span helpers.

When `settings.otel_endpoint` is unset we still call `set_tracer_provider`
with a default provider so that the helper context managers work as no-ops
(they emit unsampled spans rather than raising).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span

from sentinel.config.settings import Settings

_TRACER_NAME = "sentinel"


def configure_tracing(settings: Settings) -> None:
    resource = Resource.create({"service.name": "sentinel"})
    provider = TracerProvider(resource=resource)
    if settings.otel_endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_endpoint))
        )
    trace.set_tracer_provider(provider)


def _tracer() -> trace.Tracer:
    return trace.get_tracer(_TRACER_NAME)


@contextmanager
def span_for_fetcher(name: str) -> Iterator[Span]:
    with _tracer().start_as_current_span(f"fetcher.{name}") as span:
        span.set_attribute("sentinel.fetcher", name)
        yield span


@contextmanager
def span_for_llm(model: str) -> Iterator[Span]:
    with _tracer().start_as_current_span(f"llm.{model}") as span:
        span.set_attribute("sentinel.llm.model", model)
        yield span
