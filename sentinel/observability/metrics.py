# sentinel/observability/metrics.py
"""Prometheus metrics named per the spec. Singletons at module level.

The `_safe_*` helpers guard against double-registration when this module is
reimported (e.g., by a test reload). On collision, we return the existing
collector from the registry.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from time import perf_counter

from prometheus_client import REGISTRY, Counter, Gauge, Histogram


def _safe_counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    try:
        return Counter(name, documentation, labelnames=labelnames)
    except ValueError:
        # Already registered — return the existing collector on reload/reimport.
        # prometheus_client stores Counters under the base name (sans "_total").
        base = name[: -len("_total")] if name.endswith("_total") else name
        return REGISTRY._names_to_collectors[base]  # type: ignore[return-value]


def _safe_gauge(name: str, documentation: str, labelnames: list[str] | None = None) -> Gauge:
    try:
        return Gauge(name, documentation, labelnames=labelnames or [])
    except ValueError:
        # Already registered — return the existing collector on reload/reimport.
        return REGISTRY._names_to_collectors[name]  # type: ignore[return-value]


def _safe_histogram(
    name: str,
    documentation: str,
    labelnames: list[str],
    buckets: tuple[float, ...] | None = None,
) -> Histogram:
    try:
        if buckets is not None:
            return Histogram(name, documentation, labelnames=labelnames, buckets=buckets)
        return Histogram(name, documentation, labelnames=labelnames)
    except ValueError:
        # Already registered — return the existing collector on reload/reimport.
        # prometheus_client stores Histograms under the base name (no suffix).
        return REGISTRY._names_to_collectors[name]  # type: ignore[return-value]


webhooks_total = _safe_counter("sentinel_webhooks_total", "Incoming webhooks", ["source", "status"])
incidents_opened_total = _safe_counter(
    "sentinel_incidents_opened_total",
    "Incidents opened",
    ["service", "severity"],
)
enrichment_duration_seconds = _safe_histogram(
    "sentinel_enrichment_duration_seconds",
    "Time spent in each enrichment fetcher",
    ["fetcher"],
)
enrichment_failures_total = _safe_counter(
    "sentinel_enrichment_failures_total",
    "Enrichment fetcher failures",
    ["fetcher", "reason"],
)
circuit_breaker_state = _safe_gauge(
    "sentinel_circuit_breaker_state",
    "Breaker state (0=closed,1=open,2=half_open)",
    ["integration"],
)
diagnosis_latency_seconds = _safe_histogram(
    "sentinel_diagnosis_latency_seconds",
    "End-to-end diagnosis latency",
    [],
    buckets=(1, 2, 5, 10, 15, 30),
)
diagnosis_confidence = _safe_histogram(
    "sentinel_diagnosis_confidence",
    "Confidence values reported by the model",
    [],
    buckets=(0.1, 0.3, 0.5, 0.7, 0.85, 1.0),
)
llm_tokens_total = _safe_counter(
    "sentinel_llm_tokens_total",
    "LLM token usage",
    ["model", "kind"],
)
llm_cost_usd_total = _safe_counter(
    "sentinel_llm_cost_usd_total",
    "LLM cost (USD), cumulative",
    ["model"],
)
hallucinated_evidence_total = _safe_counter(
    "sentinel_hallucinated_evidence_total",
    "Diagnoses with at least one invented citation",
    [],
)
diagnoses_total = _safe_counter(
    "sentinel_diagnoses_total",
    "Diagnoses persisted, by outcome status",
    ["status"],
)
diagnosis_failures_total = _safe_counter(
    "sentinel_diagnosis_failures_total",
    "Diagnosis attempts that failed before persistence",
    ["reason"],
)
diagnosis_input_truncated_total = _safe_counter(
    "sentinel_diagnosis_input_truncated_total",
    "Per-section count of context items dropped to fit input budget",
    ["section"],
)
diagnosis_llm_tokens_total = _safe_counter(
    "sentinel_diagnosis_llm_tokens_total",
    "Tokens consumed by the diagnosis LLM call",
    ["kind"],
)
diagnosis_invalid_events_total = _safe_counter(
    "sentinel_diagnosis_invalid_events_total",
    "Diagnosis Kafka envelopes that failed JSON/schema parse",
    [],
)
diagnosis_correctness_rate_30d = _safe_gauge(
    "sentinel_diagnosis_correctness_rate_30d",
    "Fraction of diagnoses marked correct on resolution, 30d rolling",
)
webhook_handler_duration_seconds = _safe_histogram(
    "sentinel_webhook_handler_duration_seconds",
    "Webhook handler wall-clock duration.",
    ["source"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)
outbox_events_enqueued_total = _safe_counter(
    "sentinel_outbox_events_enqueued_total",
    "Outbox events written by topic.",
    ["topic"],
)
outbox_events_published_total = _safe_counter(
    "sentinel_outbox_events_published_total",
    "Outbox events successfully published to Kafka.",
    ["topic"],
)
outbox_events_failed_total = _safe_counter(
    "sentinel_outbox_events_failed_total",
    "Outbox events that failed to publish (per attempt).",
    ["topic"],
)
outbox_publish_latency_seconds = _safe_histogram(
    "sentinel_outbox_publish_latency_seconds",
    "Outbox row age (created_at -> published_at) in seconds.",
    ["topic"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)
outbox_unpublished_count = _safe_gauge(
    "sentinel_outbox_unpublished_count",
    "Count of outbox rows with published_at IS NULL.",
)
outbox_oldest_unpublished_age_seconds = _safe_gauge(
    "sentinel_outbox_oldest_unpublished_age_seconds",
    "Age in seconds of the oldest unpublished outbox row.",
)
outbox_event_stuck_total = _safe_counter(
    "sentinel_outbox_event_stuck_total",
    "Outbox rows skipped because attempts >= max_attempts.",
    [],
)
enrichment_assemble_duration_seconds = _safe_histogram(
    "sentinel_enrichment_assemble_duration_seconds",
    "End-to-end assemble() duration in seconds.",
    [],
)
enrichment_section_status_total = _safe_counter(
    "sentinel_enrichment_section_status_total",
    "Per-fetcher section status outcomes.",
    ["fetcher", "status"],
)
enrichment_events_consumed_total = _safe_counter(
    "sentinel_enrichment_events_consumed_total",
    "Kafka events the enricher consumed.",
    ["type"],
)
enrichment_events_failed_total = _safe_counter(
    "sentinel_enrichment_events_failed_total",
    "Kafka events the enricher could not process.",
    ["reason"],
)
enrichment_duplicates_total = _safe_counter(
    "sentinel_enrichment_duplicates_total",
    "Re-delivered events that the enricher idempotently ignored.",
    [],
)
enrichment_invalid_events_total = _safe_counter(
    "sentinel_enrichment_invalid_events_total",
    "Events the enricher rejected as malformed (poison-pill committed).",
    [],
)

_NAME_TO_ATTR: dict[str, str] = {
    "sentinel_webhooks_total": "webhooks_total",
    "sentinel_incidents_opened_total": "incidents_opened_total",
    "sentinel_enrichment_duration_seconds": "enrichment_duration_seconds",
    "sentinel_enrichment_failures_total": "enrichment_failures_total",
    "sentinel_circuit_breaker_state": "circuit_breaker_state",
    "sentinel_diagnosis_latency_seconds": "diagnosis_latency_seconds",
    "sentinel_diagnosis_confidence": "diagnosis_confidence",
    "sentinel_llm_tokens_total": "llm_tokens_total",
    "sentinel_llm_cost_usd_total": "llm_cost_usd_total",
    "sentinel_hallucinated_evidence_total": "hallucinated_evidence_total",
    "sentinel_diagnoses_total": "diagnoses_total",
    "sentinel_diagnosis_failures_total": "diagnosis_failures_total",
    "sentinel_diagnosis_input_truncated_total": "diagnosis_input_truncated_total",
    "sentinel_diagnosis_llm_tokens_total": "diagnosis_llm_tokens_total",
    "sentinel_diagnosis_invalid_events_total": "diagnosis_invalid_events_total",
    "sentinel_diagnosis_correctness_rate_30d": "diagnosis_correctness_rate_30d",
    "sentinel_webhook_handler_duration_seconds": "webhook_handler_duration_seconds",
    "sentinel_outbox_events_enqueued_total": "outbox_events_enqueued_total",
    "sentinel_outbox_events_published_total": "outbox_events_published_total",
    "sentinel_outbox_events_failed_total": "outbox_events_failed_total",
    "sentinel_outbox_publish_latency_seconds": "outbox_publish_latency_seconds",
    "sentinel_outbox_unpublished_count": "outbox_unpublished_count",
    "sentinel_outbox_oldest_unpublished_age_seconds": "outbox_oldest_unpublished_age_seconds",
    "sentinel_outbox_event_stuck_total": "outbox_event_stuck_total",
    "sentinel_enrichment_assemble_duration_seconds": "enrichment_assemble_duration_seconds",
    "sentinel_enrichment_section_status_total": "enrichment_section_status_total",
    "sentinel_enrichment_events_consumed_total": "enrichment_events_consumed_total",
    "sentinel_enrichment_events_failed_total": "enrichment_events_failed_total",
    "sentinel_enrichment_duplicates_total": "enrichment_duplicates_total",
    "sentinel_enrichment_invalid_events_total": "enrichment_invalid_events_total",
}


def attr_for(metric_name: str) -> str:
    return _NAME_TO_ATTR[metric_name]


@contextmanager
def time_histogram(metric: Histogram, **labels: str) -> Iterator[None]:
    start = perf_counter()
    try:
        yield
    finally:
        elapsed = perf_counter() - start
        if labels:
            metric.labels(**labels).observe(elapsed)
        else:
            metric.observe(elapsed)


@contextmanager
def count_failure(metric: Counter, **labels: str) -> Iterator[None]:
    try:
        yield
    except Exception:
        if labels:
            metric.labels(**labels).inc()
        else:
            metric.inc()
        raise
