# tests/unit/observability/test_metrics.py
"""Every metric from the spec exists with correct type and labels."""

from __future__ import annotations

import pytest
from prometheus_client import Counter, Gauge, Histogram
from sentinel.observability import metrics as M
from sentinel.observability.metrics import (
    outbox_event_stuck_total,
    outbox_events_enqueued_total,
    outbox_events_failed_total,
    outbox_events_published_total,
    outbox_oldest_unpublished_age_seconds,
    outbox_publish_latency_seconds,
    outbox_unpublished_count,
    webhook_handler_duration_seconds,
)


def test_all_spec_metrics_exist() -> None:
    expected: dict[str, type] = {
        "sentinel_webhooks_total": Counter,
        "sentinel_incidents_opened_total": Counter,
        "sentinel_enrichment_duration_seconds": Histogram,
        "sentinel_enrichment_failures_total": Counter,
        "sentinel_circuit_breaker_state": Gauge,
        "sentinel_diagnosis_latency_seconds": Histogram,
        "sentinel_diagnosis_confidence": Histogram,
        "sentinel_llm_tokens_total": Counter,
        "sentinel_llm_cost_usd_total": Counter,
        "sentinel_diagnosis_correctness_rate_30d": Gauge,
        "sentinel_webhook_handler_duration_seconds": Histogram,
        "sentinel_outbox_events_enqueued_total": Counter,
        "sentinel_outbox_events_published_total": Counter,
        "sentinel_outbox_events_failed_total": Counter,
        "sentinel_outbox_publish_latency_seconds": Histogram,
        "sentinel_outbox_unpublished_count": Gauge,
        "sentinel_outbox_oldest_unpublished_age_seconds": Gauge,
        "sentinel_outbox_event_stuck_total": Counter,
    }
    for name, klass in expected.items():
        instance = getattr(M, M.attr_for(name))
        assert isinstance(
            instance, klass
        ), f"{name} is {type(instance).__name__}, expected {klass.__name__}"


def test_diagnosis_latency_buckets_match_spec() -> None:
    h = M.diagnosis_latency_seconds
    bounds = [s for s in h._upper_bounds if s != float("inf")]
    assert bounds == [1, 2, 5, 10, 15, 30]


def test_diagnosis_confidence_buckets_match_spec() -> None:
    h = M.diagnosis_confidence
    bounds = [s for s in h._upper_bounds if s != float("inf")]
    assert bounds == [0.1, 0.3, 0.5, 0.7, 0.85, 1.0]


def test_time_histogram_records_on_success() -> None:
    with M.time_histogram(M.enrichment_duration_seconds, fetcher="deploys"):
        pass
    sample = M.enrichment_duration_seconds.labels(fetcher="deploys")
    assert sample._sum.get() > 0


def test_time_histogram_records_on_exception() -> None:
    with pytest.raises(RuntimeError):
        with M.time_histogram(M.enrichment_duration_seconds, fetcher="logs"):
            raise RuntimeError("boom")
    sample = M.enrichment_duration_seconds.labels(fetcher="logs")
    assert sample._sum.get() > 0


def test_double_import_does_not_raise() -> None:
    import importlib

    importlib.reload(M)  # must not throw on re-registration


def test_count_failure_success_path_does_not_increment() -> None:
    sample = M.enrichment_failures_total.labels(fetcher="deploys", reason="x")
    before = sample._value.get()
    with M.count_failure(M.enrichment_failures_total, fetcher="deploys", reason="x"):
        pass
    assert sample._value.get() == before


def test_count_failure_exception_increments_and_reraises() -> None:
    sample = M.enrichment_failures_total.labels(fetcher="logs", reason="boom")
    before = sample._value.get()
    with pytest.raises(RuntimeError):
        with M.count_failure(M.enrichment_failures_total, fetcher="logs", reason="boom"):
            raise RuntimeError("boom")
    assert sample._value.get() == before + 1


def test_webhook_handler_duration_label_set() -> None:
    webhook_handler_duration_seconds.labels(source="sentry").observe(0.01)


def test_outbox_metrics_have_expected_labels() -> None:
    outbox_events_enqueued_total.labels(topic="sentinel.incidents").inc()
    outbox_events_published_total.labels(topic="sentinel.incidents").inc()
    outbox_events_failed_total.labels(topic="sentinel.incidents").inc()
    outbox_publish_latency_seconds.labels(topic="sentinel.incidents").observe(0.1)
    outbox_unpublished_count.set(0)
    outbox_oldest_unpublished_age_seconds.set(0)
    outbox_event_stuck_total.inc()
