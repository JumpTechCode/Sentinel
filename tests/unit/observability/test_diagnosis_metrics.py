# tests/unit/observability/test_diagnosis_metrics.py
"""Diagnosis metric registration — Work Area G."""

from __future__ import annotations

import sentinel.observability.metrics  # noqa: F401 — registers all metrics as a side-effect
from prometheus_client import REGISTRY


def test_hallucinated_evidence_rate_gauge_is_removed() -> None:
    assert REGISTRY._names_to_collectors.get("sentinel_hallucinated_evidence_rate") is None


def test_new_diagnosis_counters_are_registered() -> None:
    expected = {
        "sentinel_hallucinated_evidence_total",
        "sentinel_diagnoses_total",
        "sentinel_diagnosis_failures_total",
        "sentinel_diagnosis_input_truncated_total",
        "sentinel_diagnosis_llm_tokens_total",
        "sentinel_diagnosis_invalid_events_total",
    }
    registered = set(REGISTRY._names_to_collectors.keys())
    missing = expected - registered
    assert not missing, f"missing diagnosis counters: {missing}"


def test_diagnoses_total_has_status_label() -> None:
    from sentinel.observability.metrics import diagnoses_total

    diagnoses_total.labels(status="new").inc(0)
    diagnoses_total.labels(status="duplicate").inc(0)


def test_diagnosis_failures_total_has_reason_label() -> None:
    from sentinel.observability.metrics import diagnosis_failures_total

    # The full documented label set per the design doc §6:
    # schema | timeout | transport | no_tool_call | missing_context |
    # missing_incident | exception. `exception` is the outer catch-all in
    # DiagnosisConsumer.run for errors not caught by a named branch.
    for reason in (
        "schema",
        "timeout",
        "transport",
        "no_tool_call",
        "missing_context",
        "missing_incident",
        "exception",
    ):
        diagnosis_failures_total.labels(reason=reason).inc(0)


def test_diagnosis_input_truncated_total_has_section_label() -> None:
    from sentinel.observability.metrics import diagnosis_input_truncated_total

    diagnosis_input_truncated_total.labels(section="recent_logs").inc(0)


def test_diagnosis_llm_tokens_total_has_kind_label() -> None:
    from sentinel.observability.metrics import diagnosis_llm_tokens_total

    diagnosis_llm_tokens_total.labels(kind="input").inc(0)
    diagnosis_llm_tokens_total.labels(kind="output").inc(0)
