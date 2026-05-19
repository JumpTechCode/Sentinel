# tests/unit/schemas/test_enums.py
"""Enums are the single source of truth — schemas and SA models must agree."""

from sentinel.schemas.enums import (
    CATEGORY_VALUES,
    EVIDENCE_KIND_VALUES,
    INCIDENT_STATUS_VALUES,
    SEVERITY_VALUES,
    SOURCE_VALUES,
)


def test_source_values_match_spec() -> None:
    assert SOURCE_VALUES == ("sentry", "pagerduty", "datadog", "generic")


def test_severity_values_match_spec() -> None:
    assert SEVERITY_VALUES == ("SEV1", "SEV2", "SEV3", "SEV4")


def test_incident_status_values_match_spec() -> None:
    assert INCIDENT_STATUS_VALUES == (
        "open",
        "diagnosing",
        "mitigated",
        "resolved",
        "closed",
    )


def test_category_values_match_spec() -> None:
    assert CATEGORY_VALUES == (
        "deploy",
        "config",
        "dependency",
        "capacity",
        "data",
        "external",
    )


def test_evidence_kind_values_match_spec() -> None:
    assert EVIDENCE_KIND_VALUES == (
        "deploy",
        "similar_incident",
        "runbook",
        "log",
        "related_alert",
    )
