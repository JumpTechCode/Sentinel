# sentinel/schemas/enums.py
"""Shared Literal types and value tuples.

These are imported by both Pydantic schemas (`sentinel/schemas/*.py`) and SQLAlchemy
models (`sentinel/persistence/models.py`). Add a value here, and both layers see it.
"""

from __future__ import annotations

from typing import Literal

SOURCE_VALUES: tuple[str, ...] = ("sentry", "pagerduty", "datadog", "generic")
SourceType = Literal["sentry", "pagerduty", "datadog", "generic"]

SEVERITY_VALUES: tuple[str, ...] = ("SEV1", "SEV2", "SEV3", "SEV4")
SeverityType = Literal["SEV1", "SEV2", "SEV3", "SEV4"]

INCIDENT_STATUS_VALUES: tuple[str, ...] = (
    "open",
    "diagnosing",
    "mitigated",
    "resolved",
    "closed",
)
IncidentStatusType = Literal["open", "diagnosing", "mitigated", "resolved", "closed"]

CATEGORY_VALUES: tuple[str, ...] = (
    "deploy",
    "config",
    "dependency",
    "capacity",
    "data",
    "external",
)
CategoryType = Literal["deploy", "config", "dependency", "capacity", "data", "external"]

EVIDENCE_KIND_VALUES: tuple[str, ...] = (
    "deploy",
    "similar_incident",
    "runbook",
    "log",
    "related_alert",
)
EvidenceKindType = Literal["deploy", "similar_incident", "runbook", "log", "related_alert"]
