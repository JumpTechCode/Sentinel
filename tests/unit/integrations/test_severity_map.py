from __future__ import annotations

import pytest
from sentinel.integrations.severity_map import datadog_severity, pagerduty_severity, sentry_severity


@pytest.mark.parametrize(
    "level,expected",
    [
        ("fatal", "SEV1"),
        ("error", "SEV2"),
        ("warning", "SEV3"),
        ("info", "SEV4"),
        ("debug", "SEV4"),
    ],
)
def test_sentry_severity_known(level: str, expected: str) -> None:
    assert sentry_severity(level) == expected


def test_sentry_severity_unknown_defaults_to_sev3() -> None:
    assert sentry_severity("verbose") == "SEV3"


@pytest.mark.parametrize(
    "urgency,expected",
    [
        ("high", "SEV2"),
        ("low", "SEV3"),
    ],
)
def test_pagerduty_severity_known(urgency: str, expected: str) -> None:
    assert pagerduty_severity(urgency) == expected


def test_pagerduty_severity_unknown_defaults_to_sev3() -> None:
    assert pagerduty_severity("medium") == "SEV3"


@pytest.mark.parametrize(
    "priority,expected",
    [
        ("P1", "SEV1"),
        ("P2", "SEV2"),
        ("P3", "SEV3"),
        ("P4", "SEV4"),
        ("P5", "SEV4"),
    ],
)
def test_datadog_severity_known(priority: str, expected: str) -> None:
    assert datadog_severity(priority) == expected


def test_datadog_severity_unknown_defaults_to_sev3() -> None:
    assert datadog_severity("normal") == "SEV3"
