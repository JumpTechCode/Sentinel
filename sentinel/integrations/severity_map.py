"""Per-source severity → Sentinel SeverityType mapping.

Unknown vendor values default to SEV3 with a warning log.
"""

from __future__ import annotations

import logging
from typing import Final

from sentinel.schemas.enums import SeverityType

log = logging.getLogger(__name__)


_SENTRY: Final[dict[str, SeverityType]] = {
    "fatal": "SEV1",
    "error": "SEV2",
    "warning": "SEV3",
    "info": "SEV4",
    "debug": "SEV4",
}

_PAGERDUTY: Final[dict[str, SeverityType]] = {
    "high": "SEV2",
    "low": "SEV3",
}

_DATADOG: Final[dict[str, SeverityType]] = {
    "P1": "SEV1",
    "P2": "SEV2",
    "P3": "SEV3",
    "P4": "SEV4",
    "P5": "SEV4",
}


def _default(source: str, raw: str) -> SeverityType:
    log.warning("unknown_severity_value", extra={"source": source, "raw": raw})
    return "SEV3"


def sentry_severity(level: str) -> SeverityType:
    key = level.lower()
    if key in _SENTRY:
        return _SENTRY[key]
    return _default("sentry", level)


def pagerduty_severity(urgency: str) -> SeverityType:
    key = urgency.lower()
    if key in _PAGERDUTY:
        return _PAGERDUTY[key]
    return _default("pagerduty", urgency)


def datadog_severity(priority: str) -> SeverityType:
    key = priority.upper()
    if key in _DATADOG:
        return _DATADOG[key]
    return _default("datadog", priority)
