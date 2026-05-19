# tests/unit/schemas/test_alert.py
"""NormalizedAlert is the canonical output of every adapter's normalize()."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from sentinel.schemas.alert import NormalizedAlert


def _valid_payload() -> dict[str, object]:
    return {
        "source": "sentry",
        "external_id": "evt-abc-123",
        "service": "checkout-api",
        "severity": "SEV2",
        "title": "Elevated 5xx on /checkout",
        "received_at": datetime(2026, 5, 18, 18, 0, tzinfo=UTC),
        "raw_payload": {"any": "shape"},
    }


def test_round_trip() -> None:
    alert = NormalizedAlert(**_valid_payload())
    json_blob = alert.model_dump_json()
    parsed = NormalizedAlert.model_validate_json(json_blob)
    assert parsed == alert


def test_rejects_bad_source() -> None:
    bad = _valid_payload() | {"source": "splunk"}
    with pytest.raises(ValidationError):
        NormalizedAlert(**bad)


def test_rejects_bad_severity() -> None:
    bad = _valid_payload() | {"severity": "P1"}
    with pytest.raises(ValidationError):
        NormalizedAlert(**bad)


def test_requires_tz_aware_timestamp() -> None:
    bad = _valid_payload() | {"received_at": datetime(2026, 5, 18, 18, 0)}
    with pytest.raises(ValidationError, match="timezone"):
        NormalizedAlert(**bad)
