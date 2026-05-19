from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import SecretStr
from sentinel.integrations.base import AdapterParseError, compute_hmac_sha256
from sentinel.integrations.sentry import SentryAdapter

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "webhooks" / "sentry.json"


def _body() -> bytes:
    return FIXTURE.read_bytes()


def test_normalize_extracts_canonical_fields() -> None:
    adapter = SentryAdapter()
    payload = json.loads(_body())
    alert = adapter.normalize(payload)
    assert alert.source == "sentry"
    assert alert.external_id == "1234567"
    assert alert.service == "payments-api"
    assert alert.severity == "SEV2"
    assert alert.title.startswith("ConnectionError")
    assert alert.received_at is not None
    assert alert.raw_payload == payload


def test_verify_signature_positive() -> None:
    adapter = SentryAdapter()
    body = _body()
    sig = compute_hmac_sha256(body, b"shh")
    headers = {"Sentry-Hook-Signature": sig}
    assert adapter.verify_signature(headers, body, SecretStr("shh")) is True


def test_verify_signature_negative_tampered_body() -> None:
    adapter = SentryAdapter()
    body = _body()
    sig = compute_hmac_sha256(body, b"shh")
    headers = {"Sentry-Hook-Signature": sig}
    assert adapter.verify_signature(headers, body + b" ", SecretStr("shh")) is False


def test_verify_signature_negative_missing_header() -> None:
    adapter = SentryAdapter()
    assert adapter.verify_signature({}, _body(), SecretStr("shh")) is False


def test_verify_signature_negative_wrong_secret() -> None:
    adapter = SentryAdapter()
    body = _body()
    sig = compute_hmac_sha256(body, b"shh")
    headers = {"Sentry-Hook-Signature": sig}
    assert adapter.verify_signature(headers, body, SecretStr("other")) is False


def test_normalize_raises_on_bad_shape() -> None:
    adapter = SentryAdapter()
    with pytest.raises(AdapterParseError):
        adapter.normalize({"action": "created"})  # no data.issue
