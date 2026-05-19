from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path

import pytest
from pydantic import SecretStr
from sentinel.integrations.base import AdapterParseError
from sentinel.integrations.generic import GenericAdapter

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "webhooks" / "generic.json"


def _body() -> bytes:
    return FIXTURE.read_bytes()


def _sig(secret: bytes, body: bytes) -> str:
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def test_normalize_passes_through_typed_fields() -> None:
    adapter = GenericAdapter()
    payload = json.loads(_body())
    alert = adapter.normalize(payload)
    assert alert.source == "generic"
    assert alert.external_id == "ext-9001"
    assert alert.service == "billing-worker"
    assert alert.severity == "SEV2"
    assert alert.title.startswith("Stripe")


def test_normalize_invalid_severity_raises() -> None:
    adapter = GenericAdapter()
    payload = json.loads(_body())
    payload["severity"] = "CRITICAL"  # not a SeverityType
    with pytest.raises(AdapterParseError):
        adapter.normalize(payload)


def test_verify_signature_positive_sha256_prefix() -> None:
    adapter = GenericAdapter()
    body = _body()
    sig = _sig(b"secret", body)
    headers = {"X-Sentinel-Signature": f"sha256={sig}"}
    assert adapter.verify_signature(headers, body, SecretStr("secret")) is True


def test_verify_signature_negative_missing_prefix() -> None:
    adapter = GenericAdapter()
    body = _body()
    sig = _sig(b"secret", body)
    headers = {"X-Sentinel-Signature": sig}  # no sha256= prefix
    assert adapter.verify_signature(headers, body, SecretStr("secret")) is False


def test_verify_signature_negative_wrong_secret() -> None:
    adapter = GenericAdapter()
    body = _body()
    sig = _sig(b"secret", body)
    headers = {"X-Sentinel-Signature": f"sha256={sig}"}
    assert adapter.verify_signature(headers, body, SecretStr("other")) is False
