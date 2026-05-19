from __future__ import annotations

import hashlib
import hmac
import json
import time
from pathlib import Path

import pytest
from pydantic import SecretStr
from sentinel.integrations.base import AdapterParseError
from sentinel.integrations.datadog import DATADOG_MAX_SKEW_SECONDS, DatadogAdapter

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "webhooks" / "datadog.json"


def _body() -> bytes:
    return FIXTURE.read_bytes()


def _sig(secret: bytes, ts: str, body: bytes) -> str:
    return hmac.new(secret, ts.encode() + body, hashlib.sha256).hexdigest()


def _now_ts() -> str:
    return str(int(time.time()))


def test_normalize_extracts_canonical_fields() -> None:
    adapter = DatadogAdapter()
    payload = json.loads(_body())
    alert = adapter.normalize(payload)
    assert alert.source == "datadog"
    assert alert.external_id == "DDALERT-42"
    assert alert.service == "web-frontend"
    assert alert.severity == "SEV2"
    assert "CPU" in alert.title


def test_normalize_missing_service_tag_raises() -> None:
    adapter = DatadogAdapter()
    payload = json.loads(_body())
    payload["tags"] = "env:prod,team:frontend"
    with pytest.raises(AdapterParseError):
        adapter.normalize(payload)


def test_verify_signature_positive() -> None:
    adapter = DatadogAdapter()
    body = _body()
    ts = _now_ts()
    sig = _sig(b"secret", ts, body)
    headers = {"X-Datadog-Signature": sig, "X-Datadog-Timestamp": ts}
    assert adapter.verify_signature(headers, body, SecretStr("secret")) is True


def test_verify_signature_negative_stale_timestamp() -> None:
    adapter = DatadogAdapter()
    body = _body()
    ts = str(int(time.time()) - (DATADOG_MAX_SKEW_SECONDS + 60))
    sig = _sig(b"secret", ts, body)
    headers = {"X-Datadog-Signature": sig, "X-Datadog-Timestamp": ts}
    assert adapter.verify_signature(headers, body, SecretStr("secret")) is False


def test_verify_signature_negative_tampered_body() -> None:
    adapter = DatadogAdapter()
    body = _body()
    ts = _now_ts()
    sig = _sig(b"secret", ts, body)
    headers = {"X-Datadog-Signature": sig, "X-Datadog-Timestamp": ts}
    assert adapter.verify_signature(headers, body + b" ", SecretStr("secret")) is False


def test_verify_signature_negative_missing_headers() -> None:
    adapter = DatadogAdapter()
    assert adapter.verify_signature({}, _body(), SecretStr("secret")) is False
