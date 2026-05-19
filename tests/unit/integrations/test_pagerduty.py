from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path

from pydantic import SecretStr
from sentinel.integrations.pagerduty import PagerDutyAdapter

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "webhooks" / "pagerduty.json"


def _body() -> bytes:
    return FIXTURE.read_bytes()


def _sig(secret: bytes, body: bytes) -> str:
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def test_normalize_extracts_canonical_fields() -> None:
    adapter = PagerDutyAdapter()
    payload = json.loads(_body())
    alert = adapter.normalize(payload)
    assert alert.source == "pagerduty"
    assert alert.external_id == "PINCD01"
    assert alert.service == "datastore-replication"
    assert alert.severity == "SEV2"  # urgency=high
    assert "replication lag" in alert.title.lower()


def test_verify_signature_single_v1() -> None:
    adapter = PagerDutyAdapter()
    body = _body()
    sig = _sig(b"secret", body)
    headers = {"X-PagerDuty-Signature": f"v1={sig}"}
    assert adapter.verify_signature(headers, body, SecretStr("secret")) is True


def test_verify_signature_multi_v1_list_accepts_any_match() -> None:
    adapter = PagerDutyAdapter()
    body = _body()
    good = _sig(b"secret", body)
    bad = "0" * 64
    headers = {"X-PagerDuty-Signature": f"v1={bad},v1={good}"}
    assert adapter.verify_signature(headers, body, SecretStr("secret")) is True


def test_verify_signature_rejects_unknown_version_tag() -> None:
    adapter = PagerDutyAdapter()
    body = _body()
    sig = _sig(b"secret", body)
    headers = {"X-PagerDuty-Signature": f"v99={sig}"}
    assert adapter.verify_signature(headers, body, SecretStr("secret")) is False


def test_verify_signature_negative_wrong_secret() -> None:
    adapter = PagerDutyAdapter()
    body = _body()
    headers = {"X-PagerDuty-Signature": f"v1={_sig(b'secret', body)}"}
    assert adapter.verify_signature(headers, body, SecretStr("other")) is False


def test_verify_signature_negative_missing_header() -> None:
    adapter = PagerDutyAdapter()
    assert adapter.verify_signature({}, _body(), SecretStr("secret")) is False
