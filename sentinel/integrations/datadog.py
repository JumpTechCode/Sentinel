"""Datadog webhook adapter.

Datadog signs over `timestamp + body`. We accept the request only if the
timestamp is within DATADOG_MAX_SKEW_SECONDS of server time; this defends
against replays of an old, valid signature.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, ClassVar, Final

from pydantic import SecretStr

from sentinel.integrations.base import AdapterParseError, WebhookAdapter
from sentinel.integrations.severity_map import datadog_severity
from sentinel.schemas.alert import NormalizedAlert

DATADOG_MAX_SKEW_SECONDS: Final[int] = 300


def _extract_service(tags: str) -> str:
    for raw_tag in tags.split(","):
        tag = raw_tag.strip()
        if tag.startswith("service:"):
            return tag[len("service:") :]
    raise AdapterParseError("datadog payload tags missing service:<x>")


class DatadogAdapter(WebhookAdapter):
    source: ClassVar[str] = "datadog"
    signature_headers: ClassVar[tuple[str, ...]] = (
        "X-Datadog-Signature",
        "X-Datadog-Timestamp",
    )

    def verify_signature(
        self,
        headers: Mapping[str, str],
        body: bytes,
        secret: SecretStr,
    ) -> bool:
        provided = headers.get("X-Datadog-Signature", "")
        ts_str = headers.get("X-Datadog-Timestamp", "")
        if not provided or not ts_str:
            return False
        try:
            ts_int = int(ts_str)
        except ValueError:
            return False
        if abs(int(time.time()) - ts_int) > DATADOG_MAX_SKEW_SECONDS:
            return False
        signed = ts_str.encode() + body
        expected = hmac.new(
            secret.get_secret_value().encode(),
            signed,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(provided, expected)

    def normalize(self, payload: Mapping[str, Any]) -> NormalizedAlert:
        try:
            external_id = str(payload["alert_id"])
            title = str(payload["event_title"])
            priority = str(payload.get("alert_priority", "P3"))
            tags = str(payload.get("tags", ""))
        except (KeyError, TypeError) as e:
            raise AdapterParseError(f"missing required Datadog fields: {e}") from e

        return NormalizedAlert(
            source="datadog",
            external_id=external_id,
            service=_extract_service(tags),
            severity=datadog_severity(priority),
            title=title,
            received_at=datetime.now(UTC),
            raw_payload=dict(payload),
        )
