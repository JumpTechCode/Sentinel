"""PagerDuty webhook adapter.

Signature header is a comma-separated list of `vN=<hex>` tokens. We accept v1
only; any unknown version tag in the list is ignored. Per PagerDuty docs,
multiple v1 signatures may be present when secrets are rotated — match any.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, ClassVar

from pydantic import SecretStr

from sentinel.integrations.base import (
    AdapterParseError,
    WebhookAdapter,
    compare_signature,
)
from sentinel.integrations.severity_map import pagerduty_severity
from sentinel.schemas.alert import NormalizedAlert


class PagerDutyAdapter(WebhookAdapter):
    source: ClassVar[str] = "pagerduty"
    signature_headers: ClassVar[tuple[str, ...]] = ("X-PagerDuty-Signature",)

    def verify_signature(
        self,
        headers: Mapping[str, str],
        body: bytes,
        secret: SecretStr,
    ) -> bool:
        header = headers.get("X-PagerDuty-Signature", "")
        if not header:
            return False
        for raw_token in header.split(","):
            token = raw_token.strip()
            if not token.startswith("v1="):
                continue
            hex_sig = token[len("v1=") :]
            if compare_signature(hex_sig, body, secret):
                return True
        return False

    def normalize(self, payload: Mapping[str, Any]) -> NormalizedAlert:
        try:
            event = payload["event"]
            data = event["data"]
            external_id = str(data["id"])
            title = str(data["title"])
            urgency = str(data.get("urgency", "low"))
            service = str(data["service"]["summary"])
        except (KeyError, TypeError) as e:
            raise AdapterParseError(f"missing required PagerDuty fields: {e}") from e

        return NormalizedAlert(
            source="pagerduty",
            external_id=external_id,
            service=service,
            severity=pagerduty_severity(urgency),
            title=title,
            received_at=datetime.now(UTC),
            raw_payload=dict(payload),
        )
