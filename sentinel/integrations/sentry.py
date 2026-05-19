"""Sentry webhook adapter (issue-alert variant)."""

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
from sentinel.integrations.severity_map import sentry_severity
from sentinel.schemas.alert import NormalizedAlert


class SentryAdapter(WebhookAdapter):
    source: ClassVar[str] = "sentry"
    signature_headers: ClassVar[tuple[str, ...]] = ("Sentry-Hook-Signature",)

    def verify_signature(
        self,
        headers: Mapping[str, str],
        body: bytes,
        secret: SecretStr,
    ) -> bool:
        provided = headers.get("Sentry-Hook-Signature", "")
        return compare_signature(provided, body, secret)

    def normalize(self, payload: Mapping[str, Any]) -> NormalizedAlert:
        try:
            issue = payload["data"]["issue"]
            external_id = str(issue["id"])
            title = str(issue["title"])
            level = str(issue.get("level", "warning"))
            service = str(issue["project"]["slug"])
        except (KeyError, TypeError) as e:
            raise AdapterParseError(f"missing required Sentry fields: {e}") from e

        return NormalizedAlert(
            source="sentry",
            external_id=external_id,
            service=service,
            severity=sentry_severity(level),
            title=title,
            received_at=datetime.now(UTC),
            raw_payload=dict(payload),
        )
