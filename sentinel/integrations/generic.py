"""Sentinel-native (generic) webhook adapter.

Wire format: header `X-Sentinel-Signature: sha256=<hex>`. Payload shape:
{id, service, severity, title, ...}. Severity values are validated against
the SeverityType enum (no per-source mapping table).
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, ClassVar, cast, get_args

from pydantic import SecretStr

from sentinel.integrations.base import (
    AdapterParseError,
    WebhookAdapter,
    compare_signature,
)
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.enums import SeverityType

_VALID_SEVERITIES = frozenset(get_args(SeverityType))
_PREFIX = "sha256="


class GenericAdapter(WebhookAdapter):
    source: ClassVar[str] = "generic"
    signature_headers: ClassVar[tuple[str, ...]] = ("X-Sentinel-Signature",)

    def verify_signature(
        self,
        headers: Mapping[str, str],
        body: bytes,
        secret: SecretStr,
    ) -> bool:
        provided = headers.get("X-Sentinel-Signature", "")
        if not provided.startswith(_PREFIX):
            return False
        hex_sig = provided[len(_PREFIX) :]
        return compare_signature(hex_sig, body, secret)

    def normalize(self, payload: Mapping[str, Any]) -> NormalizedAlert:
        try:
            external_id = str(payload["id"])
            service = str(payload["service"])
            title = str(payload["title"])
            severity = str(payload["severity"])
        except (KeyError, TypeError) as e:
            raise AdapterParseError(f"missing required generic fields: {e}") from e

        if severity not in _VALID_SEVERITIES:
            raise AdapterParseError(
                f"generic adapter requires severity in {sorted(_VALID_SEVERITIES)}; "
                f"got {severity!r}"
            )

        return NormalizedAlert(
            source="generic",
            external_id=external_id,
            service=service,
            severity=cast(SeverityType, severity),
            title=title,
            received_at=datetime.now(UTC),
            raw_payload=dict(payload),
        )
