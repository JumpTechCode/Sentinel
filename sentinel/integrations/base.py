"""WebhookAdapter Protocol + HMAC helpers + error types.

verify_signature implementations MUST use `compare_signature` (constant-time
via hmac.compare_digest). Never raise from verify_signature; return False.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Any, ClassVar, Protocol

from pydantic import SecretStr

from sentinel.schemas.alert import NormalizedAlert


class AdapterParseError(Exception):
    """Adapter received a payload it cannot map to NormalizedAlert."""


class MissingSecretError(Exception):
    """Per-source webhook secret is not configured."""


def compute_hmac_sha256(body: bytes, secret: bytes) -> str:
    """Hex HMAC-SHA256 digest of `body` keyed by `secret`."""
    return hmac.new(secret, body, hashlib.sha256).hexdigest()


def compare_signature(provided_hex: str, body: bytes, secret: SecretStr) -> bool:
    """Constant-time compare of provided hex against HMAC-SHA256(body)."""
    if not provided_hex:
        return False
    expected = compute_hmac_sha256(body, secret.get_secret_value().encode())
    return hmac.compare_digest(provided_hex, expected)


class WebhookAdapter(Protocol):
    source: ClassVar[str]
    signature_headers: ClassVar[tuple[str, ...]]

    def verify_signature(
        self,
        headers: Mapping[str, str],
        body: bytes,
        secret: SecretStr,
    ) -> bool: ...

    def normalize(self, payload: Mapping[str, Any]) -> NormalizedAlert: ...
