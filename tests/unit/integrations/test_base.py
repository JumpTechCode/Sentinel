from __future__ import annotations

from pydantic import SecretStr
from sentinel.integrations.base import (
    AdapterParseError,
    MissingSecretError,
    compare_signature,
    compute_hmac_sha256,
)


def test_compute_hmac_sha256_is_deterministic() -> None:
    body = b'{"a":1}'
    sig1 = compute_hmac_sha256(body, b"secret")
    sig2 = compute_hmac_sha256(body, b"secret")
    assert sig1 == sig2
    assert len(sig1) == 64  # hex digest of sha256


def test_compute_hmac_sha256_different_secret_yields_different_signature() -> None:
    body = b'{"a":1}'
    assert compute_hmac_sha256(body, b"s1") != compute_hmac_sha256(body, b"s2")


def test_compare_signature_positive() -> None:
    body = b'{"a":1}'
    sig = compute_hmac_sha256(body, b"secret")
    assert compare_signature(sig, body, SecretStr("secret")) is True


def test_compare_signature_negative_tampered() -> None:
    body = b'{"a":1}'
    sig = compute_hmac_sha256(body, b"secret")
    assert compare_signature(sig, body + b" ", SecretStr("secret")) is False


def test_compare_signature_empty_provided_signature() -> None:
    body = b'{"a":1}'
    assert compare_signature("", body, SecretStr("secret")) is False


def test_errors_are_exceptions() -> None:
    assert issubclass(AdapterParseError, Exception)
    assert issubclass(MissingSecretError, Exception)
