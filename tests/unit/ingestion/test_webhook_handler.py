from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import SecretStr
from sentinel.config.settings import Settings
from sentinel.ingestion.webhook import HandlerOutcome, WebhookHandler
from sentinel.persistence.repositories import IngestResult


@pytest.fixture
def fixture_body() -> bytes:
    return (
        Path(__file__).resolve().parents[2] / "fixtures" / "webhooks" / "sentry.json"
    ).read_bytes()


def _settings_with_sentry_secret() -> Settings:
    return Settings.model_construct(sentry_webhook_secret=SecretStr("shh"))


def _make_handler(
    *,
    ingest_result: IngestResult | None = None,
    settings: Settings | None = None,
) -> tuple[WebhookHandler, Any, Any]:
    incident_repo = MagicMock()
    incident_repo.ingest = AsyncMock(
        return_value=ingest_result or IngestResult(incident_id=uuid4(), event_kind="opened")
    )
    idempotency = MagicMock()
    idempotency.check_and_mark = AsyncMock(return_value=False)
    idempotency.unmark = AsyncMock()
    handler = WebhookHandler(
        incident_repo=incident_repo,
        idempotency=idempotency,
        settings=settings or _settings_with_sentry_secret(),
        outbox_topic="sentinel.incidents",
    )
    return handler, incident_repo, idempotency


@pytest.mark.asyncio
async def test_happy_path_returns_accepted(fixture_body: bytes) -> None:
    handler, repo, _ = _make_handler()
    from sentinel.integrations.base import compute_hmac_sha256

    sig = compute_hmac_sha256(fixture_body, b"shh")
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": sig},
        body=fixture_body,
    )
    assert result.outcome == HandlerOutcome.ACCEPTED
    assert result.incident_id is not None
    repo.ingest.assert_awaited_once()


@pytest.mark.asyncio
async def test_recurred_path_returns_recurred(fixture_body: bytes) -> None:
    handler, _, _ = _make_handler(
        ingest_result=IngestResult(incident_id=uuid4(), event_kind="recurred")
    )
    from sentinel.integrations.base import compute_hmac_sha256

    sig = compute_hmac_sha256(fixture_body, b"shh")
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": sig},
        body=fixture_body,
    )
    assert result.outcome == HandlerOutcome.RECURRED


@pytest.mark.asyncio
async def test_bad_signature_returns_unauthorized(fixture_body: bytes) -> None:
    handler, repo, _ = _make_handler()
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": "0" * 64},
        body=fixture_body,
    )
    assert result.outcome == HandlerOutcome.UNAUTHORIZED
    repo.ingest.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_secret_returns_unauthorized(fixture_body: bytes) -> None:
    from sentinel.config.settings import Settings

    handler = WebhookHandler(
        incident_repo=MagicMock(ingest=AsyncMock()),
        idempotency=MagicMock(check_and_mark=AsyncMock(return_value=False)),
        settings=Settings.model_construct(sentry_webhook_secret=None),
        outbox_topic="sentinel.incidents",
    )
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": "x" * 64},
        body=fixture_body,
    )
    assert result.outcome == HandlerOutcome.UNAUTHORIZED


@pytest.mark.asyncio
async def test_unknown_source_returns_unknown_source(fixture_body: bytes) -> None:
    handler, _, _ = _make_handler()
    result = await handler.handle(source="nope", headers={}, body=fixture_body)
    assert result.outcome == HandlerOutcome.UNKNOWN_SOURCE


@pytest.mark.asyncio
async def test_duplicate_returns_duplicate(fixture_body: bytes) -> None:
    handler, repo, idem = _make_handler()
    idem.check_and_mark = AsyncMock(return_value=True)
    from sentinel.integrations.base import compute_hmac_sha256

    sig = compute_hmac_sha256(fixture_body, b"shh")
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": sig},
        body=fixture_body,
    )
    assert result.outcome == HandlerOutcome.DUPLICATE
    repo.ingest.assert_not_awaited()


@pytest.mark.asyncio
async def test_bad_json_returns_bad_request(fixture_body: bytes) -> None:
    handler, _, _ = _make_handler()
    from sentinel.integrations.base import compute_hmac_sha256

    bad = b"not json"
    sig = compute_hmac_sha256(bad, b"shh")
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": sig},
        body=bad,
    )
    assert result.outcome == HandlerOutcome.BAD_REQUEST


@pytest.mark.asyncio
async def test_ingest_failure_unmarks_idempotency_key(fixture_body: bytes) -> None:
    """Lost-event race (issue #62): if persistence fails *after* the idempotency
    key is marked, the key must be cleared so the sender's byte-identical retry
    re-processes the event instead of being silently deduped into oblivion.
    """
    handler, repo, idem = _make_handler()
    repo.ingest = AsyncMock(side_effect=RuntimeError("transient db failure"))
    from sentinel.integrations.base import compute_hmac_sha256

    sig = compute_hmac_sha256(fixture_body, b"shh")
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": sig},
        body=fixture_body,
    )

    # Sender must see a retryable status (503), not a 2xx that ends delivery.
    assert result.outcome == HandlerOutcome.INFRA_UNAVAILABLE
    # The compensating delete must have fired so the retry is not deduped.
    idem.unmark.assert_awaited_once_with("sentry", fixture_body)


@pytest.mark.asyncio
async def test_ingest_failure_still_503_when_unmark_also_fails(
    fixture_body: bytes,
) -> None:
    """If the compensating unmark itself fails (e.g. Redis down), the handler
    must still return a retryable 503 rather than surfacing the unmark error.
    """
    handler, repo, idem = _make_handler()
    repo.ingest = AsyncMock(side_effect=RuntimeError("transient db failure"))
    idem.unmark = AsyncMock(side_effect=RuntimeError("redis down"))
    from sentinel.integrations.base import compute_hmac_sha256

    sig = compute_hmac_sha256(fixture_body, b"shh")
    result = await handler.handle(
        source="sentry",
        headers={"Sentry-Hook-Signature": sig},
        body=fixture_body,
    )

    assert result.outcome == HandlerOutcome.INFRA_UNAVAILABLE


@pytest.mark.asyncio
async def test_handler_median_under_50ms(fixture_body: bytes) -> None:
    handler, _, _ = _make_handler()
    from sentinel.integrations.base import compute_hmac_sha256

    sig = compute_hmac_sha256(fixture_body, b"shh")
    samples: list[float] = []
    for _ in range(11):
        t0 = time.perf_counter()
        await handler.handle(
            source="sentry",
            headers={"Sentry-Hook-Signature": sig},
            body=fixture_body,
        )
        samples.append(time.perf_counter() - t0)
    samples.sort()
    median = samples[len(samples) // 2]
    assert median < 0.05, f"median {median * 1000:.1f}ms exceeds 50ms budget"
