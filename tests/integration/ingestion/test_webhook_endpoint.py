"""End-to-end webhook endpoint tests with real Postgres + Redis + Kafka."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sentinel.integrations.base import compute_hmac_sha256
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


SENTRY_FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "webhooks" / "sentry.json"


@pytest.fixture(autouse=True)
def _env(
    monkeypatch: pytest.MonkeyPatch,
    pg_dsn: str,
    redis_url: str,
    kafka_brokers: str,
) -> None:
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", pg_dsn)
    monkeypatch.setenv("SENTINEL_REDIS_URL", redis_url)
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", kafka_brokers)
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "x")
    monkeypatch.setenv("SENTINEL_SENTRY_WEBHOOK_SECRET", "shh")


@pytest_asyncio.fixture
async def app(pg_dsn: str) -> AsyncIterator[FastAPI]:
    # Migrations are run once per session by conftest's autouse _migrate fixture.
    # (Don't call alembic.command.upgrade from an async context — env.py uses
    # asyncio.run internally, which raises inside a running event loop.)
    from sentinel.api.app import build_app

    a = build_app()
    async with a.router.lifespan_context(a):
        yield a


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_happy_path(client: AsyncClient, pg_dsn: str) -> None:
    body = SENTRY_FIXTURE.read_bytes()
    sig = compute_hmac_sha256(body, b"shh")
    r = await client.post(
        "/webhooks/sentry",
        content=body,
        headers={"Sentry-Hook-Signature": sig},
    )
    assert r.status_code == 202
    assert r.json()["status"] == "accepted"
    incident_id = r.json()["incident_id"]

    engine = create_async_engine(pg_dsn)
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT id FROM incidents WHERE id = :id"),
                {"id": incident_id},
            )
        ).first()
        assert row is not None
        ob = (
            await conn.execute(
                text("SELECT COUNT(*) FROM outbox_events WHERE key = :k"),
                {"k": incident_id},
            )
        ).scalar_one()
        assert ob == 1
    await engine.dispose()


@pytest.mark.asyncio
async def test_duplicate_body(client: AsyncClient) -> None:
    body = SENTRY_FIXTURE.read_bytes()
    sig = compute_hmac_sha256(body, b"shh")
    r1 = await client.post("/webhooks/sentry", content=body, headers={"Sentry-Hook-Signature": sig})
    assert r1.status_code == 202
    r2 = await client.post("/webhooks/sentry", content=body, headers={"Sentry-Hook-Signature": sig})
    assert r2.status_code == 200
    assert r2.json()["status"] == "duplicate"


@pytest.mark.asyncio
async def test_same_fingerprint_within_1h_updates_existing(
    client: AsyncClient, pg_dsn: str
) -> None:
    body1 = SENTRY_FIXTURE.read_bytes()
    payload = json.loads(body1)
    payload["data"]["issue"]["id"] = "9999999"  # different external_id
    body2 = json.dumps(payload).encode()

    r1 = await client.post(
        "/webhooks/sentry",
        content=body1,
        headers={"Sentry-Hook-Signature": compute_hmac_sha256(body1, b"shh")},
    )
    r2 = await client.post(
        "/webhooks/sentry",
        content=body2,
        headers={"Sentry-Hook-Signature": compute_hmac_sha256(body2, b"shh")},
    )
    assert r1.status_code == 202 and r1.json()["status"] == "accepted"
    assert r2.status_code == 202 and r2.json()["status"] == "recurred"
    assert r1.json()["incident_id"] == r2.json()["incident_id"]

    engine = create_async_engine(pg_dsn)
    async with engine.connect() as conn:
        cnt = (await conn.execute(text("SELECT COUNT(*) FROM incidents"))).scalar_one()
        assert cnt == 1
        meta = (
            await conn.execute(
                text("SELECT raw_payload->'_sentinel' FROM incidents WHERE id = :id"),
                {"id": r1.json()["incident_id"]},
            )
        ).scalar_one()
        assert meta["occurrence_count"] == 2
        assert len(meta["event_log"]) == 2
    await engine.dispose()


@pytest.mark.asyncio
async def test_bad_signature_rejects(client: AsyncClient, pg_dsn: str) -> None:
    body = SENTRY_FIXTURE.read_bytes()
    r = await client.post(
        "/webhooks/sentry",
        content=body,
        headers={"Sentry-Hook-Signature": "0" * 64},
    )
    assert r.status_code == 401

    engine = create_async_engine(pg_dsn)
    async with engine.connect() as conn:
        cnt = (await conn.execute(text("SELECT COUNT(*) FROM incidents"))).scalar_one()
        assert cnt == 0
    await engine.dispose()


@pytest.mark.asyncio
async def test_unknown_source_returns_404(client: AsyncClient) -> None:
    r = await client.post("/webhooks/nope", content=b"{}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_malformed_json_returns_422(client: AsyncClient) -> None:
    body = b"not json at all"
    sig = compute_hmac_sha256(body, b"shh")
    r = await client.post("/webhooks/sentry", content=body, headers={"Sentry-Hook-Signature": sig})
    assert r.status_code == 422
