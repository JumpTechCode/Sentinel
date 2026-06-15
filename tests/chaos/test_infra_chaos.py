# tests/chaos/test_infra_chaos.py
"""Chaos B1 — ingestion under infra outage, via toxiproxy in front of Redis.

Asserts the spec's "graceful degradation" contract on the synchronous ingestion
path: when the idempotency store (Redis) is unavailable the webhook fails closed
(503 INFRA_UNAVAILABLE) with no incident written, and recovers cleanly once the
dependency returns. Under mere latency, events are not dropped.

Requires Docker (testcontainers + toxiproxy). Run via `make chaos`.
"""

from __future__ import annotations

import asyncio
import json
from time import perf_counter

import pytest
from httpx import AsyncClient, Response
from sentinel.integrations.base import compute_hmac_sha256
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.chaos.conftest import GENERIC_SECRET, ChaosStack

pytestmark = pytest.mark.chaos


def _signed(i: int) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(
        {
            "id": f"chaos-{i}",
            "service": f"svc-{i}",
            "severity": "SEV2",
            "title": f"chaos alert {i}",
        }
    ).encode()
    sig = "sha256=" + compute_hmac_sha256(body, GENERIC_SECRET.encode())
    return body, {"X-Sentinel-Signature": sig, "Content-Type": "application/json"}


async def _post(client: AsyncClient, i: int) -> Response:
    """Post a signed webhook, bounded so a hung Redis call fails the test rather
    than stalling CI (the product Redis client has no socket timeout)."""
    body, headers = _signed(i)
    return await asyncio.wait_for(
        client.post("/webhooks/generic", content=body, headers=headers),
        timeout=15.0,
    )


async def _incident_count(pg_dsn: str) -> int:
    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            return int((await conn.execute(text("SELECT COUNT(*) FROM incidents"))).scalar_one())
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_redis_outage_fails_closed(client: AsyncClient, chaos_stack: ChaosStack) -> None:
    # Baseline: healthy Redis → accepted.
    assert (await _post(client, 1)).status_code == 202

    # Sever Redis: the idempotency check raises → fail closed, no incident written.
    chaos_stack.redis_proxy.disable()
    r = await _post(client, 2)
    assert r.status_code == 503, f"expected 503 under Redis outage, got {r.status_code}"
    # Pin the 503 to the simulated Redis outage — not a stray bad-sig/missing-secret 503.
    assert r.json()["reason"] == "idempotency_unavailable", f"unexpected 503 body: {r.json()}"

    # Only the first (pre-outage) webhook produced an incident — nothing dropped silently.
    assert await _incident_count(chaos_stack.pg_dsn) == 1


@pytest.mark.asyncio
async def test_recovers_after_redis_restored(client: AsyncClient, chaos_stack: ChaosStack) -> None:
    chaos_stack.redis_proxy.disable()
    assert (await _post(client, 1)).status_code == 503

    chaos_stack.redis_proxy.enable()
    assert (await _post(client, 2)).status_code == 202
    assert await _incident_count(chaos_stack.pg_dsn) == 1


@pytest.mark.asyncio
async def test_redis_latency_does_not_drop_events(
    client: AsyncClient, chaos_stack: ChaosStack
) -> None:
    # Inject 500ms of downstream latency on every Redis op.
    chaos_stack.redis_proxy.add_toxic(type="latency", attributes={"latency": 500})
    t0 = perf_counter()
    r = await _post(client, 1)
    elapsed = perf_counter() - t0

    assert r.status_code == 202
    # Prove the request actually traversed the laggy Redis (not bypassed via a cached
    # connection or skipped path): the injected 500ms toxic must surface in the request
    # time. Without this, the test would pass even if Redis weren't on the path at all.
    assert elapsed >= 0.4, f"request took only {elapsed:.3f}s — latency toxic not traversed"
    assert await _incident_count(chaos_stack.pg_dsn) == 1
