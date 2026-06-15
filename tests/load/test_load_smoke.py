# tests/load/test_load_smoke.py
"""Load smoke — concurrent ingestion invariants, CI-affordable and creditless.

This is the in-process invariant check: it fires a concurrent burst of unique,
HMAC-signed `generic` webhooks at the real app (via ASGITransport, consumers
disabled) and asserts the ingestion contract:

  1. zero dropped events — every accepted webhook produced exactly one incident
  2. no error responses
  3. p95 ingestion latency under a (generous) bound

The *throughput* number (100 req/s sustained) is measured separately by the
real locust scenario in ``locustfile.py`` via ``make load`` against a compose
stack — locust drives real sockets, which an in-process ASGI transport can't.

Spec note: spec §Testing asks for "p95 diagnosis latency < 10s". We exclude the
diagnosis path to stay creditless, so we assert p95 *ingestion* latency instead.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from time import perf_counter

import pytest
from httpx import AsyncClient
from sentinel.integrations.base import compute_hmac_sha256
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests.load.conftest import GENERIC_SECRET

pytestmark = pytest.mark.load

N_WEBHOOKS = 200
MAX_CONCURRENCY = 50
P95_LATENCY_BUDGET_S = 2.0  # generous: testcontainers + in-process transport


def _signed(i: int) -> tuple[bytes, dict[str, str]]:
    body = json.dumps(
        {
            "id": f"load-{i}",
            "service": f"svc-{i}",  # unique service → unique fingerprint → 1 incident
            "severity": "SEV2",
            "title": f"synthetic load alert {i}",
        }
    ).encode()
    sig = "sha256=" + compute_hmac_sha256(body, GENERIC_SECRET.encode())
    return body, {"X-Sentinel-Signature": sig, "Content-Type": "application/json"}


@pytest.mark.asyncio
async def test_ingestion_load_smoke(client: AsyncClient, pg_dsn: str) -> None:
    # Loud guard: this test must never touch the Anthropic path.
    assert os.environ["SENTINEL_DIAGNOSIS_CONSUMER_ENABLED"] == "false"
    assert os.environ["SENTINEL_MEMORY_CONSUMER_ENABLED"] == "false"

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def fire(i: int) -> tuple[int, float]:
        body, headers = _signed(i)
        async with sem:
            t0 = perf_counter()
            r = await client.post("/webhooks/generic", content=body, headers=headers)
            return r.status_code, perf_counter() - t0

    results = await asyncio.gather(*[fire(i) for i in range(N_WEBHOOKS)])
    statuses = Counter(s for s, _ in results)
    latencies = sorted(lat for _, lat in results)

    # (2) no error responses — every webhook accepted (202; unique bodies → no dupes)
    assert set(statuses) == {202}, f"unexpected statuses: {dict(statuses)}"

    # (3) p95 ingestion latency under budget (report the real number on failure)
    p95 = latencies[int(0.95 * (len(latencies) - 1))]
    assert p95 < P95_LATENCY_BUDGET_S, f"p95 ingestion latency {p95:.3f}s exceeds budget"

    # (1) zero dropped events — every unique webhook persisted exactly one incident.
    # The incident row is the durable artifact; its incident.opened outbox event is
    # written in the same transaction (ADR 0001), so incidents == N proves no
    # accepted webhook was lost. (We assert on incidents, not outbox rows, because
    # the outbox drainer concurrently marks rows published.)
    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            incidents = (await conn.execute(text("SELECT COUNT(*) FROM incidents"))).scalar_one()
    finally:
        await engine.dispose()

    assert incidents == N_WEBHOOKS, f"expected {N_WEBHOOKS} incidents, got {incidents}"
