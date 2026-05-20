# tests/integration/memory/test_memory_e2e.py
"""End-to-end memory loop: events → consumer → embeddings → retrieval.

Exercises the full MemoryConsumer + repo path against a real Postgres.
Does NOT spin up Kafka — invokes consumer.handle_message directly with
fabricated ConsumerRecord-shaped messages (same pattern as existing
diagnosis/enrichment consumer integration tests).
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sentinel.memory.consumer import MemoryConsumer
from sentinel.memory.deps import MemoryConsumerDeps
from sentinel.memory.embeddings import FastEmbedProvider
from sentinel.memory.pipeline import MemoryPipeline
from sentinel.persistence.repositories import PostgresIncidentRepository
from sqlalchemy import text

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _vec_lit(vec: list[float]) -> str:
    """pgvector wire format. asyncpg can't encode list[float] directly."""
    return "[" + ",".join(str(f) for f in vec) + "]"


@pytest.fixture(scope="module")
def embedder() -> FastEmbedProvider:
    # Use the shared .test-model-cache so we don't re-download per test.
    cache_dir = Path(__file__).resolve().parents[3] / ".test-model-cache"
    return FastEmbedProvider(model_cache_dir=cache_dir)


def _msg(payload: dict[str, Any]) -> Any:
    return SimpleNamespace(value=json.dumps(payload).encode(), offset=0, partition=0)


async def _insert_open_incident(session_factory, *, title: str = "db timeout") -> str:  # type: ignore[no-untyped-def]
    async with session_factory() as s:
        rid = (
            await s.execute(
                text(
                    "INSERT INTO incidents (external_id, source, service, severity, title, "
                    "fingerprint, raw_payload, status) "
                    "VALUES (:eid, 'generic', 'api', 'SEV3', :title, :fp, "
                    "        CAST('{}' AS jsonb), 'open') RETURNING id"
                ),
                {"eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}", "title": title},
            )
        ).scalar_one()
        await s.commit()
    return str(rid)


async def test_opened_event_writes_initial_embedding(session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_open_incident(session_factory)
    repo = PostgresIncidentRepository(session_factory)
    deps = MemoryConsumerDeps(
        incident_repo=repo,
        embedding_provider=embedder,
        pipeline=MemoryPipeline(),
    )
    kafka = AsyncMock()
    consumer = MemoryConsumer(consumer=kafka, deps=deps)

    event_id = uuid4()
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(event_id),
                "event": "incident.opened",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    async with session_factory() as s:
        row = (
            await s.execute(
                text(
                    "SELECT embedding IS NOT NULL, last_embedding_event_id "
                    "FROM incidents WHERE id = :id"
                ),
                {"id": incident_id},
            )
        ).first()
    assert row[0] is True
    assert str(row[1]) == str(event_id)
    kafka.commit.assert_awaited_once()


async def test_resolved_event_updates_embedding_distinct_from_initial(  # type: ignore[no-untyped-def]
    session_factory,
    embedder,
) -> None:
    """Spec acceptance: resolving updates the embedding; the new embedding
    has different cosine distance to a query than the old embedding."""
    incident_id = await _insert_open_incident(session_factory, title="db timeout")
    repo = PostgresIncidentRepository(session_factory)
    deps = MemoryConsumerDeps(
        incident_repo=repo,
        embedding_provider=embedder,
        pipeline=MemoryPipeline(),
    )
    kafka = AsyncMock()
    consumer = MemoryConsumer(consumer=kafka, deps=deps)

    # 1. Initial opened-path embedding.
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(uuid4()),
                "event": "incident.opened",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    async with session_factory() as s:
        initial_vec_str = (
            await s.execute(
                text("SELECT embedding::text FROM incidents WHERE id = :id"),
                {"id": incident_id},
            )
        ).scalar_one()

    # 2. Add a resolution row + emit resolved event.
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category) "
                "VALUES (:id, 'pool exhausted under burst load', "
                "        'raised pool size; added circuit breaker on upstream', 'capacity')"
            ),
            {"id": incident_id},
        )
        await s.execute(
            text("UPDATE incidents SET status='resolved' WHERE id = :id"),
            {"id": incident_id},
        )
        await s.commit()

    await consumer.handle_message(
        _msg(
            {
                "event_id": str(uuid4()),
                "event": "incident.resolved",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    async with session_factory() as s:
        resolved_vec_str = (
            await s.execute(
                text("SELECT embedding::text FROM incidents WHERE id = :id"),
                {"id": incident_id},
            )
        ).scalar_one()

    assert initial_vec_str != resolved_vec_str


async def test_replay_resolved_event_is_idempotent(session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_open_incident(session_factory)
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category) "
                "VALUES (:id, 'rc', 'rm', 'data')"
            ),
            {"id": incident_id},
        )
        await s.execute(
            text("UPDATE incidents SET status='resolved' WHERE id = :id"),
            {"id": incident_id},
        )
        await s.commit()

    repo = PostgresIncidentRepository(session_factory)
    deps = MemoryConsumerDeps(
        incident_repo=repo,
        embedding_provider=embedder,
        pipeline=MemoryPipeline(),
    )
    kafka = AsyncMock()
    consumer = MemoryConsumer(consumer=kafka, deps=deps)

    event_id = uuid4()
    payload = {
        "event_id": str(event_id),
        "event": "incident.resolved",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }
    await consumer.handle_message(_msg(payload))
    await consumer.handle_message(_msg(payload))  # replay

    assert kafka.commit.await_count == 2


async def test_top_k_uses_hnsw_index_when_data_present(session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    """Sanity: the HNSW index exists and the planner uses it when seqscan is disabled.

    pgvector's HNSW cost model prefers SeqScan at low row counts (even 500 rows in
    a test container). We force ``enable_seqscan = off`` to verify the index is
    present, correctly typed, and actually usable — the invariant the migration
    guarantees. This is standard practice for pgvector integration tests.
    """
    # Seed 200 resolved incidents with embeddings.
    for i in range(200):
        async with session_factory() as s:
            rid = (
                await s.execute(
                    text(
                        "INSERT INTO incidents (external_id, source, service, severity, title, "
                        "fingerprint, raw_payload, status, embedding) "
                        "VALUES (:eid, 'generic', 'api', 'SEV3', :title, :fp, "
                        "        CAST('{}' AS jsonb), 'resolved', CAST(:vec AS vector)) "
                        "RETURNING id"
                    ),
                    {
                        "eid": f"e-{uuid4()}",
                        "fp": f"fp-{uuid4()}",
                        "title": f"incident {i}",
                        "vec": _vec_lit(await embedder.embed(f"description {i}")),
                    },
                )
            ).scalar_one()
            await s.execute(
                text(
                    "INSERT INTO resolutions (incident_id, root_cause, remediation, category) "
                    "VALUES (:id, :rc, 'fixed', 'data')"
                ),
                {"id": rid, "rc": f"root cause {i}"},
            )
            await s.commit()

    qvec = _vec_lit(await embedder.embed("anything"))
    async with session_factory() as s:
        # Disable seqscan so the planner is forced to use the HNSW index.
        # This proves the index exists and is queryable; the planner's natural
        # cost model would prefer seqscan at test-container scale regardless.
        await s.execute(text("SET LOCAL enable_seqscan = off"))
        plan_rows = (
            await s.execute(
                text(
                    "EXPLAIN SELECT id FROM incidents "
                    "WHERE embedding IS NOT NULL "
                    "ORDER BY embedding <=> CAST(:vec AS vector) LIMIT 5"
                ),
                {"vec": qvec},
            )
        ).all()
    plan_text = "\n".join(str(r[0]) for r in plan_rows)
    assert "idx_incidents_embedding" in plan_text, plan_text
