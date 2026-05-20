# tests/integration/memory/test_store_e2e.py
"""PgVectorIncidentStore against a real Postgres with pgvector + HNSW."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sentinel.memory.embeddings import FastEmbedProvider
from sentinel.memory.store import PgVectorIncidentStore
from sqlalchemy import text

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _vector_literal(vec: list[float]) -> str:
    """Serialize a list of floats to pgvector's wire format for test seed queries."""
    return "[" + ",".join(str(f) for f in vec) + "]"


@pytest.fixture(scope="module")
def embedder() -> FastEmbedProvider:
    # Use the same .test-model-cache as test_embeddings.py to avoid re-downloading.
    cache_dir = Path(__file__).resolve().parents[3] / ".test-model-cache"
    return FastEmbedProvider(model_cache_dir=cache_dir)


@pytest.fixture
def store(session_factory, embedder) -> PgVectorIncidentStore:  # type: ignore[no-untyped-def]
    from sentinel.persistence.repositories import PostgresIncidentRepository

    repo = PostgresIncidentRepository(session_factory)
    return PgVectorIncidentStore(incident_repo=repo, embedding_provider=embedder)


async def _seed_resolved_incident(  # type: ignore[no-untyped-def]
    session_factory,
    embedder,
    *,
    service: str,
    title: str,
    root_cause: str,
    remediation: str,
    status: str = "resolved",
    diagnosis_was_correct: bool | None = True,
) -> str:
    vec = await embedder.embed(f"{title}\n{root_cause}\n{remediation}")
    async with session_factory() as s:
        incident_id = (
            await s.execute(
                text(
                    "INSERT INTO incidents (external_id, source, service, severity, title, "
                    "fingerprint, raw_payload, status, embedding) "
                    "VALUES (:eid, 'generic', :service, 'SEV3', :title, :fp, "
                    "        CAST('{}' AS jsonb), :status, CAST(:vec AS vector)) "
                    "RETURNING id"
                ),
                {
                    "eid": f"e-{uuid4()}",
                    "service": service,
                    "title": title,
                    "fp": f"fp-{uuid4()}",
                    "status": status,
                    "vec": _vector_literal(vec),
                },
            )
        ).scalar_one()
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category, "
                "diagnosis_was_correct) "
                "VALUES (:id, :rc, :rm, 'data', :dc)"
            ),
            {"id": incident_id, "rc": root_cause, "rm": remediation, "dc": diagnosis_was_correct},
        )
        await s.commit()
    return str(incident_id)


async def test_top_k_filters_unresolved_incidents(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    await _seed_resolved_incident(
        session_factory,
        embedder,
        service="api",
        title="db connection timeout",
        root_cause="pool exhaustion",
        remediation="raised pool size",
    )
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO incidents (external_id, source, service, severity, title, "
                "fingerprint, raw_payload, status, embedding) "
                "VALUES (:eid, 'generic', 'api', 'SEV3', 'unrelated', :fp, "
                "        CAST('{}' AS jsonb), 'open', CAST(:vec AS vector))"
            ),
            {
                "eid": f"e-{uuid4()}",
                "fp": f"fp-{uuid4()}",
                "vec": _vector_literal(await embedder.embed("unrelated text")),
            },
        )
        await s.commit()

    result = await store.top_k(
        query_text="connection pool exhaustion", k=5, exclude_incident_id=None
    )
    assert result.status == "ok"
    titles = {item.title for item in result.data}
    assert "db connection timeout" in titles
    assert "unrelated" not in titles


async def test_top_k_excludes_diagnosis_was_correct_false(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    await _seed_resolved_incident(
        session_factory,
        embedder,
        service="api",
        title="should be excluded",
        root_cause="x",
        remediation="y",
        diagnosis_was_correct=False,
    )
    await _seed_resolved_incident(
        session_factory,
        embedder,
        service="api",
        title="should be included (null)",
        root_cause="x",
        remediation="y",
        diagnosis_was_correct=None,
    )
    await _seed_resolved_incident(
        session_factory,
        embedder,
        service="api",
        title="should be included (true)",
        root_cause="x",
        remediation="y",
        diagnosis_was_correct=True,
    )
    result = await store.top_k(query_text="anything", k=10, exclude_incident_id=None)
    titles = {item.title for item in result.data}
    assert "should be excluded" not in titles
    assert "should be included (null)" in titles
    assert "should be included (true)" in titles


async def test_top_k_excludes_query_incident(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    excluded = await _seed_resolved_incident(
        session_factory,
        embedder,
        service="api",
        title="exclude me",
        root_cause="x",
        remediation="y",
    )
    await _seed_resolved_incident(
        session_factory,
        embedder,
        service="api",
        title="keep me",
        root_cause="x",
        remediation="y",
    )
    result = await store.top_k(
        query_text="anything",
        k=10,
        exclude_incident_id=UUID(excluded),
    )
    titles = {item.title for item in result.data}
    assert "exclude me" not in titles
    assert "keep me" in titles


async def test_top_k_returns_id_in_similar_prefix(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    await _seed_resolved_incident(
        session_factory,
        embedder,
        service="api",
        title="t",
        root_cause="x",
        remediation="y",
    )
    result = await store.top_k(query_text="anything", k=5, exclude_incident_id=None)
    assert result.status == "ok"
    assert result.data, "expected at least one result"
    for item in result.data:
        assert item.id.startswith("similar:"), item.id
        assert -1.0 <= item.cosine_similarity <= 1.0
