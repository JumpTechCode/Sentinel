# tests/integration/memory/test_incident_repo_memory.py
"""Integration tests for IncidentRepository.set_embedding and load_for_memory."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sentinel.persistence.repositories import (
    MemoryIncidentRow,
    PostgresIncidentRepository,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture
def incident_repo(session_factory) -> PostgresIncidentRepository:  # type: ignore[no-untyped-def]
    return PostgresIncidentRepository(session_factory)


async def _insert_incident(session_factory, **kwargs) -> str:  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    defaults = {
        "external_id": "ext-1",
        "source": "generic",
        "service": "api",
        "severity": "SEV3",
        "title": "test incident",
        "fingerprint": "fp1",
        "raw_payload": '{"_sentinel": {"occurrence_count": 1}}',
    }
    defaults.update(kwargs)
    async with session_factory() as s:
        row = (
            await s.execute(
                text(
                    "INSERT INTO incidents (external_id, source, service, severity, title, "
                    "fingerprint, raw_payload) "
                    "VALUES (:external_id, :source, :service, :severity, :title, "
                    ":fingerprint, CAST(:raw_payload AS jsonb)) RETURNING id"
                ),
                defaults,
            )
        ).scalar_one()
        await s.commit()
    return str(row)


async def test_set_embedding_first_call_writes(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_incident(session_factory)
    event_id = uuid4()
    vec = [0.1] * 1024
    result = await incident_repo.set_embedding(incident_id, vec, event_id=event_id)
    assert result == "written"


async def test_set_embedding_duplicate_event_id_is_noop(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_incident(session_factory)
    event_id = uuid4()
    vec = [0.1] * 1024
    first = await incident_repo.set_embedding(incident_id, vec, event_id=event_id)
    second = await incident_repo.set_embedding(incident_id, vec, event_id=event_id)
    assert first == "written"
    assert second == "duplicate"


async def test_set_embedding_unknown_incident(incident_repo) -> None:  # type: ignore[no-untyped-def]
    result = await incident_repo.set_embedding(uuid4(), [0.0] * 1024, event_id=uuid4())
    assert result == "unknown_incident"


async def test_load_for_memory_returns_none_for_missing(incident_repo) -> None:  # type: ignore[no-untyped-def]
    assert await incident_repo.load_for_memory(uuid4()) is None


async def test_load_for_memory_no_resolution(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_incident(session_factory, service="svc", title="hello")
    row = await incident_repo.load_for_memory(incident_id)
    assert isinstance(row, MemoryIncidentRow)
    assert row.service == "svc"
    assert row.title == "hello"
    assert row.resolution is None


async def test_load_for_memory_with_resolution(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    from sqlalchemy import text

    incident_id = await _insert_incident(session_factory, service="db", title="deadlock")
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category, "
                "diagnosis_was_correct) "
                "VALUES (:id, 'long tx', 'killed', 'data', TRUE)"
            ),
            {"id": incident_id},
        )
        await s.commit()
    row = await incident_repo.load_for_memory(incident_id)
    assert row is not None
    assert row.resolution is not None
    assert row.resolution.root_cause == "long tx"
    assert row.resolution.remediation == "killed"
    assert row.resolution.diagnosis_was_correct is True
