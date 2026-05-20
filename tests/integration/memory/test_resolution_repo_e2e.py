# tests/integration/memory/test_resolution_repo_e2e.py
"""PostgresResolutionRepository against a real Postgres."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import PostgresResolutionRepository
from sentinel.schemas.api import ResolveIncidentRequest
from sqlalchemy import text

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture
def resolution_repo(session_factory) -> PostgresResolutionRepository:  # type: ignore[no-untyped-def]
    return PostgresResolutionRepository(session_factory)


def _body() -> ResolveIncidentRequest:
    return ResolveIncidentRequest(
        root_cause="db deadlock during migration",
        remediation="killed migration; ran online schema change instead",
        category="data",
        diagnosis_was_correct=True,
        notes="see #1234",
        resolved_by="oncall@example.com",
    )


async def _insert_open_incident(session_factory: Any) -> UUID:
    async with session_factory() as s:
        rid = (
            await s.execute(
                text(
                    "INSERT INTO incidents (external_id, source, service, severity, title, "
                    "fingerprint, raw_payload, status) "
                    "VALUES (:eid, 'generic', 'api', 'SEV3', 'test', :fp, "
                    "CAST('{}' AS jsonb), 'open') RETURNING id"
                ),
                {"eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}"},
            )
        ).scalar_one()
        await s.commit()
    return UUID(str(rid))


async def test_record_success_returns_event_id_and_writes_all_three(
    resolution_repo: PostgresResolutionRepository, session_factory: Any
) -> None:
    incident_id = await _insert_open_incident(session_factory)
    result = await resolution_repo.record(incident_id, _body(), outbox_topic="sentinel.incidents")
    assert result.incident_id == incident_id
    assert result.event_id is not None
    assert result.resolved_at is not None

    async with session_factory() as s:
        res_row = (
            await s.execute(
                text(
                    "SELECT root_cause, remediation, category FROM resolutions "
                    "WHERE incident_id = :id"
                ),
                {"id": incident_id},
            )
        ).first()
        assert res_row is not None
        assert res_row[0] == "db deadlock during migration"

        inc_row = (
            await s.execute(
                text("SELECT status, resolved_at FROM incidents WHERE id = :id"),
                {"id": incident_id},
            )
        ).first()
        assert inc_row is not None
        assert inc_row[0] == "resolved"
        assert inc_row[1] is not None

        out_row = (
            await s.execute(
                text("SELECT topic, payload FROM outbox_events WHERE id = :id"),
                {"id": result.event_id},
            )
        ).first()
        assert out_row is not None
        assert out_row[0] == "sentinel.incidents"
        assert out_row[1]["event"] == "incident.resolved"
        assert out_row[1]["event_id"] == str(result.event_id)


async def test_record_raises_for_missing_incident(
    resolution_repo: PostgresResolutionRepository,
) -> None:
    with pytest.raises(IncidentNotFound):
        await resolution_repo.record(uuid4(), _body(), outbox_topic="sentinel.incidents")


async def test_record_raises_for_already_resolved(
    resolution_repo: PostgresResolutionRepository, session_factory: Any
) -> None:
    incident_id = await _insert_open_incident(session_factory)
    await resolution_repo.record(incident_id, _body(), outbox_topic="sentinel.incidents")
    with pytest.raises(IncidentAlreadyResolved):
        await resolution_repo.record(incident_id, _body(), outbox_topic="sentinel.incidents")
