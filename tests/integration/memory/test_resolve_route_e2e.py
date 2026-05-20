# tests/integration/memory/test_resolve_route_e2e.py
"""POST /incidents/{id}/resolve against a real app + Postgres.

Builds a MINIMAL FastAPI app inline (just the resolve route + state).
Does NOT use sentinel.api.app.build_app() — its lifespan boots Kafka,
Anthropic, Redis. Route-level E2E only needs the route + state.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sentinel.api.routes.resolve import router as resolve_router
from sentinel.persistence.repositories import PostgresResolutionRepository
from sqlalchemy import text

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest_asyncio.fixture()
async def app_client(session_factory) -> AsyncIterator[AsyncClient]:  # type: ignore[no-untyped-def]
    app = FastAPI()
    app.state.resolution_repo = PostgresResolutionRepository(session_factory)
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(resolve_router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def _insert_open_incident(session_factory) -> str:  # type: ignore[no-untyped-def]
    async with session_factory() as s:
        rid = (
            await s.execute(
                text(
                    "INSERT INTO incidents (external_id, source, service, severity, title, "
                    "fingerprint, raw_payload, status) "
                    "VALUES (:eid, 'generic', 'api', 'SEV3', 't', :fp, "
                    "        CAST('{}' AS jsonb), 'open') RETURNING id"
                ),
                {"eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}"},
            )
        ).scalar_one()
        await s.commit()
    return str(rid)


def _body() -> dict[str, str | bool]:
    return {
        "root_cause": "rc",
        "remediation": "rm",
        "category": "data",
        "diagnosis_was_correct": True,
    }


async def test_resolve_success_then_409_on_replay(  # type: ignore[no-untyped-def]
    app_client: AsyncClient,
    session_factory,
) -> None:
    incident_id = await _insert_open_incident(session_factory)
    r1 = await app_client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert r1.status_code == 200, r1.text
    data = r1.json()
    assert data["status"] == "resolved"
    assert data["incident_id"] == incident_id

    async with session_factory() as s:
        event_val = (
            await s.execute(
                text(
                    "SELECT payload->>'event' FROM outbox_events "
                    "WHERE payload->>'incident_id' = :id"
                ),
                {"id": incident_id},
            )
        ).scalar_one()
    assert event_val == "incident.resolved"

    # Second call returns 409.
    r2 = await app_client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert r2.status_code == 409


async def test_resolve_404_for_missing_incident(app_client: AsyncClient) -> None:
    r = await app_client.post(f"/incidents/{uuid4()}/resolve", json=_body())
    assert r.status_code == 404
