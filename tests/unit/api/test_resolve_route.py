# tests/unit/api/test_resolve_route.py
"""Resolve route — exception mapping and happy path."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.resolve import router as resolve_router
from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import ResolveRecordResult


def _app(repo: object) -> FastAPI:
    app = FastAPI()
    app.state.resolution_repo = repo
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(resolve_router)
    return app


def _body() -> dict[str, str]:
    return {
        "root_cause": "rc",
        "remediation": "rm",
        "category": "data",
    }


def test_success_returns_200_with_response_schema() -> None:
    incident_id = uuid4()
    event_id = uuid4()
    now = datetime.now(UTC)
    repo = type("R", (), {})()
    repo.record = AsyncMock(
        return_value=ResolveRecordResult(
            incident_id=incident_id,
            resolved_at=now,
            event_id=event_id,
        )
    )
    client = TestClient(_app(repo))
    resp = client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["incident_id"] == str(incident_id)
    assert data["status"] == "resolved"
    assert data["event_id"] == str(event_id)


def test_returns_404_when_incident_missing() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.record = AsyncMock(side_effect=IncidentNotFound(incident_id))
    client = TestClient(_app(repo))
    resp = client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert resp.status_code == 404
    assert resp.json()["detail"] == "incident_not_found"


def test_returns_409_when_already_resolved() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.record = AsyncMock(side_effect=IncidentAlreadyResolved(incident_id))
    client = TestClient(_app(repo))
    resp = client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert resp.status_code == 409
    assert resp.json()["detail"] == "incident_already_resolved"


def test_invalid_payload_returns_422() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.record = AsyncMock()
    client = TestClient(_app(repo))
    resp = client.post(
        f"/incidents/{incident_id}/resolve",
        json={"root_cause": "x", "remediation": "y"},
    )
    assert resp.status_code == 422
