"""Incident routes — list/detail/create against a fake repository."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.incidents import router as incidents_router
from sentinel.schemas.api import IncidentListItem


def _app(repo: object) -> FastAPI:
    app = FastAPI()
    app.state.incident_repo = repo
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(incidents_router)
    return app


def _item() -> IncidentListItem:
    return IncidentListItem(
        id=uuid4(),
        service="checkout",
        severity="SEV1",
        status="open",
        title="500s spiking",
        opened_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
    )


def test_list_returns_envelope() -> None:
    item = _item()
    repo = type("R", (), {})()
    repo.list_incidents = AsyncMock(return_value=([item], 1))
    client = TestClient(_app(repo))
    resp = client.get("/incidents?service=checkout&severity=SEV1&limit=10&offset=0")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["total"] == 1
    assert data["limit"] == 10
    assert data["offset"] == 0
    assert data["items"][0]["service"] == "checkout"
    repo.list_incidents.assert_awaited_once_with(
        status=None, service="checkout", severity="SEV1", limit=10, offset=0
    )


def test_list_defaults_and_rejects_bad_limit() -> None:
    repo = type("R", (), {})()
    repo.list_incidents = AsyncMock(return_value=([], 0))
    client = TestClient(_app(repo))
    # default limit/offset when omitted
    ok = client.get("/incidents")
    assert ok.status_code == 200
    assert ok.json()["limit"] == 50
    assert ok.json()["offset"] == 0
    # limit below the allowed minimum is rejected by FastAPI validation
    bad = client.get("/incidents?limit=0")
    assert bad.status_code == 422
