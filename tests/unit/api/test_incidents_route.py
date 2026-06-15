"""Incident routes — list/detail/create against a fake repository."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.incidents import router as incidents_router
from sentinel.schemas.api import IncidentDetailResponse, IncidentListItem

from tests.unit.diagnosis.fakes import make_context


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


def _detail(incident_id: UUID) -> IncidentDetailResponse:
    return IncidentDetailResponse(
        id=incident_id,
        source="sentry",
        external_id="ext-1",
        service="checkout",
        severity="SEV1",
        status="open",
        title="500s spiking",
        fingerprint="f" * 64,
        opened_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
    )


def test_detail_returns_incident_with_no_context() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    client = TestClient(_app(repo))
    resp = client.get(f"/incidents/{incident_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["id"] == str(incident_id)
    assert data["context"] is None
    assert data["diagnoses"] == []


def test_detail_returns_404_when_missing() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=None)
    repo.get_enrichment_context = AsyncMock(return_value=None)
    client = TestClient(_app(repo))
    resp = client.get(f"/incidents/{incident_id}")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "incident_not_found"
    # 404 short-circuits before any enrichment fetch — a behavioral guarantee.
    repo.get_enrichment_context.assert_not_called()


def test_detail_folds_in_enrichment_context() -> None:
    incident_id = uuid4()
    ctx = make_context(incident_id=incident_id)
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=SimpleNamespace(context=ctx))
    client = TestClient(_app(repo))
    resp = client.get(f"/incidents/{incident_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["context"] is not None
    assert data["context"]["incident_id"] == str(incident_id)
