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


def _create_body() -> dict[str, object]:
    return {
        "source": "generic",
        "external_id": "manual-1",
        "service": "checkout",
        "severity": "SEV2",
        "title": "manual incident",
        "raw_payload": {"note": "filed by operator"},
    }


def test_create_returns_201_for_new_incident() -> None:
    from sentinel.persistence.repositories import IngestResult

    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.ingest = AsyncMock(return_value=IngestResult(incident_id=incident_id, event_kind="opened"))
    client = TestClient(_app(repo))
    resp = client.post("/incidents", json=_create_body())
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["incident_id"] == str(incident_id)
    # Verify it went through the ingest path with a fingerprint + payload_hash.
    kwargs = repo.ingest.await_args.kwargs
    assert kwargs["outbox_topic"] == "sentinel.incidents"
    assert len(kwargs["fingerprint"]) == 64
    assert len(kwargs["payload_hash"]) == 64


def test_create_returns_200_for_recurred() -> None:
    from sentinel.persistence.repositories import IngestResult

    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.ingest = AsyncMock(
        return_value=IngestResult(incident_id=incident_id, event_kind="recurred")
    )
    client = TestClient(_app(repo))
    resp = client.post("/incidents", json=_create_body())
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "recurred"


def test_create_rejects_unknown_field() -> None:
    repo = type("R", (), {})()
    repo.ingest = AsyncMock()
    client = TestClient(_app(repo))
    body = _create_body() | {"bogus": 1}
    resp = client.post("/incidents", json=body)
    assert resp.status_code == 422  # CreateIncidentRequest has extra="forbid"


def test_create_increments_outbox_enqueue_metric() -> None:
    from prometheus_client import REGISTRY
    from sentinel.persistence.repositories import IngestResult

    def _count() -> float:
        return (
            REGISTRY.get_sample_value(
                "sentinel_outbox_events_enqueued_total", {"topic": "sentinel.incidents"}
            )
            or 0.0
        )

    before = _count()
    repo = type("R", (), {})()
    repo.ingest = AsyncMock(return_value=IngestResult(incident_id=uuid4(), event_kind="opened"))
    client = TestClient(_app(repo))
    resp = client.post("/incidents", json=_create_body())
    assert resp.status_code == 201, resp.text
    # The handler mirrors the webhook path's outbox-enqueue counter.
    assert _count() - before == 1.0


def test_detail_includes_latest_diagnosis() -> None:
    from decimal import Decimal

    from sentinel.diagnosis.persisted import PersistedDiagnosis
    from sentinel.schemas.diagnosis import EvidenceRef

    incident_id = uuid4()
    pd = PersistedDiagnosis(
        hypothesis="pool exhausted",
        confidence=Decimal("0.82"),
        reasoning="saturated after deploy",
        evidence=[EvidenceRef(kind="deploy", id="deploy:abc", note="10m before")],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        latency_ms=10,
        token_usage={},
    )
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    diag_repo = type("D", (), {})()
    diag_repo.get_by_incident_id = AsyncMock(return_value=pd)

    app = FastAPI()
    app.state.incident_repo = repo
    app.state.diagnosis_repo = diag_repo
    app.include_router(incidents_router)
    resp = TestClient(app).get(f"/incidents/{incident_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data["diagnoses"]) == 1
    assert data["diagnoses"][0]["hypothesis"] == "pool exhausted"
    assert data["diagnoses"][0]["hallucinated_evidence"] is False
    # Internal metrics are intentionally not part of the diagnosis read shape.
    assert "token_usage" not in data["diagnoses"][0]
    assert "latency_ms" not in data["diagnoses"][0]


def test_detail_empty_diagnoses_when_none() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    diag_repo = type("D", (), {})()
    diag_repo.get_by_incident_id = AsyncMock(return_value=None)

    app = FastAPI()
    app.state.incident_repo = repo
    app.state.diagnosis_repo = diag_repo
    app.include_router(incidents_router)
    resp = TestClient(app).get(f"/incidents/{incident_id}")
    assert resp.status_code == 200
    assert resp.json()["diagnoses"] == []


def test_detail_tolerates_missing_diagnosis_repo() -> None:
    # PR 1's _app(repo) helper sets only incident_repo; the handler must not 500.
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.get = AsyncMock(return_value=_detail(incident_id))
    repo.get_enrichment_context = AsyncMock(return_value=None)
    resp = TestClient(_app(repo)).get(f"/incidents/{incident_id}")
    assert resp.status_code == 200
    assert resp.json()["diagnoses"] == []
