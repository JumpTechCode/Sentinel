# tests/unit/schemas/test_api.py
"""Wire shapes for the API endpoints landing in Work Area I."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sentinel.schemas.api import (
    CreateIncidentRequest,
    DiagnoseResponse,
    DiagnosisView,
    EvalRunSummary,
    HealthResponse,
    IncidentDetailResponse,
    IncidentListItem,
    IncidentListResponse,
    ResolveIncidentRequest,
    WebhookAcceptedResponse,
)
from sentinel.schemas.diagnosis import Diagnosis, EvidenceRef, SuggestedAction


def test_resolve_request_round_trip() -> None:
    body = {
        "root_cause": "BGP misconfig",
        "remediation": "revert change",
        "category": "config",
        "diagnosis_was_correct": True,
        "notes": "see incident #42",
    }
    parsed = ResolveIncidentRequest.model_validate(body)
    assert parsed.category == "config"
    assert parsed.diagnosis_was_correct is True


def test_resolve_request_rejects_bad_category() -> None:
    with pytest.raises(ValidationError):
        ResolveIncidentRequest(
            root_cause="x",
            remediation="y",
            category="not-a-category",
            diagnosis_was_correct=True,
        )


def test_create_incident_request() -> None:
    r = CreateIncidentRequest(
        source="generic",
        external_id="e-1",
        service="x",
        severity="SEV3",
        title="x",
        raw_payload={"k": "v"},
    )
    assert r.source == "generic"


def test_incident_list_item() -> None:
    i = IncidentListItem(
        id=uuid4(),
        service="x",
        severity="SEV2",
        status="open",
        title="t",
        opened_at=datetime(2026, 5, 18, tzinfo=UTC),
    )
    assert i.status == "open"


def test_health_response() -> None:
    h = HealthResponse(status="ok", checks={"db": "ok", "redis": "ok", "kafka": "ok"})
    assert h.status == "ok"


def test_eval_run_summary() -> None:
    s = EvalRunSummary(
        id=uuid4(),
        started_at=datetime(2026, 5, 18, tzinfo=UTC),
        completed_at=None,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        corpus_version="2026-05-18",
        summary=None,
    )
    assert s.completed_at is None


# --- helpers ---------------------------------------------------------------- #


def _valid_diagnosis() -> Diagnosis:
    return Diagnosis(
        hypothesis="bad deploy abc123 broke checkout",
        confidence=0.78,
        reasoning="recent deploy at 06:25 immediately preceded errors",
        evidence=[EvidenceRef(kind="deploy", id="deploy:abc123", note="deploy at fault")],
        suggested_actions=[
            SuggestedAction(
                description="Roll back deploy",
                risk="medium",
                rationale="recent change suspected",
            )
        ],
        likely_category="deploy",
    )


def _valid_diagnosis_view(*, hallucinated_evidence: bool = False) -> DiagnosisView:
    return DiagnosisView(
        hypothesis="bad deploy abc123 broke checkout",
        confidence=0.78,
        reasoning="recent deploy at 06:25 immediately preceded errors",
        evidence=[EvidenceRef(kind="deploy", id="deploy:abc123", note="deploy at fault")],
        suggested_actions=[
            SuggestedAction(
                description="Roll back deploy",
                risk="medium",
                rationale="recent change suspected",
            )
        ],
        likely_category="deploy",
        hallucinated_evidence=hallucinated_evidence,
        model="claude-sonnet-4-5",
        prompt_version="v1",
    )


# --- DiagnoseResponse ------------------------------------------------------- #


def test_diagnose_response_hallucinated_false() -> None:
    resp = DiagnoseResponse(
        incident_id=uuid4(),
        diagnosis=_valid_diagnosis_view(hallucinated_evidence=False),
        persisted="new",
    )
    assert resp.diagnosis.hallucinated_evidence is False


def test_diagnose_response_hallucinated_true() -> None:
    resp = DiagnoseResponse(
        incident_id=uuid4(),
        diagnosis=_valid_diagnosis_view(hallucinated_evidence=True),
        persisted="duplicate",
    )
    assert resp.diagnosis.hallucinated_evidence is True


def test_diagnose_response_round_trip() -> None:
    resp = DiagnoseResponse(
        incident_id=uuid4(),
        diagnosis=_valid_diagnosis_view(),
        persisted="new",
    )
    assert DiagnoseResponse.model_validate_json(resp.model_dump_json()) == resp


# --- IncidentDetailResponse ------------------------------------------------- #


def test_incident_detail_response_minimal() -> None:
    """context=None and diagnoses=[] is the default-ish case for a fresh incident."""
    resp = IncidentDetailResponse(
        id=uuid4(),
        source="sentry",
        external_id="evt-abc-123",
        service="checkout-api",
        severity="SEV2",
        status="open",
        title="Elevated 5xx on /checkout",
        fingerprint="deadbeef" * 8,
        opened_at=datetime(2026, 5, 18, 18, 0, tzinfo=UTC),
        resolved_at=None,
        context=None,
        diagnoses=[],
    )
    assert resp.context is None
    assert resp.diagnoses == []


def test_incident_detail_response_round_trip() -> None:
    resp = IncidentDetailResponse(
        id=uuid4(),
        source="generic",
        external_id="e-1",
        service="payments",
        severity="SEV1",
        status="diagnosing",
        title="DB connection pool exhausted",
        fingerprint="cafebabe" * 8,
        opened_at=datetime(2026, 5, 18, 12, 0, tzinfo=UTC),
        context=None,
        diagnoses=[],
    )
    assert IncidentDetailResponse.model_validate_json(resp.model_dump_json()) == resp


# --- WebhookAcceptedResponse ------------------------------------------------ #


def test_webhook_accepted_response_allows_recurred() -> None:
    resp = WebhookAcceptedResponse(status="recurred", incident_id=None)
    assert resp.status == "recurred"


def test_webhook_accepted_response_rejects_unknown_status() -> None:
    with pytest.raises(ValidationError):
        WebhookAcceptedResponse.model_validate({"status": "something_else", "incident_id": None})


def test_incident_list_response_round_trips() -> None:
    item = IncidentListItem(
        id=uuid4(),
        service="checkout",
        severity="SEV1",
        status="open",
        title="500s spiking",
        opened_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
    )
    resp = IncidentListResponse(items=[item], total=1, limit=50, offset=0)
    dumped = resp.model_dump(mode="json")
    assert dumped["total"] == 1
    assert dumped["limit"] == 50
    assert dumped["offset"] == 0
    assert len(dumped["items"]) == 1


def test_incident_detail_diagnoses_uses_view_and_allows_empty_evidence() -> None:
    view = DiagnosisView(
        hypothesis="h",
        confidence=0.4,
        reasoning="r",
        evidence=[],  # empty — only DiagnosisView permits this
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=True,
        model="claude-sonnet-4-5",
        prompt_version="v1",
    )
    detail = IncidentDetailResponse(
        id=uuid4(),
        source="sentry",
        external_id="e",
        service="s",
        severity="SEV1",
        status="diagnosing",
        title="t",
        fingerprint="f" * 64,
        opened_at=datetime(2026, 6, 15, 12, 0, tzinfo=UTC),
        diagnoses=[view],
    )
    assert detail.diagnoses[0].hallucinated_evidence is True


def test_diagnose_response_carries_view_and_persisted_status() -> None:
    view = DiagnosisView(
        hypothesis="h",
        confidence=0.82,
        reasoning="r",
        evidence=[],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model="m",
        prompt_version="v1",
    )
    resp = DiagnoseResponse(incident_id=uuid4(), diagnosis=view, persisted="new")
    assert resp.persisted == "new"
    assert resp.diagnosis.confidence == 0.82
