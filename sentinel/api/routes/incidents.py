# sentinel/api/routes/incidents.py
"""Incident read/create/diagnose routes.

`GET /incidents`                — filtered, paginated list.
`GET /incidents/{id}`           — detail incl. enrichment context and latest diagnosis.
`POST /incidents`               — manual create through the fingerprint + ingest path.
`POST /incidents/{id}/diagnose` — replay-backed re-diagnosis; persists via outbox.

All handlers are thin: querying lives in `IncidentRepository` / `DiagnosisRepository`.
Repos are retrieved from `request.app.state`, matching the `resolve.py` /
`webhooks.py` dependency pattern.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from sentinel.diagnosis.agent import diagnose
from sentinel.diagnosis.errors import DiagnosisInvalid, LLMError
from sentinel.diagnosis.persisted import to_view
from sentinel.evals.cassette import CassetteContext, CassetteMiss
from sentinel.ingestion.fingerprint import fingerprint, normalize_title
from sentinel.observability.metrics import outbox_events_enqueued_total
from sentinel.persistence.repositories import IncidentRepository, OutboxEvent
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.api import (
    CreateIncidentRequest,
    DiagnoseResponse,
    IncidentDetailResponse,
    IncidentListResponse,
    WebhookAcceptedResponse,
)
from sentinel.schemas.enums import IncidentStatusType, SeverityType

router = APIRouter(tags=["incidents"])


@router.get(
    "/incidents",
    response_model=IncidentListResponse,
    responses={422: {"description": "Invalid query parameter"}},
)
async def list_incidents(
    request: Request,
    status: IncidentStatusType | None = Query(default=None),
    service: str | None = Query(default=None),
    severity: SeverityType | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> IncidentListResponse:
    repo: IncidentRepository = request.app.state.incident_repo
    items, total = await repo.list_incidents(
        status=status,
        service=service,
        severity=severity,
        limit=limit,
        offset=offset,
    )
    return IncidentListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get(
    "/incidents/{incident_id}",
    response_model=IncidentDetailResponse,
    responses={404: {"description": "Incident not found"}},
)
async def get_incident(incident_id: UUID, request: Request) -> IncidentDetailResponse:
    repo: IncidentRepository = request.app.state.incident_repo
    detail = await repo.get(incident_id)
    if detail is None:
        raise HTTPException(status_code=404, detail="incident_not_found")
    # Enrichment context is stored separately (JSONB read-back); fold it in.
    # Diagnoses are deferred to PR 2 — the wire `Diagnosis` model requires
    # evidence min_length=1, which a fully-hallucinated PersistedDiagnosis
    # cannot satisfy. PR 2 introduces the relaxed view model.
    stored = await repo.get_enrichment_context(incident_id)
    if stored is not None:
        detail = detail.model_copy(update={"context": stored.context})
    # Latest diagnosis (0 or 1) rendered via the relaxed view model.
    # diagnosis_repo is wired unconditionally in the production lifespan; the
    # getattr guard tolerates minimal test apps that omit it (→ "no diagnoses").
    diag_repo = getattr(request.app.state, "diagnosis_repo", None)
    if diag_repo is not None:
        persisted = await diag_repo.get_by_incident_id(incident_id)
        if persisted is not None:
            detail = detail.model_copy(update={"diagnoses": [to_view(persisted)]})
    return detail


@router.post(
    "/incidents",
    response_model=None,
    status_code=201,
    responses={
        200: {"model": WebhookAcceptedResponse, "description": "Recurred (deduped)"},
        201: {"model": WebhookAcceptedResponse, "description": "Created"},
        422: {"description": "Invalid payload"},
    },
)
async def create_incident(body: CreateIncidentRequest, request: Request) -> JSONResponse:
    repo: IncidentRepository = request.app.state.incident_repo
    outbox_topic: str = request.app.state.outbox_topic
    # Build the canonical alert from the validated request. received_at is set
    # server-side (manual creation has no wire timestamp).
    alert = NormalizedAlert(
        source=body.source,
        external_id=body.external_id,
        service=body.service,
        severity=body.severity,
        title=body.title,
        received_at=datetime.now(UTC),
        raw_payload=body.raw_payload,
    )
    # Same fingerprint contract as the webhook path: manual incidents dedup
    # against webhook incidents within the 1h window.
    fp = fingerprint(alert.service, normalize_title(alert.title), alert.severity)
    # Manual creation has no raw HTTP body; hash the canonical JSON payload so
    # the ingest contract (payload_hash) is satisfied deterministically.
    payload_hash = sha256(
        json.dumps(body.raw_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    # No Redis NX idempotency guard here (unlike the webhook path): there is no
    # raw delivery body to dedup retries on. Fingerprint dedup inside ingest()
    # is the idempotency mechanism for manual creation.
    result = await repo.ingest(
        alert,
        fingerprint=fp,
        outbox_topic=outbox_topic,
        payload_hash=payload_hash,
    )
    # Mirror the webhook handler so manual creates are counted in the same
    # outbox-enqueue throughput metric.
    outbox_events_enqueued_total.labels(topic=outbox_topic).inc()
    wire_status = "recurred" if result.event_kind == "recurred" else "accepted"
    status_code = 200 if result.event_kind == "recurred" else 201
    return JSONResponse(
        status_code=status_code,
        content=WebhookAcceptedResponse(
            status=wire_status, incident_id=result.incident_id
        ).model_dump(mode="json"),
    )


@router.post(
    "/incidents/{incident_id}/diagnose",
    response_model=DiagnoseResponse,
    responses={
        404: {"description": "Incident not found"},
        409: {"description": "Already resolved / no context / no recording"},
        422: {"description": "Diagnosis failed schema validation"},
        502: {"description": "LLM error"},
        503: {"description": "Diagnosis deps unavailable"},
    },
)
async def diagnose_incident(incident_id: UUID, request: Request) -> DiagnoseResponse:
    state = request.app.state
    deps = getattr(state, "diagnosis_deps", None)
    diag_repo = getattr(state, "diagnosis_repo", None)
    if deps is None or diag_repo is None:
        raise HTTPException(status_code=503, detail="diagnosis_unavailable")

    repo: IncidentRepository = state.incident_repo
    incident = await repo.get(incident_id)
    if incident is None:
        raise HTTPException(status_code=404, detail="incident_not_found")
    if incident.status == "resolved":
        raise HTTPException(status_code=409, detail="incident_already_resolved")
    stored = await repo.get_enrichment_context(incident_id)
    if stored is None:
        raise HTTPException(status_code=409, detail="context_not_assembled")

    # Cassette replay (eval/demo mode): key on this incident's id so an arbitrary
    # incident with no recording fails closed with 409 rather than a live call.
    cassette = getattr(state, "cassette_transport", None)
    if cassette is not None:
        cassette.set_context(
            CassetteContext(
                prompt_version=state.diagnosis_prompt_version,
                model_id=state.diagnosis_model,
                case_id=str(incident_id),
                shot_index=0,
            )
        )

    try:
        record = await diagnose(incident, stored.context, deps)
    except CassetteMiss as e:
        raise HTTPException(status_code=409, detail="no_recording_available") from e
    except DiagnosisInvalid as e:
        raise HTTPException(status_code=422, detail="diagnosis_schema_invalid") from e
    except LLMError as e:
        raise HTTPException(status_code=502, detail="diagnosis_llm_error") from e

    outbox_id = uuid4()
    outbox_event = OutboxEvent(
        id=outbox_id,
        topic=state.outbox_topic,
        key=str(incident_id),
        payload={
            "event_id": str(outbox_id),
            "event": "incident.diagnosed",
            "incident_id": str(incident_id),
            "ts": datetime.now(UTC).isoformat(),
        },
        attempts=0,
        created_at=datetime.now(UTC),
    )
    # upstream_event_id is synthesized for the manual trigger (no Kafka envelope).
    _diagnosis_id, persisted = await diag_repo.save_with_outbox(
        incident_id=incident_id,
        record=record,
        upstream_event_id=uuid4(),
        outbox_event=outbox_event,
    )
    return DiagnoseResponse(incident_id=incident_id, diagnosis=to_view(record), persisted=persisted)
