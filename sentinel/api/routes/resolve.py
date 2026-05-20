# sentinel/api/routes/resolve.py
"""POST /incidents/{incident_id}/resolve — persist resolution + status + outbox event."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import ResolutionRepository
from sentinel.schemas.api import ResolveIncidentRequest, ResolveIncidentResponse

router = APIRouter(tags=["incidents"])


@router.post(
    "/incidents/{incident_id}/resolve",
    response_model=ResolveIncidentResponse,
    status_code=200,
    responses={
        404: {"description": "Incident not found"},
        409: {"description": "Incident already resolved"},
        422: {"description": "Invalid payload"},
    },
)
async def resolve_incident(
    incident_id: UUID,
    body: ResolveIncidentRequest,
    request: Request,
) -> ResolveIncidentResponse:
    repo: ResolutionRepository = request.app.state.resolution_repo
    outbox_topic: str = request.app.state.outbox_topic
    try:
        result = await repo.record(incident_id, body, outbox_topic=outbox_topic)
    except IncidentNotFound as e:
        raise HTTPException(status_code=404, detail="incident_not_found") from e
    except IncidentAlreadyResolved as e:
        raise HTTPException(status_code=409, detail="incident_already_resolved") from e
    return ResolveIncidentResponse(
        incident_id=result.incident_id,
        status="resolved",
        resolved_at=result.resolved_at,
        event_id=result.event_id,
    )
