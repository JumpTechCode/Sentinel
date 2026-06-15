# sentinel/api/routes/incidents.py
"""Incident read/create routes.

`GET /incidents`        — filtered, paginated list.
`GET /incidents/{id}`   — detail incl. enrichment context (added in a later task).
`POST /incidents`       — manual create (added in a later task).

All handlers are thin: querying lives in `IncidentRepository`. The repo is
retrieved from `request.app.state.incident_repo`, matching the `resolve.py` /
`webhooks.py` dependency pattern. The lifespan stash of `incident_repo` and the
`include_router` registration land together in the app-wiring task (T8).
"""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from sentinel.persistence.repositories import IncidentRepository
from sentinel.schemas.api import IncidentListResponse
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
