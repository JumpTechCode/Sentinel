"""POST /webhooks/{source} — thin route; orchestration in WebhookHandler."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from sentinel.ingestion.webhook import HandlerOutcome
from sentinel.schemas.api import WebhookAcceptedResponse

router = APIRouter(tags=["webhooks"])

# Outcomes that produce an accepted-style response body.
_SUCCESS_OUTCOMES: dict[
    HandlerOutcome, tuple[int, Literal["accepted", "recurred", "duplicate"]]
] = {
    HandlerOutcome.ACCEPTED: (202, "accepted"),
    HandlerOutcome.RECURRED: (202, "recurred"),
    HandlerOutcome.DUPLICATE: (200, "duplicate"),
}

# Outcomes that produce a minimal error body.
_ERROR_STATUS: dict[HandlerOutcome, int] = {
    HandlerOutcome.UNAUTHORIZED: 401,
    HandlerOutcome.BAD_REQUEST: 422,
    HandlerOutcome.UNKNOWN_SOURCE: 404,
    HandlerOutcome.INFRA_UNAVAILABLE: 503,
}


@router.post(
    "/webhooks/{source}",
    response_model=None,
    status_code=202,
    responses={
        200: {"model": WebhookAcceptedResponse, "description": "Duplicate"},
        202: {"model": WebhookAcceptedResponse, "description": "Accepted"},
        401: {"description": "Bad signature or missing secret"},
        404: {"description": "Unknown source"},
        422: {"description": "Bad request payload"},
        503: {"description": "Infra unavailable"},
    },
)
async def receive_webhook(source: str, request: Request) -> JSONResponse:
    body = await request.body()
    handler = request.app.state.webhook_handler
    # Pass starlette's Headers object directly — it does case-insensitive lookup,
    # which is what the HTTP spec requires (adapter `.get("Sentry-Hook-Signature")`
    # must match `sentry-hook-signature` from the wire).
    result = await handler.handle(
        source=source,
        headers=request.headers,
        body=body,
    )
    if result.outcome in _SUCCESS_OUTCOMES:
        status_code, wire_status = _SUCCESS_OUTCOMES[result.outcome]
        body_obj = WebhookAcceptedResponse(
            status=wire_status,
            incident_id=result.incident_id,
        )
        return JSONResponse(status_code=status_code, content=body_obj.model_dump(mode="json"))
    # Error path: minimal JSON body.
    status_code = _ERROR_STATUS[result.outcome]
    return JSONResponse(
        status_code=status_code,
        content={"status": result.outcome.value, "reason": result.reason},
    )
