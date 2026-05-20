"""Liveness + readiness endpoints.

`/healthz` returns 200 unconditionally — the process is up. It is NOT a proxy
for "consumers are processing"; after issue #36 decoupled consumer startup from
the FastAPI lifespan, the app boots before Kafka is necessarily reachable, so
a liveness probe must not block on consumer state.

`/readyz` is the consumer-aware signal. It returns 200 only when every
expected background Kafka consumer (memory / enrichment / diagnosis) is alive,
as tracked in `app.state.consumer_alive` and flipped by the
`_on_consumer_task_done` done-callback. If any consumer is dead, or no
aliveness state is recorded (lifespan didn't run, or no consumers configured),
it returns 503 so readiness gates do not route traffic.

Pg/redis/kafka dependency probes land alongside this in Work Area I.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

router = APIRouter(tags=["health"])


class Healthz(BaseModel):
    status: Literal["ok"] = "ok"


CheckStatus = Literal["ok", "dead"]


class Readyz(BaseModel):
    status: Literal["ok", "degraded", "unknown"] = "unknown"
    checks: dict[str, CheckStatus] = Field(default_factory=dict)


@router.get("/healthz", response_model=Healthz)
async def healthz() -> Healthz:
    return Healthz()


@router.get(
    "/readyz",
    response_model=Readyz,
    responses={
        200: {"description": "All dependency checks passed", "model": Readyz},
        503: {"description": "Dependency checks not yet wired or failing", "model": Readyz},
    },
)
async def readyz(request: Request) -> JSONResponse:
    alive = getattr(request.app.state, "consumer_alive", None)
    # Missing dict (lifespan never ran) or empty (no consumers configured)
    # both fall back to "unknown" — never route traffic on no signal.
    if not isinstance(alive, dict) or not alive:
        return JSONResponse(
            content=Readyz(status="unknown", checks={}).model_dump(),
            status_code=503,
        )

    checks: dict[str, CheckStatus] = {
        f"consumer:{name}": ("ok" if v else "dead") for name, v in alive.items()
    }
    if all(alive.values()):
        return JSONResponse(
            content=Readyz(status="ok", checks=checks).model_dump(),
            status_code=200,
        )
    return JSONResponse(
        content=Readyz(status="degraded", checks=checks).model_dump(),
        status_code=503,
    )
