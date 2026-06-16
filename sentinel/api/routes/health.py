"""Liveness + readiness endpoints.

`/healthz` returns 200 unconditionally — the process is up. It is NOT a proxy
for "consumers are processing"; after issue #36 decoupled consumer startup from
the FastAPI lifespan, the app boots before Kafka is necessarily reachable, so
a liveness probe must not block on consumer state.

`/readyz` is the full dependency readiness signal. It probes Postgres
(`app.state.engine`, via the persistence-layer `check_connection`) and Redis
(`app.state.redis`, a `redis.asyncio.Redis`) directly, and represents Kafka
readiness via consumer aliveness tracked in `app.state.consumer_alive` (a dead
broker flips consumers dead). Status is `ok` (200) only when every check passes;
otherwise `degraded` (503). When there is no consumer-aliveness signal at all
(lifespan never ran, or no consumers are configured) it stays 503 — `unknown`
if the mandatory deps are healthy, `degraded` if they are also failing — so a
readiness gate never routes to a pod whose consumer wiring is unverified. A bare
`build_app()` with no lifespan has no engine/redis on state, so both probe as
`dead` → 503/`degraded`.
"""

from __future__ import annotations

import asyncio
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from sentinel.persistence.session import check_connection

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


_PROBE_TIMEOUT_S = 2.0


async def _check_postgres(engine: AsyncEngine | None) -> CheckStatus:
    if engine is None:
        return "dead"
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT_S):
            await check_connection(engine)
        return "ok"
    except Exception:
        return "dead"


async def _check_redis(redis: Redis | None) -> CheckStatus:
    if redis is None:
        return "dead"
    try:
        async with asyncio.timeout(_PROBE_TIMEOUT_S):
            await redis.ping()
        return "ok"
    except Exception:
        return "dead"


@router.get(
    "/readyz",
    response_model=Readyz,
    responses={
        200: {"description": "All dependency checks passed", "model": Readyz},
        503: {"description": "One or more dependency checks failing", "model": Readyz},
    },
)
async def readyz(request: Request) -> JSONResponse:
    state = request.app.state
    engine = getattr(state, "engine", None)
    redis = getattr(state, "redis", None)
    alive = getattr(state, "consumer_alive", None)

    pg, rd = await asyncio.gather(_check_postgres(engine), _check_redis(redis))
    checks: dict[str, CheckStatus] = {"postgres": pg, "redis": rd}
    consumers = alive if isinstance(alive, dict) else None
    if consumers:
        for name, ok in consumers.items():
            checks[f"consumer:{name}"] = "ok" if ok else "dead"

    if not consumers:
        # No consumer-aliveness signal: lifespan never ran, or no consumers are
        # configured. A readiness gate must not route to a pod whose consumer
        # wiring is unverified — stay 503. Report "unknown" when the mandatory
        # deps are otherwise healthy, "degraded" when they are also failing.
        deps_ok = pg == "ok" and rd == "ok"
        status: Literal["ok", "degraded", "unknown"] = "unknown" if deps_ok else "degraded"
        return JSONResponse(
            content=Readyz(status=status, checks=checks).model_dump(),
            status_code=503,
        )

    all_ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        content=Readyz(status="ok" if all_ok else "degraded", checks=checks).model_dump(),
        status_code=200 if all_ok else 503,
    )
