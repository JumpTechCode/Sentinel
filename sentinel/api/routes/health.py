"""Liveness + readiness endpoints.

Liveness (`/healthz`) returns 200 unconditionally — the process is up.
Readiness (`/readyz`) will gain real dependency checks (pg, redis, kafka)
in Work Area I. Until then it returns 503 with status ``"unknown"`` so a
real readiness gate never accidentally routes traffic to an unverified pod.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

router = APIRouter(tags=["health"])


class Healthz(BaseModel):
    status: Literal["ok"] = "ok"


class Readyz(BaseModel):
    status: Literal["ok", "degraded", "unknown"] = "unknown"
    checks: dict[str, str] = Field(default_factory=dict)


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
async def readyz() -> JSONResponse:
    # Real checks land in Work Area I. Until then, return 503/"unknown" so a
    # readiness gate does not accidentally mark the pod ready.
    body = Readyz()
    return JSONResponse(content=body.model_dump(), status_code=503)
