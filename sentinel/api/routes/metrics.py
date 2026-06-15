# sentinel/api/routes/metrics.py
"""GET /metrics — Prometheus exposition of the default registry.

The metrics module (`sentinel/observability/metrics.py`) registers all
collectors on the prometheus_client default REGISTRY at import time. This route
is a read-only scrape; it must never block request handlers.
"""

from __future__ import annotations

from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter(tags=["observability"])


@router.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
