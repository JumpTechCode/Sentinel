"""GET /metrics exposes the default Prometheus registry."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.routes.metrics import router as metrics_router


def test_metrics_exposes_registry() -> None:
    # Touch a known metric so it appears in the exposition output.
    from sentinel.observability.metrics import webhooks_total

    webhooks_total.labels(source="generic", status="accepted").inc()

    app = FastAPI()
    app.include_router(metrics_router)
    client = TestClient(app)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "sentinel_webhooks_total" in resp.text
