"""Tests for the FastAPI health and readiness endpoints."""

from __future__ import annotations

from fastapi.testclient import TestClient
from sentinel.api.app import build_app


def test_healthz_returns_ok() -> None:
    """`/healthz` always returns 200 with `{"status": "ok"}` while the process is up."""
    app = build_app()
    client = TestClient(app)
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"status": "ok"}


def test_readyz_503_without_lifespan() -> None:
    """Bare build_app() has no engine/redis on state -> probes fail -> 503."""
    app = build_app()
    client = TestClient(app)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["checks"]["postgres"] == "dead"
    assert body["checks"]["redis"] == "dead"
