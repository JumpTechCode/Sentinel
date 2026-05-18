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


def test_readyz_reports_dependencies() -> None:
    """`/readyz` returns 503/"unknown" until Work Area I wires real dependency checks.

    A 200 with empty checks would let a readiness gate route traffic to an unverified pod;
    the conservative default protects rollouts.
    """
    app = build_app()
    client = TestClient(app)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "unknown"
    assert isinstance(body["checks"], dict)
