"""End-to-end API surface against the real app + testcontainers (creditless)."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.integration


async def test_incidents_surface_end_to_end(client: AsyncClient) -> None:
    # 1. Manual create through the ingest path.
    body = {
        "source": "generic",
        "external_id": "it-1",
        "service": "checkout",
        "severity": "SEV2",
        "title": "integration incident",
        "raw_payload": {"k": "v"},
    }
    created = await client.post("/incidents", json=body)
    assert created.status_code == 201, created.text
    incident_id = created.json()["incident_id"]
    assert created.json()["status"] == "accepted"

    # 2. It appears in the filtered list.
    listed = await client.get("/incidents?service=checkout")
    assert listed.status_code == 200, listed.text
    payload = listed.json()
    assert payload["total"] >= 1
    assert any(item["id"] == incident_id for item in payload["items"])

    # 3. Detail resolves (diagnoses empty in PR 1).
    detail = await client.get(f"/incidents/{incident_id}")
    assert detail.status_code == 200, detail.text
    assert detail.json()["id"] == incident_id
    assert detail.json()["diagnoses"] == []

    # 4. Metrics scrape exposes the registry.
    metrics = await client.get("/metrics")
    assert metrics.status_code == 200
    assert "sentinel_" in metrics.text

    # 5. readyz: deps healthy; the app boots with the enrichment consumer
    #    enabled (diagnosis/memory disabled), so it should be ready.
    ready = await client.get("/readyz")
    assert ready.json()["checks"]["postgres"] == "ok", ready.text
    assert ready.json()["checks"]["redis"] == "ok"
    assert ready.status_code == 200, ready.text


async def test_get_unknown_incident_returns_404(client: AsyncClient) -> None:
    resp = await client.get("/incidents/00000000-0000-0000-0000-000000000000")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "incident_not_found"
