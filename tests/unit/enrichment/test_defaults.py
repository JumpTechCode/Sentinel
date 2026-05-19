# tests/unit/enrichment/test_defaults.py
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sentinel.enrichment.defaults import (
    NotConfiguredActiveAlerts,
    NotConfiguredLogSearch,
    NotConfiguredRunbookRetrieval,
    NotConfiguredSimilarIncidents,
)


@pytest.mark.asyncio
async def test_similar_incidents_default_returns_degraded() -> None:
    r = await NotConfiguredSimilarIncidents().top_k(
        query_text="x",
        k=5,
        exclude_incident_id=uuid4(),
    )
    assert r.status == "degraded"
    assert r.data == []
    assert r.error == "not_configured"


@pytest.mark.asyncio
async def test_runbooks_default_returns_degraded() -> None:
    r = await NotConfiguredRunbookRetrieval().top_k(
        query_text="x",
        k=3,
        min_cosine=0.6,
    )
    assert r.status == "degraded"
    assert r.error == "not_configured"


@pytest.mark.asyncio
async def test_log_search_default_returns_degraded() -> None:
    r = await NotConfiguredLogSearch().recent_errors(
        service="api",
        since=datetime.now(UTC),
        limit=50,
    )
    assert r.status == "degraded"
    assert r.error == "not_configured"


@pytest.mark.asyncio
async def test_active_alerts_default_returns_degraded() -> None:
    r = await NotConfiguredActiveAlerts().active_for_service(
        service="api",
        exclude_external_id="ext-1",
    )
    assert r.status == "degraded"
    assert r.error == "not_configured"
