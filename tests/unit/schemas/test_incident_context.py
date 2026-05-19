# tests/unit/schemas/test_incident_context.py
"""IncidentContext requires incident_id and assembled_at."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sentinel.schemas.context import FetcherResult, IncidentContext


def _empty_results() -> dict[str, FetcherResult[object]]:
    now = datetime.now(UTC)
    empty: FetcherResult[object] = FetcherResult(status="ok", data=[], fetched_at=now)
    return {
        "recent_deploys": empty,
        "related_alerts": empty,
        "similar_incidents": empty,
        "runbooks": empty,
        "recent_logs": empty,
        "active_alerts": empty,
    }


def test_incident_context_has_incident_id_and_assembled_at() -> None:
    incident_id = uuid4()
    assembled_at = datetime.now(UTC)
    ctx = IncidentContext(
        incident_id=incident_id,
        assembled_at=assembled_at,
        **_empty_results(),
    )
    assert ctx.incident_id == incident_id
    assert ctx.assembled_at == assembled_at


def test_incident_context_rejects_missing_incident_id() -> None:
    with pytest.raises(ValidationError):
        IncidentContext(assembled_at=datetime.now(UTC), **_empty_results())
