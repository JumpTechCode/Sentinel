# tests/unit/enrichment/fetchers/test_related_alerts.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.fetchers.related_alerts import RelatedAlertsFetcher
from sentinel.schemas.context import RelatedAlertItem


@dataclass(frozen=True)
class _Incident:
    id: UUID
    service: str


class _FakeIncidentRepo:
    def __init__(self, rows: list[RelatedAlertItem]) -> None:
        self._rows = rows
        self.called_with: dict[str, Any] | None = None

    async def recent_for_service(
        self,
        *,
        service: str,
        since: datetime,
        exclude_incident_id: UUID,
    ) -> list[RelatedAlertItem]:
        self.called_with = {
            "service": service,
            "since": since,
            "exclude_incident_id": exclude_incident_id,
        }
        return self._rows


def _deps(repo: _FakeIncidentRepo) -> EnrichmentDeps:
    return EnrichmentDeps(
        fetchers=(),
        breakers={},
        incident_repo=repo,  # type: ignore[arg-type]
        deploy_repo=None,  # type: ignore[arg-type]
        similar_incidents=None,  # type: ignore[arg-type]
        runbooks=None,  # type: ignore[arg-type]
        log_search=None,  # type: ignore[arg-type]
        active_alerts=None,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_related_alerts_returns_ok_excluding_self() -> None:
    me = uuid4()
    items = [
        RelatedAlertItem(
            id=f"related:{uuid4()}",
            service="api",
            severity="SEV2",
            title="boom",
            opened_at=datetime.now(UTC),
        )
    ]
    repo = _FakeIncidentRepo(items)
    r = await RelatedAlertsFetcher().fetch(
        _Incident(id=me, service="api"),
        _deps(repo),
    )
    assert r.status == "ok"
    assert len(r.data) == 1
    assert repo.called_with is not None
    assert repo.called_with["exclude_incident_id"] == me
    assert datetime.now(UTC) - repo.called_with["since"] > timedelta(hours=23)
