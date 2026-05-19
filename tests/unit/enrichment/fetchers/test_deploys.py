# tests/unit/enrichment/fetchers/test_deploys.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.fetchers.deploys import DeploysFetcher
from sentinel.schemas.context import DeployItem


@dataclass(frozen=True)
class _Incident:
    id: UUID
    service: str


class _FakeDeployRepo:
    def __init__(self, rows: list[DeployItem]) -> None:
        self._rows = rows
        self.called_with: dict[str, Any] | None = None

    async def recent_for_service_window(
        self,
        *,
        service: str,
        since: datetime,
    ) -> list[DeployItem]:
        self.called_with = {"service": service, "since": since}
        return self._rows


def _deps(deploy_repo: _FakeDeployRepo) -> EnrichmentDeps:
    return EnrichmentDeps(
        fetchers=(),
        breakers={},
        incident_repo=None,  # type: ignore[arg-type]
        deploy_repo=deploy_repo,  # type: ignore[arg-type]
        similar_incidents=None,  # type: ignore[arg-type]
        runbooks=None,  # type: ignore[arg-type]
        log_search=None,  # type: ignore[arg-type]
        active_alerts=None,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_deploys_fetcher_returns_ok_with_rows() -> None:
    fixed = datetime(2026, 5, 19, tzinfo=UTC)
    repo = _FakeDeployRepo(
        [
            DeployItem(
                id="deploy:abc",
                service="api",
                sha="abc",
                pr_number=None,
                pr_title=None,
                pr_diff_summary=None,
                deployed_at=fixed,
                deployed_by=None,
            ),
        ]
    )
    incident = _Incident(id=uuid4(), service="api")
    r = await DeploysFetcher().fetch(incident, _deps(repo))
    assert r.status == "ok"
    assert len(r.data) == 1
    assert r.data[0].sha == "abc"
    assert repo.called_with is not None
    assert repo.called_with["service"] == "api"
    assert datetime.now(UTC) - repo.called_with["since"] > timedelta(hours=23)


@pytest.mark.asyncio
async def test_deploys_fetcher_returns_ok_empty_when_no_rows() -> None:
    repo = _FakeDeployRepo([])
    incident = _Incident(id=uuid4(), service="api")
    r = await DeploysFetcher().fetch(incident, _deps(repo))
    assert r.status == "ok"
    assert r.data == []
