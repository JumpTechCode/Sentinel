# tests/integration/persistence/test_repositories.py
"""Round-trip each repo against a live Postgres."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sentinel.persistence.repositories import (
    DeployRepository,
    IncidentRepository,
    PostgresDeployRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas import NormalizedAlert
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


@pytest.fixture()
def migrated_dsn(pg_dsn: str) -> str:
    command.upgrade(_alembic_cfg(pg_dsn), "head")
    return pg_dsn


@pytest.mark.asyncio
async def test_incident_insert_then_fetch(migrated_dsn: str) -> None:
    engine = create_async_engine(migrated_dsn)
    factory = make_session_factory(engine)
    repo: IncidentRepository = PostgresIncidentRepository(factory)

    alert = NormalizedAlert(
        source="generic",
        external_id="ext-1",
        service="svc",
        severity="SEV3",
        title="t",
        received_at=datetime.now(UTC),
        raw_payload={"k": "v"},
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-1")

    fetched = await repo.get(incident_id)
    assert fetched is not None
    assert fetched.id == incident_id
    assert fetched.source == "generic"
    assert fetched.external_id == "ext-1"
    assert fetched.service == "svc"
    assert fetched.severity == "SEV3"
    assert fetched.title == "t"
    assert fetched.fingerprint == "fp-1"
    assert fetched.status == "open"
    assert fetched.opened_at is not None
    assert fetched.resolved_at is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_incident_get_missing_returns_none(migrated_dsn: str) -> None:
    engine = create_async_engine(migrated_dsn)
    factory = make_session_factory(engine)
    repo: IncidentRepository = PostgresIncidentRepository(factory)
    fetched = await repo.get(uuid4())
    assert fetched is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_incident_mark_diagnosing_updates_status(migrated_dsn: str) -> None:
    engine = create_async_engine(migrated_dsn)
    factory = make_session_factory(engine)
    repo: IncidentRepository = PostgresIncidentRepository(factory)
    alert = NormalizedAlert(
        source="generic",
        external_id="ext-2",
        service="svc",
        severity="SEV3",
        title="t2",
        received_at=datetime.now(UTC),
        raw_payload={"k": "v"},
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-2")

    await repo.mark_diagnosing(incident_id)
    fetched = await repo.get(incident_id)
    assert fetched is not None
    assert fetched.status == "diagnosing"
    await engine.dispose()


@pytest.mark.asyncio
async def test_deploy_insert_then_list_by_service(migrated_dsn: str) -> None:
    engine = create_async_engine(migrated_dsn)
    factory = make_session_factory(engine)
    repo: DeployRepository = PostgresDeployRepository(factory)
    await repo.record(service="svc", sha="abc123", deployed_at=datetime.now(UTC))
    deploys = await repo.recent_for_service("svc", limit=10)
    assert any(d.sha == "abc123" for d in deploys)
    await engine.dispose()


@pytest.mark.asyncio
async def test_list_incidents_filters_and_paginates(migrated_dsn: str) -> None:
    engine = create_async_engine(migrated_dsn)
    factory = make_session_factory(engine)
    repo: IncidentRepository = PostgresIncidentRepository(factory)

    svc = f"listtest-{uuid4().hex[:8]}"

    async def _seed(external_id: str, severity: str, title: str, fp: str) -> UUID:
        alert = NormalizedAlert(
            source="generic",
            external_id=external_id,
            service=svc,
            severity=severity,
            title=title,
            received_at=datetime.now(UTC),
            raw_payload={"k": "v"},
        )
        return await repo.create_from_alert(alert, fingerprint=fp)

    a = await _seed("lst-1", "SEV1", "alpha", f"{svc}-1")
    await _seed("lst-2", "SEV3", "beta", f"{svc}-2")
    await _seed("lst-3", "SEV1", "gamma", f"{svc}-3")

    # service filter
    items, total = await repo.list_incidents(service=svc)
    assert total == 3
    assert {i.service for i in items} == {svc}

    # service + severity filter
    sev_items, sev_total = await repo.list_incidents(service=svc, severity="SEV1")
    assert sev_total == 2
    assert all(i.severity == "SEV1" for i in sev_items)

    # status filter (mark one diagnosing)
    await repo.mark_diagnosing(a)
    st_items, st_total = await repo.list_incidents(service=svc, status="diagnosing")
    assert st_total == 1
    assert st_items[0].id == a

    # pagination: limit caps page size, total reflects full match count
    page1, total_all = await repo.list_incidents(service=svc, limit=2, offset=0)
    assert total_all == 3
    assert len(page1) == 2
    page2, _ = await repo.list_incidents(service=svc, limit=2, offset=2)
    assert len(page2) == 1
    assert {i.id for i in page1}.isdisjoint({i.id for i in page2})

    await engine.dispose()
