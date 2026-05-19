# tests/integration/persistence/test_enrichment_context_repo.py
"""IncidentRepository enrichment-context methods."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from sentinel.persistence.repositories import (
    OutboxEvent,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.context import FetcherResult, IncidentContext
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _empty_ctx(incident_id: UUID, assembled_at: datetime) -> IncidentContext:
    empty: FetcherResult[object] = FetcherResult(status="ok", data=[], fetched_at=assembled_at)
    return IncidentContext(
        incident_id=incident_id,
        assembled_at=assembled_at,
        recent_deploys=empty,
        related_alerts=empty,
        similar_incidents=empty,
        runbooks=empty,
        recent_logs=empty,
        active_alerts=empty,
    )


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


@pytest.fixture()
def migrated(pg_dsn: str) -> None:
    """Apply all migrations to the test DB.

    Mirrors the in-process alembic pattern used by ``test_migration_0003.py``
    and ``test_migration_roundtrip.py`` — env.py runs ``asyncio.run`` itself,
    so sync ``command.upgrade`` is safe from this non-async test context.
    """
    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")


@pytest.fixture()
async def repo(pg_dsn: str, migrated: None) -> AsyncIterator[PostgresIncidentRepository]:
    engine = create_async_engine(pg_dsn)
    sf = make_session_factory(engine)
    try:
        yield PostgresIncidentRepository(sf)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_write_then_get_round_trips(repo: PostgresIncidentRepository) -> None:
    alert = NormalizedAlert(
        source="generic",
        external_id="ext-1",
        service="api",
        severity="SEV2",
        title="boom",
        raw_payload={},
        received_at=datetime.now(UTC),
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-1")

    event_id = uuid4()
    assembled_at = datetime.now(UTC)
    result = await repo.write_enrichment_context(
        incident_id=incident_id,
        event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at,
        outbox_event=None,
    )
    assert result.status == "written"
    assert result.version == 1

    stored = await repo.get_enrichment_context(incident_id)
    assert stored is not None
    assert stored.version == 1
    assert stored.assembled_at == assembled_at
    assert stored.last_event_id == event_id
    assert stored.context.incident_id == incident_id


@pytest.mark.asyncio
async def test_duplicate_event_id_is_noop(repo: PostgresIncidentRepository) -> None:
    alert = NormalizedAlert(
        source="generic",
        external_id="ext-dup",
        service="api",
        severity="SEV2",
        title="boom",
        raw_payload={},
        received_at=datetime.now(UTC),
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-dup")
    event_id = uuid4()
    assembled_at = datetime.now(UTC)
    first = await repo.write_enrichment_context(
        incident_id=incident_id,
        event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at,
        outbox_event=None,
    )
    assert first.status == "written"
    assert first.version == 1
    second = await repo.write_enrichment_context(
        incident_id=incident_id,
        event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at,
        outbox_event=None,
    )
    assert second.status == "duplicate"
    assert second.version == 1
    stored = await repo.get_enrichment_context(incident_id)
    assert stored is not None
    assert stored.version == 1


@pytest.mark.asyncio
async def test_outbox_event_staged_in_same_tx(repo: PostgresIncidentRepository) -> None:
    alert = NormalizedAlert(
        source="generic",
        external_id="ext-ob",
        service="api",
        severity="SEV2",
        title="boom",
        raw_payload={},
        received_at=datetime.now(UTC),
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-ob")
    event_id = uuid4()
    assembled_at = datetime.now(UTC)
    outbox_event = OutboxEvent(
        id=uuid4(),
        topic="sentinel.incidents",
        key=str(incident_id),
        payload={
            "event": "incident.enriched",
            "event_id": str(uuid4()),
            "incident_id": str(incident_id),
        },
        attempts=0,
        created_at=assembled_at,
    )
    result = await repo.write_enrichment_context(
        incident_id=incident_id,
        event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at,
        outbox_event=outbox_event,
    )
    assert result.status == "written"

    from sentinel.persistence.models import OutboxEventModel
    from sqlalchemy import select

    async with repo._session_factory() as s:
        rows = (
            (
                await s.execute(
                    select(OutboxEventModel).where(OutboxEventModel.id == outbox_event.id)
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].payload["event"] == "incident.enriched"
