# tests/integration/persistence/test_diagnosis_repository.py
"""PostgresDiagnosisRepository — integration tests with real Postgres."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.persistence.repositories import (
    OutboxEvent,
    PostgresDiagnosisRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.diagnosis import EvidenceRef
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


@pytest.fixture()
def migrated(pg_dsn: str) -> None:
    """Apply all migrations to the test DB."""
    command.upgrade(_alembic_cfg(pg_dsn), "head")


@pytest.fixture()
async def repos(
    pg_dsn: str, migrated: None
) -> AsyncIterator[tuple[PostgresIncidentRepository, PostgresDiagnosisRepository]]:
    engine = create_async_engine(pg_dsn)
    sf = make_session_factory(engine)
    try:
        yield PostgresIncidentRepository(sf), PostgresDiagnosisRepository(sf)
    finally:
        await engine.dispose()


def _alert() -> NormalizedAlert:
    return NormalizedAlert(
        source="generic",
        external_id=f"ext-{uuid4().hex[:8]}",
        service="api",
        severity="SEV2",
        title="boom",
        raw_payload={},
        received_at=datetime.now(UTC),
    )


def _record(*, model: str = "claude-sonnet-4-5", prompt_version: str = "v1") -> PersistedDiagnosis:
    return PersistedDiagnosis(
        hypothesis="h",
        confidence=Decimal("0.5"),
        reasoning="r",
        evidence=[EvidenceRef(kind="deploy", id="deploy:abc", note="n")],
        suggested_actions=[],
        likely_category="deploy",
        hallucinated_evidence=False,
        model=model,
        prompt_version=prompt_version,
        latency_ms=100,
        token_usage={
            "input": 10,
            "output": 5,
            "cost_usd": "0.0001",
            "prompt_sha": "0" * 64,
            "truncated": {},
        },
    )


def _outbox(incident_id: object) -> OutboxEvent:
    eid = uuid4()
    return OutboxEvent(
        id=eid,
        topic="sentinel.incidents",
        key=str(incident_id),
        payload={
            "event_id": str(eid),
            "event": "incident.diagnosed",
            "incident_id": str(incident_id),
            "ts": datetime.now(UTC).isoformat(),
        },
        attempts=0,
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_save_with_outbox_inserts_both_atomically(
    repos: tuple[PostgresIncidentRepository, PostgresDiagnosisRepository],
    pg_dsn: str,
) -> None:
    incident_repo, diag_repo = repos
    incident_id = await incident_repo.create_from_alert(
        _alert(), fingerprint=f"fp-{uuid4().hex[:8]}"
    )
    outbox_template = _outbox(incident_id)
    diag_id, status = await diag_repo.save_with_outbox(
        incident_id=incident_id,
        record=_record(),
        upstream_event_id=uuid4(),
        outbox_event=outbox_template,
    )
    assert status == "new"

    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            row = (
                await conn.execute(
                    text("SELECT payload FROM outbox_events WHERE id = :id"),
                    {"id": outbox_template.id},
                )
            ).one()
            assert row[0]["diagnosis_id"] == str(diag_id)
            assert row[0]["event"] == "incident.diagnosed"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_duplicate_returns_existing_and_skips_outbox(
    repos: tuple[PostgresIncidentRepository, PostgresDiagnosisRepository],
    pg_dsn: str,
) -> None:
    incident_repo, diag_repo = repos
    incident_id = await incident_repo.create_from_alert(
        _alert(), fingerprint=f"fp-{uuid4().hex[:8]}"
    )
    first = _outbox(incident_id)
    second = _outbox(incident_id)
    diag_id_1, status_1 = await diag_repo.save_with_outbox(
        incident_id=incident_id,
        record=_record(),
        upstream_event_id=uuid4(),
        outbox_event=first,
    )
    diag_id_2, status_2 = await diag_repo.save_with_outbox(
        incident_id=incident_id,
        record=_record(),
        upstream_event_id=uuid4(),
        outbox_event=second,
    )
    assert status_1 == "new"
    assert status_2 == "duplicate"
    assert diag_id_1 == diag_id_2

    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            count = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM outbox_events "
                        "WHERE key = :k AND payload->>'event' = 'incident.diagnosed'"
                    ),
                    {"k": str(incident_id)},
                )
            ).scalar_one()
            assert count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_different_prompt_version_produces_new_row(
    repos: tuple[PostgresIncidentRepository, PostgresDiagnosisRepository],
) -> None:
    incident_repo, diag_repo = repos
    incident_id = await incident_repo.create_from_alert(
        _alert(), fingerprint=f"fp-{uuid4().hex[:8]}"
    )
    await diag_repo.save_with_outbox(
        incident_id=incident_id,
        record=_record(prompt_version="v1"),
        upstream_event_id=uuid4(),
        outbox_event=_outbox(incident_id),
    )
    _, status = await diag_repo.save_with_outbox(
        incident_id=incident_id,
        record=_record(prompt_version="v2"),
        upstream_event_id=uuid4(),
        outbox_event=_outbox(incident_id),
    )
    assert status == "new"
