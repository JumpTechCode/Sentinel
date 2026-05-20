# tests/integration/persistence/test_diagnosis_repository_e2e.py
"""End-to-end: save a diagnosis via save_with_outbox, read via get_by_incident_id."""

from __future__ import annotations

import asyncio
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
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def _build_alert() -> NormalizedAlert:
    # Field names verified against sentinel/schemas/alert.py:14-32.
    # NormalizedAlert has model_config = ConfigDict(frozen=True, extra="forbid"),
    # so passing unknown kwargs (e.g. fingerprint, opened_at) raises.
    return NormalizedAlert(
        source="generic",
        external_id="ext-eval-1",
        service="svc",
        severity="SEV2",
        title="test alert",
        received_at=datetime.now(UTC),
        raw_payload={},
    )


def test_get_by_incident_id_returns_most_recent(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            incidents = PostgresIncidentRepository(session_factory)
            diagnoses = PostgresDiagnosisRepository(session_factory)

            alert = _build_alert()
            incident_id = await incidents.create_from_alert(alert, fingerprint="fp-eval-1")

            diag = PersistedDiagnosis(
                hypothesis="cause X",
                confidence=Decimal("0.80"),
                reasoning="because Y",
                evidence=[EvidenceRef(kind="deploy", id="deploy:abc", note="recent")],
                suggested_actions=[
                    SuggestedAction(
                        description="rollback",
                        command=None,
                        risk="low",
                        rationale="safe",
                        requires_human_approval=True,
                    )
                ],
                likely_category="deploy",
                hallucinated_evidence=False,
                model="claude-sonnet-4-5",
                prompt_version="v1",
                latency_ms=1500,
                token_usage={"input": 100, "output": 50},
            )
            outbox_event = OutboxEvent(
                id=uuid4(),
                topic="diagnoses",
                key=str(incident_id),
                payload={"incident_id": str(incident_id)},
                attempts=0,
                created_at=datetime.now(UTC),
            )
            await diagnoses.save_with_outbox(
                incident_id=incident_id,
                record=diag,
                upstream_event_id=uuid4(),
                outbox_event=outbox_event,
            )

            got = await diagnoses.get_by_incident_id(incident_id)
            assert got is not None
            assert got.hypothesis == "cause X"
            assert got.confidence == Decimal("0.80")
            assert len(got.evidence) == 1
            assert got.evidence[0].id == "deploy:abc"

            assert await diagnoses.get_by_incident_id(uuid4()) is None
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())
