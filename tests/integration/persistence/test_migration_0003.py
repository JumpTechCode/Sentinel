# tests/integration/persistence/test_migration_0003.py
"""Migration 0003 adds the four enrichment columns and is reversible."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def _enrichment_column_names(dsn: str) -> set[str]:
    async def _q() -> set[str]:
        engine = create_async_engine(dsn)
        try:
            async with engine.begin() as conn:
                cols = await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'incidents' AND column_name IN "
                        "('context_json','context_assembled_at',"
                        "'context_version','last_enrichment_event_id')"
                    )
                )
                return {row[0] for row in cols.fetchall()}
        finally:
            await engine.dispose()

    return asyncio.run(_q())


def test_upgrade_adds_columns_then_downgrade_drops_them(pg_dsn: str) -> None:
    cfg = _alembic_cfg(pg_dsn)

    command.upgrade(cfg, "0003_incident_enrichment_context")
    assert _enrichment_column_names(pg_dsn) == {
        "context_json",
        "context_assembled_at",
        "context_version",
        "last_enrichment_event_id",
    }

    command.downgrade(cfg, "0002_outbox_events")
    assert _enrichment_column_names(pg_dsn) == set()

    # Leave the test database empty for any subsequent session-scoped use.
    command.downgrade(cfg, "base")
