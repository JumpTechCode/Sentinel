# tests/integration/persistence/test_migration_roundtrip.py
"""upgrade head -> downgrade base on a real Postgres with pgvector.

Note: alembic's sync `command.upgrade` works here because our env.py calls
`asyncio.run(run_migrations_online())`. This is only safe when called from a
sync context (no running event loop). If future tests run in an async context,
wrap with `loop.run_in_executor`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def test_upgrade_then_downgrade(pg_dsn: str) -> None:
    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")

    async def _list_tables() -> set[str]:
        eng = create_async_engine(pg_dsn)
        try:
            async with eng.connect() as conn:
                names = await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
        finally:
            await eng.dispose()
        return names

    tables = asyncio.run(_list_tables())
    assert {"incidents", "diagnoses", "resolutions", "deploys", "runbooks", "eval_runs"}.issubset(
        tables
    )

    command.downgrade(cfg, "base")
    tables_after = asyncio.run(_list_tables())
    assert (
        not {"incidents", "diagnoses", "resolutions", "deploys", "runbooks", "eval_runs"}
        & tables_after
    )


def test_vector_extension_present_after_upgrade(pg_dsn: str) -> None:
    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")

    async def _check() -> bool:
        eng = create_async_engine(pg_dsn)
        try:
            async with eng.connect() as conn:
                row = await conn.execute(
                    text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
                )
                return row.first() is not None
        finally:
            await eng.dispose()

    assert asyncio.run(_check()) is True

    command.downgrade(cfg, "base")
