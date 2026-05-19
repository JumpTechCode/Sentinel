"""Migration 0002 upgrade/downgrade round-trip."""

from __future__ import annotations

import asyncio

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_upgrade_then_downgrade_outbox_events(pg_dsn: str) -> None:
    # env.py uses async_engine_from_config, so the URL must keep the asyncpg driver.
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", pg_dsn)

    # alembic's env.py wraps run_migrations_online() with asyncio.run(...),
    # which fails inside a running event loop. Hand it to a worker thread.
    await asyncio.to_thread(command.upgrade, cfg, "head")

    engine = create_async_engine(pg_dsn)
    try:
        async with engine.connect() as conn:
            exists = (
                await conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_name = 'outbox_events'"
                    )
                )
            ).scalar()
            assert exists == 1

        # Downgrade past 0002 — should drop outbox_events.
        await asyncio.to_thread(command.downgrade, cfg, "0001_initial")
        async with engine.connect() as conn:
            exists = (
                await conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_name = 'outbox_events'"
                    )
                )
            ).scalar()
            assert exists is None

        # Restore head so subsequent tests can use the table.
        await asyncio.to_thread(command.upgrade, cfg, "head")
    finally:
        await engine.dispose()
