# tests/integration/memory/conftest.py
"""Testcontainers-backed Postgres for memory integration tests.

Mirrors tests/integration/diagnosis/conftest.py. Memory tests invoke
MemoryConsumer.handle_message directly with fabricated ConsumerRecords,
so no Kafka container is required.

The session-scope ``_migrate`` autouse + a round-trip test
(``test_migration_0005.py`` does ``downgrade -1`` then ``upgrade head``) is
safe because the round-trip ends at head and pytest runs tests sequentially
within a session by default. (The diagnosis conftest doesn't include a session
``_migrate`` for the same reason — its tests own their own migration lifecycle.
The memory conftest takes the opposite approach because most memory tests want
a pre-migrated database.)
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    image = os.environ.get("SENTINEL_TEST_PG_IMAGE", "pgvector/pgvector:pg16")
    with PostgresContainer(image, driver="asyncpg") as pg:
        yield pg


@pytest.fixture()
def pg_dsn(pg_container: PostgresContainer) -> str:
    raw: str = pg_container.get_connection_url()
    return raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )


@pytest.fixture(scope="session", autouse=True)
def _migrate(pg_container: PostgresContainer) -> None:
    """Apply all migrations once per session via in-process alembic.

    Sync fixture on purpose: alembic env.py uses asyncio.run itself, which
    fails if called from inside a running event loop.
    """
    from alembic import command
    from alembic.config import Config

    raw = pg_container.get_connection_url()
    async_dsn = raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", async_dsn)
    command.upgrade(cfg, "head")


@pytest.fixture(autouse=True)
def _reset_state(pg_dsn: str) -> Iterator[None]:
    """Reset Postgres per-test. CASCADE handles resolutions/diagnoses FK refs."""

    async def _truncate() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("TRUNCATE incidents, outbox_events CASCADE"))
        finally:
            await engine.dispose()

    asyncio.run(_truncate())
    yield


# Used by later integration tests (Tasks 4, 5, 7, 11) — not dead code.
@pytest.fixture()
def session_factory(pg_dsn: str):  # type: ignore[no-untyped-def]
    """Builds a session_factory for a fresh engine per test."""
    from sentinel.persistence.session import make_session_factory

    engine = create_async_engine(pg_dsn)
    yield make_session_factory(engine)
    asyncio.run(engine.dispose())
