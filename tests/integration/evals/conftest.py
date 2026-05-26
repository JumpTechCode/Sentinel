"""Testcontainers-backed Postgres for eval-run lifecycle integration tests."""

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
    """Apply all migrations once per session via in-process alembic."""
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
def _truncate_eval_tables_between_tests(pg_dsn: str) -> Iterator[None]:
    """Truncate eval_runs + eval_case_results before each test."""

    async def _truncate() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("TRUNCATE eval_case_results, eval_runs RESTART IDENTITY CASCADE")
                )
        finally:
            await engine.dispose()

    asyncio.run(_truncate())
    yield


@pytest.fixture()
def session_factory(pg_dsn: str):  # type: ignore[no-untyped-def]
    """Builds a session_factory for a fresh engine per test."""
    from sentinel.persistence.session import make_session_factory

    engine = create_async_engine(pg_dsn)
    yield make_session_factory(engine)
    asyncio.run(engine.dispose())
