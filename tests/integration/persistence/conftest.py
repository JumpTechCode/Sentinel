# tests/integration/persistence/conftest.py
"""Testcontainers-backed Postgres with pgvector."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator

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
    # testcontainers returns a sync-flavored URL; rewrite to asyncpg.
    raw: str = pg_container.get_connection_url()
    return raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )


@pytest.fixture(autouse=True)
def _truncate_eval_tables_between_tests(pg_dsn: str) -> Iterator[None]:
    """Function-scoped: truncate eval_runs + eval_case_results before each test.

    Skips silently if the tables don't exist yet (migration tests transition
    the DB across these states). CASCADE handles the FK from eval_case_results
    to eval_runs and prevents cross-test data leakage in a session-scoped container.
    """

    async def _truncate() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            async with engine.begin() as conn:
                existing = (
                    (
                        await conn.execute(
                            text(
                                "SELECT table_name FROM information_schema.tables "
                                "WHERE table_name IN ('eval_runs','eval_case_results')"
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
                if "eval_runs" in existing and "eval_case_results" in existing:
                    await conn.execute(
                        text("TRUNCATE eval_case_results, eval_runs RESTART IDENTITY CASCADE")
                    )
        finally:
            await engine.dispose()

    asyncio.run(_truncate())
    yield
