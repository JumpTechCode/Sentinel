# tests/integration/enrichment/conftest.py
"""Testcontainers fixtures for enrichment end-to-end tests."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.kafka import KafkaContainer
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


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[KafkaContainer]:
    image = os.environ.get("SENTINEL_TEST_KAFKA_IMAGE", "confluentinc/cp-kafka:7.8.0")
    with KafkaContainer(image) as kc:
        yield kc


@pytest.fixture()
def kafka_brokers(kafka_container: KafkaContainer) -> str:
    brokers = kafka_container.get_bootstrap_server()
    return str(brokers)


@pytest.fixture(scope="session", autouse=True)
def _migrate(pg_container: PostgresContainer) -> None:
    """Apply all migrations once per session via in-process alembic.

    Sync fixture on purpose: alembic env.py uses ``asyncio.run`` itself, which
    fails if called from inside a running event loop (which pytest-asyncio
    creates for async fixtures). Mirrors the pattern in
    ``tests/integration/ingestion/conftest.py``.
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
    """Reset Postgres per-test to prevent state leakage.

    The pg_container is session-scoped (cheap to spin up once); per-test we
    just clear data. CASCADE handles FK refs from diagnoses → incidents.
    Sync fixture so it doesn't fight pytest-asyncio's event loop.
    """
    import asyncio

    async def _truncate() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("TRUNCATE incidents, outbox_events CASCADE"))
        finally:
            await engine.dispose()

    asyncio.run(_truncate())
    yield
