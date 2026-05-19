"""Testcontainers fixtures for ingestion integration tests."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer


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
def redis_container() -> Iterator[RedisContainer]:
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest.fixture()
def redis_url(redis_container: RedisContainer) -> str:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[KafkaContainer]:
    image = os.environ.get("SENTINEL_TEST_KAFKA_IMAGE", "confluentinc/cp-kafka:7.8.0")
    with KafkaContainer(image) as k:
        yield k


@pytest.fixture()
def kafka_brokers(kafka_container: KafkaContainer) -> str:
    brokers = kafka_container.get_bootstrap_server()
    return str(brokers)


@pytest.fixture(scope="session", autouse=True)
def _migrate(pg_container: PostgresContainer) -> None:
    """Run alembic migrations once per test session.

    Synchronous fixture: alembic's env.py uses `asyncio.run(...)`, which fails
    if called from inside a running event loop (which pytest-asyncio creates
    for async fixtures). Keep this sync so alembic can drive its own loop.
    """
    from alembic import command
    from alembic.config import Config

    raw = pg_container.get_connection_url()
    # env.py uses async_engine_from_config, so the URL must use the asyncpg driver.
    async_dsn = raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", async_dsn)
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture(autouse=True)
async def _reset_state(pg_dsn: str, redis_url: str) -> None:
    """Reset Postgres + Redis per-test to prevent state leakage.

    The pg_container and redis_container are session-scoped (cheap to spin up
    once); per-test we just clear data. RESTART IDENTITY isn't needed (UUID
    PKs); CASCADE handles FK refs from diagnoses → incidents. Redis FLUSHDB
    clears webhook-idempotency keys so per-body dedup doesn't bleed across tests.
    """
    from redis.asyncio import Redis

    engine = create_async_engine(pg_dsn)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE incidents, outbox_events CASCADE"))
    finally:
        await engine.dispose()

    redis = Redis.from_url(redis_url)
    try:
        await redis.flushdb()
    finally:
        await redis.aclose()
