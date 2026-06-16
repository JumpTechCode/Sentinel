"""Testcontainers fixtures for the API integration suite.

Boots the real app (Postgres + Redis + Kafka) with the diagnosis and memory
consumers DISABLED, so API surface tests exercise the full stack creditlessly
without reaching the Anthropic API.
Mirrors tests/load/conftest.py.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

GENERIC_SECRET = "integration-secret"


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
    return str(kafka_container.get_bootstrap_server())


@pytest.fixture(scope="session", autouse=True)
def _migrate(pg_container: PostgresContainer) -> None:
    from alembic import command
    from alembic.config import Config

    raw = pg_container.get_connection_url()
    async_dsn = raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", async_dsn)
    command.upgrade(cfg, "head")


@pytest_asyncio.fixture(autouse=True)
async def _reset_state(pg_dsn: str, redis_url: str) -> None:
    from redis.asyncio import Redis

    engine = create_async_engine(pg_dsn)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE incidents, diagnoses, outbox_events CASCADE"))
    finally:
        await engine.dispose()

    redis = Redis.from_url(redis_url)
    try:
        await redis.flushdb()
    finally:
        await redis.aclose()


@pytest.fixture(autouse=True)
def _env(
    monkeypatch: pytest.MonkeyPatch,
    pg_dsn: str,
    redis_url: str,
    kafka_brokers: str,
) -> None:
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", pg_dsn)
    monkeypatch.setenv("SENTINEL_REDIS_URL", redis_url)
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", kafka_brokers)
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "x")
    monkeypatch.setenv("SENTINEL_GENERIC_WEBHOOK_SECRET", GENERIC_SECRET)
    # Keep load traffic off the Anthropic path — ingestion only.
    monkeypatch.setenv("SENTINEL_DIAGNOSIS_CONSUMER_ENABLED", "false")
    monkeypatch.setenv("SENTINEL_MEMORY_CONSUMER_ENABLED", "false")


@pytest_asyncio.fixture
async def app() -> AsyncIterator[FastAPI]:
    from sentinel.api.app import build_app

    a = build_app()
    async with a.router.lifespan_context(a):
        yield a


@pytest_asyncio.fixture
async def client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
