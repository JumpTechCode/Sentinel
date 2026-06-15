"""Testcontainers-backed Postgres (+ Kafka, Redis) for eval integration tests.

Postgres + the eval-run lifecycle test (``test_run_lifecycle``) need only the
``pg_*`` fixtures. The full-pipeline runner e2e (``test_runner_e2e``) also boots
the FastAPI app, which connects to Kafka and Redis — those containers are
session-scoped and lazy, so they only start when a test actually requests
``kafka_brokers`` / ``redis_url``.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
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
    raw: str = pg_container.get_connection_url()
    return raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[KafkaContainer]:
    image = os.environ.get("SENTINEL_TEST_KAFKA_IMAGE", "confluentinc/cp-kafka:7.8.0")
    # KRaft (no ZooKeeper) starts faster and sidesteps the single-node group
    # coordinator races; the default 30s readiness wait is too tight when Docker
    # has just cold-started, so give startup 120s explicitly rather than via the
    # context-manager form (which hardcodes 30s).
    kc = KafkaContainer(image).with_kraft()
    kc.start(timeout=120)
    try:
        yield kc
    finally:
        kc.stop()


@pytest.fixture()
def kafka_brokers(kafka_container: KafkaContainer) -> str:
    return str(kafka_container.get_bootstrap_server())


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
    image = os.environ.get("SENTINEL_TEST_REDIS_IMAGE", "redis:7-alpine")
    with RedisContainer(image) as rc:
        yield rc


@pytest.fixture()
def redis_url(redis_container: RedisContainer) -> str:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


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
    """Truncate the eval + pipeline tables before each test.

    eval_runs / eval_case_results cover the lifecycle test; incidents /
    outbox_events / diagnoses are added for the full-pipeline e2e (CASCADE
    clears enrichment-context + any dependents). Harmless for the
    lifecycle test, which never writes those tables.
    """

    async def _truncate() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "TRUNCATE eval_case_results, eval_runs, diagnoses, "
                        "incidents, outbox_events RESTART IDENTITY CASCADE"
                    )
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
