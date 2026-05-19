"""Integration smoke tests against the live docker-compose stack.

Each test is independent and verifies one service is reachable from the host
using the same env vars the API uses. Run via `make test-integration`
(requires `make compose-up` first), or in CI where the integration job
starts compose explicitly.

When the required env vars are not set (typical local dev without `.env`),
tests skip with a clear message instead of failing with a Pydantic
ValidationError — they exercise live infra, not application logic, so
running them without that infra configured isn't a regression.
"""

from __future__ import annotations

import asyncpg
import pytest
from aiokafka import AIOKafkaProducer
from redis.asyncio import Redis
from sentinel.config.settings import Settings, load_settings

pytestmark = pytest.mark.integration


@pytest.fixture()
def settings() -> Settings:
    try:
        return load_settings()
    except Exception as err:
        pytest.skip(
            "smoke test requires SENTINEL_POSTGRES_DSN / SENTINEL_REDIS_URL / "
            f"SENTINEL_KAFKA_BROKERS / SENTINEL_ANTHROPIC_API_KEY in env: {err}"
        )


def _asyncpg_dsn(sqlalchemy_dsn: str) -> str:
    """Strip the `+asyncpg` SQLAlchemy driver suffix for raw asyncpg connect."""
    return sqlalchemy_dsn.replace("postgresql+asyncpg://", "postgresql://", 1)


@pytest.mark.asyncio
async def test_postgres_reachable_and_pgvector_available(settings: Settings) -> None:
    conn = await asyncpg.connect(_asyncpg_dsn(settings.postgres_dsn))
    try:
        one = await conn.fetchval("SELECT 1")
        assert one == 1
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        ext = await conn.fetchval("SELECT extname FROM pg_extension WHERE extname = 'vector'")
        assert ext == "vector", "pgvector extension must be available on this image"
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_redis_ping_works(settings: Settings) -> None:
    client = Redis.from_url(settings.redis_url)
    try:
        pong = await client.ping()
        assert pong is True
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_kafka_metadata_reachable(settings: Settings) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_brokers)
    await producer.start()
    try:
        brokers = producer.client.cluster.brokers()
        assert brokers, "Kafka cluster metadata returned no brokers"
    finally:
        await producer.stop()
