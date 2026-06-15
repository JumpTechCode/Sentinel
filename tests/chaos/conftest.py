"""Chaos B1 stack — toxiproxy in front of Redis; Postgres + Kafka direct.

Why only Redis is proxied: the ingestion idempotency store (Redis) is the
dependency whose outage behaviour we assert (fail closed → 503). Postgres and
Kafka stay direct so the app boots and the outbox/enrichment background tasks
run normally. Kafka in particular is impractical to toxiproxy because the broker
advertises its own listener address, so clients reconnect around the proxy
(see ADR 0010).

Enrichment fetcher *breakers* are covered creditlessly in test_breaker_chaos.py
(in-process fault injection); this file covers infra-dependency outages.
"""

from __future__ import annotations

import os
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer
from toxiproxy import Toxiproxy
from toxiproxy.proxy import Proxy

GENERIC_SECRET = "chaos-secret"
_TOXIPROXY_IMAGE = os.environ.get(
    "SENTINEL_TEST_TOXIPROXY_IMAGE", "ghcr.io/shopify/toxiproxy:2.9.0"
)
_REDIS_PROXY_PORT = 16379  # toxiproxy listen port (inside container), exposed to host


@dataclass
class ChaosStack:
    pg_dsn: str
    redis_url: str  # points at the toxiproxy proxy, not Redis directly
    kafka_brokers: str
    redis_proxy: Proxy


def _async_dsn(pg: PostgresContainer) -> str:
    raw: str = pg.get_connection_url()
    return raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )


@pytest.fixture(scope="session")
def chaos_stack() -> Iterator[ChaosStack]:
    with (
        Network() as net,
        PostgresContainer("pgvector/pgvector:pg16", driver="asyncpg") as pg,
        KafkaContainer(
            os.environ.get("SENTINEL_TEST_KAFKA_IMAGE", "confluentinc/cp-kafka:7.8.0")
        ) as kafka,
        RedisContainer("redis:7-alpine")
        .with_network(net)
        .with_network_aliases("redisupstream") as _redis,
        DockerContainer(_TOXIPROXY_IMAGE)
        .with_network(net)
        .with_command("-host=0.0.0.0")
        .with_exposed_ports(8474, _REDIS_PROXY_PORT) as toxi,
    ):
        # Apply migrations against Postgres directly (the app talks to it direct too).
        from alembic import command
        from alembic.config import Config

        pg_dsn = _async_dsn(pg)
        cfg = Config("alembic.ini")
        cfg.set_main_option("sqlalchemy.url", pg_dsn)
        command.upgrade(cfg, "head")

        # Wire the toxiproxy client to the container's exposed API and wait for it.
        api_host = toxi.get_container_host_ip()
        api_port = int(toxi.get_exposed_port(8474))
        client = Toxiproxy()
        client.update_api_consumer(api_host, api_port)
        ready = False
        last_exc: Exception | None = None
        for _ in range(50):
            try:
                ready = bool(client.running())
            except Exception as exc:
                last_exc = exc
                ready = False
            if ready:
                break
            time.sleep(0.2)
        if not ready:  # pragma: no cover - infra failure
            raise RuntimeError(
                f"toxiproxy API never became ready (last error: {last_exc!r})"
            ) from last_exc

        redis_proxy = client.create(
            upstream="redisupstream:6379",
            name="redis",
            listen=f"0.0.0.0:{_REDIS_PROXY_PORT}",
            enabled=True,
        )
        proxy_host = toxi.get_container_host_ip()
        proxy_port = int(toxi.get_exposed_port(_REDIS_PROXY_PORT))
        redis_url = f"redis://{proxy_host}:{proxy_port}/0"

        yield ChaosStack(
            pg_dsn=pg_dsn,
            redis_url=redis_url,
            kafka_brokers=str(kafka.get_bootstrap_server()),
            redis_proxy=redis_proxy,
        )


@pytest_asyncio.fixture(autouse=True)
async def _reset(chaos_stack: ChaosStack) -> None:
    """Per-test: heal the proxy, clear toxics, truncate PG, flush Redis."""
    chaos_stack.redis_proxy.enable()
    for toxic in list(chaos_stack.redis_proxy.toxics().values()):
        chaos_stack.redis_proxy.destroy_toxic(toxic.name)

    engine = create_async_engine(chaos_stack.pg_dsn)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE incidents, outbox_events CASCADE"))
    finally:
        await engine.dispose()

    from redis.asyncio import Redis

    redis = Redis.from_url(chaos_stack.redis_url)
    try:
        await redis.flushdb()
    finally:
        await redis.aclose()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch, chaos_stack: ChaosStack) -> None:
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", chaos_stack.pg_dsn)
    monkeypatch.setenv("SENTINEL_REDIS_URL", chaos_stack.redis_url)
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", chaos_stack.kafka_brokers)
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "x")
    monkeypatch.setenv("SENTINEL_GENERIC_WEBHOOK_SECRET", GENERIC_SECRET)
    # No diagnosis/memory consumers — chaos traffic must not reach Anthropic.
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
