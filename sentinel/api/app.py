"""FastAPI application factory and uvicorn entrypoint."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from sentinel import __version__
from sentinel.api.routes.health import router as health_router
from sentinel.api.routes.webhooks import router as webhooks_router
from sentinel.config.settings import load_settings
from sentinel.ingestion.idempotency import RedisIdempotencyStore
from sentinel.ingestion.kafka_producer import KafkaProducer
from sentinel.ingestion.outbox_drainer import OutboxDrainer
from sentinel.ingestion.webhook import WebhookHandler
from sentinel.persistence.repositories import (
    PostgresIncidentRepository,
    PostgresOutboxRepository,
)
from sentinel.persistence.session import make_async_engine, make_session_factory


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()

    engine = make_async_engine(settings)
    session_factory = make_session_factory(engine)
    incident_repo = PostgresIncidentRepository(session_factory)
    outbox_repo = PostgresOutboxRepository(session_factory)

    redis = Redis.from_url(settings.redis_url)
    idempotency = RedisIdempotencyStore(redis)

    producer = KafkaProducer(settings.kafka_brokers)
    await producer.start()
    drainer = OutboxDrainer(outbox_repo=outbox_repo, producer=producer)
    drainer_task = asyncio.create_task(drainer.run(), name="outbox-drainer")

    handler = WebhookHandler(
        incident_repo=incident_repo,
        idempotency=idempotency,
        settings=settings,
        outbox_topic=settings.kafka_topic_incidents,
    )

    app.state.webhook_handler = handler
    app.state.kafka_producer = producer
    app.state.outbox_drainer = drainer
    app.state.engine = engine
    app.state.redis = redis

    try:
        yield
    finally:
        drainer.stop()
        try:
            await asyncio.wait_for(drainer_task, timeout=5.0)
        except TimeoutError:
            drainer_task.cancel()
        await producer.stop()
        await redis.aclose()
        await engine.dispose()


def build_app() -> FastAPI:
    app = FastAPI(
        title="Sentinel",
        version=__version__,
        description=(
            "AI on-call copilot — alert ingestion, context assembly, " "evidence-cited diagnosis."
        ),
        lifespan=lifespan,
    )
    app.include_router(health_router)
    app.include_router(webhooks_router)
    return app


def run() -> None:
    import uvicorn

    settings = load_settings()
    uvicorn.run(
        "sentinel.api.app:build_app",
        factory=True,
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":  # pragma: no cover
    run()
