"""FastAPI application factory and uvicorn entrypoint."""

from __future__ import annotations

import asyncio
import contextlib
import logging
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
    OutboxRepository,
    PostgresIncidentRepository,
    PostgresOutboxRepository,
)
from sentinel.persistence.session import make_async_engine, make_session_factory

log = logging.getLogger(__name__)


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
    gauge_task = asyncio.create_task(_refresh_outbox_gauges(outbox_repo), name="outbox-gauges")

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
        # Shutdown sequence: drainer first (so it stops claiming new rows),
        # then producer/redis/engine in parallel via gather. We collect
        # exceptions so a failure in one cleanup doesn't mask the others —
        # graceful shutdown means *all* resources get a chance to release.
        drainer.stop()
        gauge_task.cancel()
        try:
            await asyncio.wait_for(drainer_task, timeout=5.0)
        except TimeoutError:
            drainer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drainer_task
        with contextlib.suppress(asyncio.CancelledError):
            await gauge_task

        results = await asyncio.gather(
            producer.stop(),
            redis.aclose(),
            engine.dispose(),
            return_exceptions=True,
        )
        for r in results:
            if isinstance(r, BaseException):
                log.exception("lifespan_shutdown_cleanup_failed", exc_info=r)


async def _refresh_outbox_gauges(outbox_repo: OutboxRepository, interval: float = 30.0) -> None:
    """Periodically populate outbox monitoring gauges from a cheap aggregate query.

    `outbox_unpublished_count` and `outbox_oldest_unpublished_age_seconds`
    would otherwise be declared-but-never-set, which is worse than missing —
    dashboards built on them would render valid-looking zero series. The
    repository's `unpublished_stats()` issues a single aggregate query against
    the partial `idx_outbox_unpublished` index.
    """
    from sentinel.observability.metrics import (
        outbox_oldest_unpublished_age_seconds,
        outbox_unpublished_count,
    )

    while True:
        try:
            count, age_seconds = await outbox_repo.unpublished_stats()
            outbox_unpublished_count.set(float(count))
            outbox_oldest_unpublished_age_seconds.set(float(age_seconds))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("outbox_gauge_refresh_failed")
        await asyncio.sleep(interval)


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
