"""FastAPI application factory and uvicorn entrypoint."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from redis.asyncio import Redis

from sentinel import __version__
from sentinel.api.routes.health import router as health_router
from sentinel.api.routes.webhooks import router as webhooks_router
from sentinel.config.settings import load_settings
from sentinel.diagnosis.agent import diagnose as diagnose_fn
from sentinel.diagnosis.consumer import DiagnosisConsumer
from sentinel.diagnosis.deps import ConsumerDeps as DiagnosisConsumerDeps
from sentinel.diagnosis.llm_client import AnthropicClient
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.enrichment import (
    EnrichmentDeps,
    assemble,
    default_fetchers,
    make_breaker,
)
from sentinel.enrichment.consumer import EnrichmentConsumer
from sentinel.enrichment.defaults import (
    NotConfiguredActiveAlerts,
    NotConfiguredLogSearch,
    NotConfiguredRunbookRetrieval,
    NotConfiguredSimilarIncidents,
)
from sentinel.ingestion.idempotency import RedisIdempotencyStore
from sentinel.ingestion.kafka_producer import KafkaProducer
from sentinel.ingestion.outbox_drainer import OutboxDrainer
from sentinel.ingestion.webhook import WebhookHandler
from sentinel.observability.llm_audit import LLMAuditLogger
from sentinel.observability.tracing import configure_tracing
from sentinel.persistence.repositories import (
    OutboxRepository,
    PostgresDeployRepository,
    PostgresDiagnosisRepository,
    PostgresIncidentRepository,
    PostgresOutboxRepository,
)
from sentinel.persistence.session import make_async_engine, make_session_factory

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()
    configure_tracing(settings)

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

    # ---- Enrichment ----------------------------------------------------
    # Unconditional startup (matches Phase 3 drainer pattern). The consumer
    # depends on `incident_repo` constructed above; placing it after the
    # drainer keeps shutdown ordering simple (enricher first, drainer second).
    deploy_repo = PostgresDeployRepository(session_factory)

    fetchers = default_fetchers()
    breakers = {f.name: make_breaker(f.name) for f in fetchers}

    enrich_deps = EnrichmentDeps(
        fetchers=fetchers,
        breakers=breakers,
        incident_repo=incident_repo,
        deploy_repo=deploy_repo,
        similar_incidents=NotConfiguredSimilarIncidents(),
        runbooks=NotConfiguredRunbookRetrieval(),
        log_search=NotConfiguredLogSearch(),
        active_alerts=NotConfiguredActiveAlerts(),
    )

    kafka_consumer = AIOKafkaConsumer(
        settings.kafka_topic_incidents,
        bootstrap_servers=settings.kafka_brokers,
        group_id=settings.kafka_consumer_group_enricher,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await kafka_consumer.start()
    enricher = EnrichmentConsumer(
        consumer=kafka_consumer,
        deps=enrich_deps,
        assemble_fn=assemble,
        topic=settings.kafka_topic_incidents,
        enriched_topic=settings.kafka_topic_incidents,
    )
    enricher_task = asyncio.create_task(enricher.run(), name="enrichment-consumer")
    app.state.enrichment_consumer = enricher

    # ---- Diagnosis ---------------------------------------------------------
    # Guarded by `diagnosis_consumer_enabled` so it can be disabled in envs
    # that lack Anthropic credentials (e.g., integration-test runs that only
    # exercise ingestion/enrichment). The consumer subscribes to the same
    # `sentinel.incidents` topic as the enricher but uses a distinct group ID
    # so both receive every event independently.
    diagnosis_task: asyncio.Task[None] | None = None
    diag_kafka_consumer: AIOKafkaConsumer | None = None
    diagnoser: DiagnosisConsumer | None = None
    if settings.diagnosis_consumer_enabled:
        diag_kafka_consumer = AIOKafkaConsumer(
            settings.kafka_topic_incidents,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.kafka_consumer_group_diagnoser,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await diag_kafka_consumer.start()

        llm_client = AnthropicClient(
            api_key=settings.anthropic_api_key,
            model=settings.anthropic_model,
            timeout_s=settings.diagnosis_llm_timeout_seconds,
            max_output_tokens=settings.diagnosis_max_output_tokens,
        )
        prompt_bundle = PromptBundle.load(settings.diagnosis_prompt_version)
        diagnosis_repo = PostgresDiagnosisRepository(session_factory)
        audit_logger = LLMAuditLogger(settings.llm_audit_log_path)

        diagnosis_deps = DiagnosisConsumerDeps(
            llm=llm_client,
            prompt=prompt_bundle,
            max_input_tokens=settings.diagnosis_max_input_tokens,
            incident_repo=incident_repo,
            diagnosis_repo=diagnosis_repo,
            audit_logger=audit_logger,
        )
        diagnoser = DiagnosisConsumer(
            consumer=diag_kafka_consumer,
            deps=diagnosis_deps,
            agent_fn=diagnose_fn,
        )
        diagnosis_task = asyncio.create_task(diagnoser.run(), name="diagnosis-consumer")
        app.state.diagnosis_consumer = diagnoser

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
        # Shutdown sequence: diagnoser and enricher first (both consume from
        # Kafka and may write rows via repos), then drainer (stops claiming new
        # outbox rows), then producer/redis/engine in parallel via gather. We
        # collect exceptions so a failure in one cleanup doesn't mask the
        # others — graceful shutdown means *all* resources get a chance to
        # release.
        if diagnoser is not None and diagnosis_task is not None and diag_kafka_consumer is not None:
            diagnoser.stop()
            try:
                await asyncio.wait_for(diagnosis_task, timeout=5.0)
            except TimeoutError:
                diagnosis_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await diagnosis_task
            with contextlib.suppress(Exception):
                await diag_kafka_consumer.stop()

        enricher.stop()
        try:
            await asyncio.wait_for(enricher_task, timeout=5.0)
        except TimeoutError:
            enricher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await enricher_task
        with contextlib.suppress(Exception):
            await kafka_consumer.stop()

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
