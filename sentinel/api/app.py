"""FastAPI application factory and uvicorn entrypoint."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from redis.asyncio import Redis

from sentinel import __version__
from sentinel.api.routes.health import router as health_router
from sentinel.api.routes.resolve import router as resolve_router
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
from sentinel.enrichment.orchestrator import Fetcher
from sentinel.ingestion.idempotency import RedisIdempotencyStore
from sentinel.ingestion.kafka_producer import KafkaProducer
from sentinel.ingestion.outbox_drainer import OUTBOX_MAX_ATTEMPTS, OutboxDrainer
from sentinel.ingestion.webhook import WebhookHandler
from sentinel.memory import (
    FastEmbedProvider,
    MemoryConsumer,
    MemoryConsumerDeps,
    MemoryPipeline,
    PgVectorIncidentStore,
)
from sentinel.observability.llm_audit import LLMAuditLogger
from sentinel.observability.metrics import consumer_crashed_total
from sentinel.observability.tracing import configure_tracing
from sentinel.persistence.repositories import (
    OutboxRepository,
    PostgresDeployRepository,
    PostgresDiagnosisRepository,
    PostgresIncidentRepository,
    PostgresOutboxRepository,
    PostgresResolutionRepository,
)
from sentinel.persistence.session import make_async_engine, make_session_factory

if TYPE_CHECKING:
    from sentinel.evals.cassette import CassetteTransport

log = logging.getLogger(__name__)


async def _start_then_run(
    kafka: AIOKafkaConsumer,
    run: Callable[[], Awaitable[None]],
    name: str,
) -> None:
    """Start an aiokafka consumer and then drive its wrapper's `run()` loop.

    Calling `AIOKafkaConsumer.start()` directly inside `lifespan` blocks until
    the broker is reachable AND a group coordinator is found. On a freshly
    booted Kafka container the coordinator can take 30s+ to provision, during
    which `FindCoordinator` retries silently (gh#36). Wrapping start+run in a
    background task lets `lifespan` return immediately so `/healthz` is ready
    while the consumer comes online in the background.

    Exceptions from `start()` and `run()` both propagate out of this coroutine
    so the asyncio.Task carries the failure — a `task.add_done_callback`
    attached in `lifespan` observes `task.exception()` and flips the
    per-consumer aliveness flag exposed via `/readyz`. Swallowing here would
    make `task.exception()` return None and silently lose the failure.
    Cancellation (graceful shutdown) propagates as `CancelledError`.
    """
    try:
        await kafka.start()
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("kafka_consumer_start_failed", extra={"consumer": name})
        raise
    await run()


def _on_consumer_task_done(
    name: str,
    app: FastAPI,
) -> Callable[[asyncio.Task[None]], None]:
    """Build a done-callback that records consumer death.

    Consumer tasks exit under three conditions:

    1. **Cancelled** — graceful shutdown via task.cancel(). No-op.
    2. **Clean return during shutdown** — `consumer.run()` saw its `_stop`
       flag set and returned cleanly. No-op (gated on `app.state.shutting_down`
       which the lifespan finally-block sets before calling `consumer.stop()`).
       Without this gate, every graceful shutdown would spam the crash metric
       and ERROR-log three lines per restart, destroying the signal value of
       `sentinel_consumer_crashed_total`.
    3. **Anything else** — start failure, run-loop crash past the inner
       `except Exception`, or a clean return outside shutdown (a bug in the
       consumer wrapper). All three are operationally a wedged stream:
       flip aliveness, bump the crash counter, log ERROR with exc_info.
    """

    def _callback(task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        if getattr(app.state, "shutting_down", False):
            return
        alive = getattr(app.state, "consumer_alive", None)
        if isinstance(alive, dict):
            alive[name] = False
        consumer_crashed_total.labels(consumer=name).inc()
        exc = task.exception()
        if exc is not None:
            log.error(
                "kafka_consumer_task_crashed",
                extra={"consumer": name},
                exc_info=exc,
            )
        else:
            log.error(
                "kafka_consumer_task_exited_unexpectedly",
                extra={"consumer": name},
            )

    return _callback


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()
    configure_tracing(settings)

    engine = make_async_engine(settings)
    session_factory = make_session_factory(engine)
    incident_repo = PostgresIncidentRepository(session_factory)
    outbox_repo = PostgresOutboxRepository(session_factory)

    # Resolve route deps (independent of memory_consumer_enabled).
    resolution_repo = PostgresResolutionRepository(session_factory)
    app.state.resolution_repo = resolution_repo
    app.state.outbox_topic = settings.kafka_topic_incidents

    # Aliveness map for background Kafka consumers. Populated below per-stream
    # and flipped to False by `_on_consumer_task_done` if the task dies. /readyz
    # returns 503 if any entry is False or if the dict is missing/empty so
    # readiness gates don't route traffic to a pod that's lost a consumer.
    app.state.consumer_alive = {}
    # Shutdown gate read by `_on_consumer_task_done`: a clean `run()` return
    # while shutting_down is True is the expected outcome of `consumer.stop()`
    # and must NOT be counted as a crash.
    app.state.shutting_down = False

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

    fetchers: tuple[Fetcher, ...]
    if settings.eval_mode:
        from sentinel.evals.fetcher_override import corpus_fetchers
        from sentinel.evals.registry import REGISTRY

        fetchers = corpus_fetchers(REGISTRY)
    else:
        fetchers = default_fetchers()
    breakers = {f.name: make_breaker(f.name) for f in fetchers}

    # ---- Memory (Phase 4) --------------------------------------------------
    memory_consumer: MemoryConsumer | None = None
    memory_task: asyncio.Task[None] | None = None
    mem_kafka_consumer: AIOKafkaConsumer | None = None
    similar_incidents_store: PgVectorIncidentStore | NotConfiguredSimilarIncidents = (
        NotConfiguredSimilarIncidents()
    )

    if settings.memory_consumer_enabled:
        embedding_provider = FastEmbedProvider(
            model_cache_dir=Path(settings.embedding_model_cache_dir),
            compute_timeout_s=settings.embedding_compute_timeout_seconds,
            model_name=settings.embedding_model_name,
        )

        # CORRECTED constructor (Task 5 refactored to repo-delegation):
        similar_incidents_store = PgVectorIncidentStore(
            incident_repo=incident_repo,
            embedding_provider=embedding_provider,
        )

        mem_kafka_consumer = AIOKafkaConsumer(
            settings.kafka_topic_incidents,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.kafka_consumer_group_memory,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

        memory_deps = MemoryConsumerDeps(
            incident_repo=incident_repo,
            embedding_provider=embedding_provider,
            pipeline=MemoryPipeline(),
        )
        memory_consumer = MemoryConsumer(consumer=mem_kafka_consumer, deps=memory_deps)
        memory_task = asyncio.create_task(
            _start_then_run(mem_kafka_consumer, memory_consumer.run, "memory-consumer"),
            name="memory-consumer",
        )
        app.state.consumer_alive["memory"] = True
        memory_task.add_done_callback(_on_consumer_task_done("memory", app))
        app.state.memory_consumer = memory_consumer

    enrich_deps = EnrichmentDeps(
        fetchers=fetchers,
        breakers=breakers,
        incident_repo=incident_repo,
        deploy_repo=deploy_repo,
        similar_incidents=similar_incidents_store,
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
    enricher = EnrichmentConsumer(
        consumer=kafka_consumer,
        deps=enrich_deps,
        assemble_fn=assemble,
        topic=settings.kafka_topic_incidents,
        enriched_topic=settings.kafka_topic_incidents,
    )
    enricher_task = asyncio.create_task(
        _start_then_run(kafka_consumer, enricher.run, "enrichment-consumer"),
        name="enrichment-consumer",
    )
    app.state.consumer_alive["enrichment"] = True
    enricher_task.add_done_callback(_on_consumer_task_done("enrichment", app))
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
    cassette_http_client: httpx.AsyncClient | None = None
    cassette_transport: CassetteTransport | None = None
    if settings.diagnosis_consumer_enabled:
        diag_kafka_consumer = AIOKafkaConsumer(
            settings.kafka_topic_incidents,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.kafka_consumer_group_diagnoser,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )

        if settings.eval_mode and settings.eval_cassette_dir is not None:
            from sentinel.evals.cassette import CassetteTransport

            cassette_transport = CassetteTransport(
                mode=settings.eval_cassette_mode,
                cassette_dir=settings.eval_cassette_dir,
            )
            cassette_http_client = httpx.AsyncClient(transport=cassette_transport)

        llm_client = AnthropicClient(
            api_key=settings.anthropic_api_key,
            model=settings.anthropic_model,
            timeout_s=settings.diagnosis_llm_timeout_seconds,
            max_output_tokens=settings.diagnosis_max_output_tokens,
            http_client=cassette_http_client,
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
        diagnosis_task = asyncio.create_task(
            _start_then_run(diag_kafka_consumer, diagnoser.run, "diagnosis-consumer"),
            name="diagnosis-consumer",
        )
        app.state.consumer_alive["diagnosis"] = True
        diagnosis_task.add_done_callback(_on_consumer_task_done("diagnosis", app))
        app.state.diagnosis_consumer = diagnoser
    elif settings.eval_mode:
        # Eval mode requires a diagnosis consumer to wrap the LLM client with
        # the cassette transport. Without it, the runner has no AnthropicClient
        # to drive — surface this as a loud warning rather than failing silently.
        log.warning(
            "eval_mode_skipping_cassette_wiring",
            extra={"reason": "diagnosis_consumer_enabled=False"},
        )

    # Stashed unconditionally (None when not eval mode or no cassette dir) so
    # the runner can rely on `app.state.cassette_transport` existing.
    app.state.cassette_transport = cassette_transport

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
        # Flip the shutdown gate BEFORE calling any consumer.stop(): once
        # consumer.stop() lands, run() may return cleanly within the same event
        # loop tick, firing the done_callback. Without `shutting_down=True`
        # set first, that clean return would be mis-counted as a crash.
        app.state.shutting_down = True

        # Shutdown sequence: diagnoser and enricher first (both consume from
        # Kafka and may write rows via repos), then drainer (stops claiming new
        # outbox rows), then producer/redis/engine in parallel via gather. We
        # collect exceptions so a failure in one cleanup doesn't mask the
        # others — graceful shutdown means *all* resources get a chance to
        # release.
        # Consumer tasks may already be `done()` with an exception (start
        # failure or run-loop crash, surfaced via `_on_consumer_task_done`).
        # `await`ing such a task re-raises that exception — the done_callback
        # has already logged it and bumped `consumer_crashed_total`, so we
        # suppress here to keep cleanup going.
        if diagnoser is not None and diagnosis_task is not None and diag_kafka_consumer is not None:
            diagnoser.stop()
            try:
                await asyncio.wait_for(diagnosis_task, timeout=5.0)
            except TimeoutError:
                diagnosis_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await diagnosis_task
            except Exception:  # noqa: S110 — done_callback already logged + bumped crash metric
                pass
            with contextlib.suppress(Exception):
                await diag_kafka_consumer.stop()

        # Cassette-wrapped httpx client is owned by lifespan when constructed;
        # close it after the diagnosis consumer (which holds the AnthropicClient
        # that uses it) has stopped to avoid in-flight request cancellation.
        if cassette_http_client is not None:
            with contextlib.suppress(Exception):
                await cassette_http_client.aclose()

        if (
            memory_consumer is not None
            and memory_task is not None
            and mem_kafka_consumer is not None
        ):
            memory_consumer.stop()
            try:
                await asyncio.wait_for(memory_task, timeout=5.0)
            except TimeoutError:
                memory_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await memory_task
            except Exception:  # noqa: S110 — done_callback already logged + bumped crash metric
                pass
            with contextlib.suppress(Exception):
                await mem_kafka_consumer.stop()

        enricher.stop()
        try:
            await asyncio.wait_for(enricher_task, timeout=5.0)
        except TimeoutError:
            enricher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await enricher_task
        except Exception:  # noqa: S110 — done_callback already logged + bumped crash metric
            pass
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


async def _refresh_outbox_gauges(
    outbox_repo: OutboxRepository,
    interval: float = 30.0,
    max_attempts: int = OUTBOX_MAX_ATTEMPTS,
) -> None:
    """Periodically populate outbox monitoring gauges from cheap aggregate queries.

    `outbox_unpublished_count`, `outbox_oldest_unpublished_age_seconds`, and
    `outbox_stuck_events_count` would otherwise be declared-but-never-set,
    which is worse than missing — dashboards built on them would render
    valid-looking zero series. `unpublished_stats()` and `stuck_count()`
    each issue a single aggregate query against the partial
    `idx_outbox_unpublished` index.
    """
    from sentinel.observability.metrics import (
        outbox_oldest_unpublished_age_seconds,
        outbox_stuck_events_count,
        outbox_unpublished_count,
    )

    while True:
        try:
            count, age_seconds = await outbox_repo.unpublished_stats()
            stuck = await outbox_repo.stuck_count(max_attempts=max_attempts)
            outbox_unpublished_count.set(float(count))
            outbox_oldest_unpublished_age_seconds.set(float(age_seconds))
            outbox_stuck_events_count.set(float(stuck))
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
    app.include_router(resolve_router)
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
