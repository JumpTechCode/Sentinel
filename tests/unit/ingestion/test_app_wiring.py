from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sentinel.api.app import _refresh_outbox_gauges, build_app
from sentinel.observability import metrics as M


@pytest.mark.asyncio
async def test_lifespan_starts_and_stops_producer_and_drainer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Stub out heavy infra so lifespan can run without docker.
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://x/y")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "localhost:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "x")

    with (
        patch("sentinel.api.app.KafkaProducer") as MockProducer,
        patch("sentinel.api.app.OutboxDrainer") as MockDrainer,
        patch("sentinel.api.app.make_async_engine") as MockEngine,
        patch("sentinel.api.app.Redis") as MockRedis,
        patch("sentinel.api.app.AIOKafkaConsumer") as MockConsumer,
        patch("sentinel.api.app.EnrichmentConsumer") as MockEnricher,
        patch("sentinel.api.app.DiagnosisConsumer") as MockDiagnoser,
        patch("sentinel.api.app.MemoryConsumer") as MockMemoryConsumer,
        patch("sentinel.api.app.FastEmbedProvider") as MockEmbedProvider,
        patch("sentinel.api.app.PromptBundle") as MockPromptBundle,
        patch("sentinel.api.app._refresh_outbox_gauges") as MockGauges,
        patch("sentinel.api.app.configure_observability") as MockConfigureObs,
    ):
        prod_instance = MockProducer.return_value
        prod_instance.start = AsyncMock()
        prod_instance.stop = AsyncMock()

        drainer_instance = MockDrainer.return_value
        drainer_instance.run = AsyncMock()
        drainer_instance.stop = MagicMock()

        engine_instance = MockEngine.return_value
        engine_instance.dispose = AsyncMock()

        redis_instance = MagicMock()
        redis_instance.aclose = AsyncMock()
        MockRedis.from_url = MagicMock(return_value=redis_instance)

        # AIOKafkaConsumer is called three times: memory consumer first (Phase 4
        # block runs before enricher), then enricher, then diagnoser. Each call
        # returns a distinct mock so start/stop counts are per-instance.
        mem_consumer_instance = MagicMock()
        mem_consumer_instance.start = AsyncMock()
        mem_consumer_instance.stop = AsyncMock()
        enrich_consumer_instance = MagicMock()
        enrich_consumer_instance.start = AsyncMock()
        enrich_consumer_instance.stop = AsyncMock()
        diag_consumer_instance = MagicMock()
        diag_consumer_instance.start = AsyncMock()
        diag_consumer_instance.stop = AsyncMock()
        MockConsumer.side_effect = [
            mem_consumer_instance,
            enrich_consumer_instance,
            diag_consumer_instance,
        ]

        enricher_instance = MockEnricher.return_value
        enricher_instance.run = AsyncMock()
        enricher_instance.stop = MagicMock()

        diagnoser_instance = MockDiagnoser.return_value
        diagnoser_instance.run = AsyncMock()
        diagnoser_instance.stop = MagicMock()

        memory_consumer_instance = MockMemoryConsumer.return_value
        memory_consumer_instance.run = AsyncMock()
        memory_consumer_instance.stop = MagicMock()

        MockEmbedProvider.return_value = MagicMock()
        MockPromptBundle.load = MagicMock(return_value=MagicMock())

        async def _noop_gauges(*args: object, **kwargs: object) -> None:
            return None

        MockGauges.side_effect = _noop_gauges

        app = build_app()
        async with app.router.lifespan_context(app):
            assert hasattr(app.state, "webhook_handler")
            assert hasattr(app.state, "kafka_producer")
            assert hasattr(app.state, "outbox_drainer")
            assert hasattr(app.state, "enrichment_consumer")
            assert hasattr(app.state, "diagnosis_consumer")
            assert hasattr(app.state, "memory_consumer")

        # gh#64: the lifespan must wire observability (logging + tracing), not
        # just tracing as it did before.
        MockConfigureObs.assert_called_once()
        prod_instance.start.assert_awaited_once()
        prod_instance.stop.assert_awaited_once()
        drainer_instance.stop.assert_called_once()
        mem_consumer_instance.start.assert_awaited_once()
        mem_consumer_instance.stop.assert_awaited_once()
        memory_consumer_instance.stop.assert_called_once()
        enrich_consumer_instance.start.assert_awaited_once()
        enrich_consumer_instance.stop.assert_awaited_once()
        enricher_instance.stop.assert_called_once()
        diag_consumer_instance.start.assert_awaited_once()
        diag_consumer_instance.stop.assert_awaited_once()
        diagnoser_instance.stop.assert_called_once()


@pytest.mark.asyncio
async def test_refresh_outbox_gauges_sets_stuck_count_gauge() -> None:
    repo = MagicMock()
    repo.unpublished_stats = AsyncMock(return_value=(5, 1.5))
    repo.stuck_count = AsyncMock(return_value=3)

    task = asyncio.create_task(_refresh_outbox_gauges(repo, interval=3600.0, max_attempts=7))
    # Yield long enough for the first iteration to run, then cancel.
    for _ in range(50):
        if repo.stuck_count.await_count >= 1:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    repo.stuck_count.assert_awaited_with(max_attempts=7)
    assert M.outbox_stuck_events_count._value.get() == 3.0
    assert M.outbox_unpublished_count._value.get() == 5.0
    assert M.outbox_oldest_unpublished_age_seconds._value.get() == 1.5
