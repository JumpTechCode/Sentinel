from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sentinel.api.app import build_app


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
        patch("sentinel.api.app.PromptBundle") as MockPromptBundle,
        patch("sentinel.api.app._refresh_outbox_gauges") as MockGauges,
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

        # AIOKafkaConsumer is called twice: once for enricher, once for diagnoser.
        # Each call returns a distinct mock so start/stop counts are per-instance.
        enrich_consumer_instance = MagicMock()
        enrich_consumer_instance.start = AsyncMock()
        enrich_consumer_instance.stop = AsyncMock()
        diag_consumer_instance = MagicMock()
        diag_consumer_instance.start = AsyncMock()
        diag_consumer_instance.stop = AsyncMock()
        MockConsumer.side_effect = [enrich_consumer_instance, diag_consumer_instance]

        enricher_instance = MockEnricher.return_value
        enricher_instance.run = AsyncMock()
        enricher_instance.stop = MagicMock()

        diagnoser_instance = MockDiagnoser.return_value
        diagnoser_instance.run = AsyncMock()
        diagnoser_instance.stop = MagicMock()

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

        prod_instance.start.assert_awaited_once()
        prod_instance.stop.assert_awaited_once()
        drainer_instance.stop.assert_called_once()
        enrich_consumer_instance.start.assert_awaited_once()
        enrich_consumer_instance.stop.assert_awaited_once()
        enricher_instance.stop.assert_called_once()
        diag_consumer_instance.start.assert_awaited_once()
        diag_consumer_instance.stop.assert_awaited_once()
        diagnoser_instance.stop.assert_called_once()
