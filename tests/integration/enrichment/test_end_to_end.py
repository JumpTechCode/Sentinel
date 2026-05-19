# tests/integration/enrichment/test_end_to_end.py
"""End-to-end: incident.opened on Kafka → context_json on row → incident.enriched in outbox."""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
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
from sentinel.persistence.models import OutboxEventModel
from sentinel.persistence.repositories import (
    PostgresDeployRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.alert import NormalizedAlert
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

pytestmark = pytest.mark.integration


async def _setup_enricher(
    pg_dsn: str, kafka_brokers: str, topic: str
) -> tuple[
    AsyncEngine,
    async_sessionmaker[AsyncSession],
    PostgresIncidentRepository,
    AIOKafkaConsumer,
    EnrichmentConsumer,
]:
    engine = create_async_engine(pg_dsn)
    sf = make_session_factory(engine)
    incident_repo = PostgresIncidentRepository(sf)
    deploy_repo = PostgresDeployRepository(sf)
    fetchers = default_fetchers()
    deps = EnrichmentDeps(
        fetchers=fetchers,
        breakers={f.name: make_breaker(f.name) for f in fetchers},
        incident_repo=incident_repo,
        deploy_repo=deploy_repo,
        similar_incidents=NotConfiguredSimilarIncidents(),
        runbooks=NotConfiguredRunbookRetrieval(),
        log_search=NotConfiguredLogSearch(),
        active_alerts=NotConfiguredActiveAlerts(),
    )

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_brokers,
        group_id=f"test-{uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=deps,
        assemble_fn=assemble,
        topic=topic,
        enriched_topic=topic,
    )
    return engine, sf, incident_repo, consumer, enricher


@pytest.mark.asyncio
async def test_end_to_end_writes_context_and_emits_enriched(
    pg_dsn: str,
    kafka_brokers: str,
) -> None:
    topic = "sentinel.incidents"
    engine, sf, incident_repo, consumer, enricher = await _setup_enricher(
        pg_dsn,
        kafka_brokers,
        topic,
    )
    try:
        alert = NormalizedAlert(
            source="generic",
            external_id="ext-e2e",
            service="api",
            severity="SEV2",
            title="boom",
            raw_payload={},
            received_at=datetime.now(UTC),
        )
        incident_id = await incident_repo.create_from_alert(alert, fingerprint="fp-e2e")

        producer = AIOKafkaProducer(bootstrap_servers=kafka_brokers)
        await producer.start()
        try:
            event_id = uuid4()
            await producer.send_and_wait(
                topic,
                value=json.dumps(
                    {
                        "event_id": str(event_id),
                        "event": "incident.opened",
                        "incident_id": str(incident_id),
                        "fingerprint": "fp-e2e",
                        "source": "generic",
                        "ts": datetime.now(UTC).isoformat(),
                    }
                ).encode(),
                key=str(incident_id).encode(),
            )
        finally:
            await producer.stop()

        runner = asyncio.create_task(enricher.run())
        try:
            stored = None
            for _ in range(120):
                stored = await incident_repo.get_enrichment_context(incident_id)
                if stored is not None:
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("context_json was never written")

            assert stored is not None
            assert stored.version == 1
            assert stored.context.incident_id == incident_id

            # Outbox row for incident.enriched
            async with sf() as s:
                rows = (
                    (
                        await s.execute(
                            select(OutboxEventModel).where(OutboxEventModel.topic == topic)
                        )
                    )
                    .scalars()
                    .all()
                )
                enriched = [r for r in rows if r.payload.get("event") == "incident.enriched"]
                assert len(enriched) == 1
                assert enriched[0].payload["incident_id"] == str(incident_id)
        finally:
            enricher.stop()
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner
    finally:
        await consumer.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_replay_same_event_id_is_noop(
    pg_dsn: str,
    kafka_brokers: str,
) -> None:
    topic = "sentinel.incidents"
    engine, sf, incident_repo, consumer, enricher = await _setup_enricher(
        pg_dsn,
        kafka_brokers,
        topic,
    )
    try:
        alert = NormalizedAlert(
            source="generic",
            external_id="ext-replay",
            service="api",
            severity="SEV2",
            title="boom",
            raw_payload={},
            received_at=datetime.now(UTC),
        )
        incident_id = await incident_repo.create_from_alert(alert, fingerprint="fp-replay")

        event_id = uuid4()
        producer = AIOKafkaProducer(bootstrap_servers=kafka_brokers)
        await producer.start()
        body = json.dumps(
            {
                "event_id": str(event_id),
                "event": "incident.opened",
                "incident_id": str(incident_id),
                "fingerprint": "fp-replay",
                "source": "generic",
                "ts": datetime.now(UTC).isoformat(),
            }
        ).encode()
        try:
            await producer.send_and_wait(topic, value=body, key=str(incident_id).encode())
            await producer.send_and_wait(topic, value=body, key=str(incident_id).encode())
        finally:
            await producer.stop()

        runner = asyncio.create_task(enricher.run())
        try:
            stored = None
            for _ in range(120):
                stored = await incident_repo.get_enrichment_context(incident_id)
                if stored is not None and stored.version >= 1:
                    break
                await asyncio.sleep(0.1)
            # Give the consumer time to process the second (duplicate) delivery
            # before asserting the version did not advance.
            await asyncio.sleep(1.0)
            stored = await incident_repo.get_enrichment_context(incident_id)
            assert stored is not None
            assert stored.version == 1  # not 2 — duplicate was a no-op
        finally:
            enricher.stop()
            runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await runner
    finally:
        await consumer.stop()
        await engine.dispose()
