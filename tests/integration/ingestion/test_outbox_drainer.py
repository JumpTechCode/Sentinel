"""Outbox drainer with real Postgres + Kafka."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from sentinel.ingestion.kafka_producer import KafkaProducer
from sentinel.ingestion.outbox_drainer import OutboxDrainer
from sentinel.persistence.repositories import PostgresOutboxRepository
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def session_factory(
    pg_dsn: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(pg_dsn)
    factory = async_sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)
    yield factory
    await engine.dispose()


@pytest.mark.asyncio
async def test_drainer_publishes_enqueued_events(
    session_factory: async_sessionmaker[AsyncSession], kafka_brokers: str
) -> None:
    repo = PostgresOutboxRepository(session_factory)
    topic = "sentinel.it.test-publish"
    for i in range(3):
        await repo.enqueue(topic=topic, key=f"k{i}", payload={"i": i})

    producer = KafkaProducer(kafka_brokers)
    await producer.start()
    drainer = OutboxDrainer(outbox_repo=repo, producer=producer, poll_interval=0.05)
    task = asyncio.create_task(drainer.run())

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_brokers,
        auto_offset_reset="earliest",
        group_id="it-test",
    )
    await consumer.start()
    received: list[dict[str, int]] = []
    try:

        async def _collect() -> None:
            async for msg in consumer:
                received.append(json.loads(msg.value))
                if len(received) >= 3:
                    return

        await asyncio.wait_for(_collect(), timeout=10.0)
    finally:
        await consumer.stop()
        drainer.stop()
        await asyncio.wait_for(task, timeout=5.0)
        await producer.stop()

    assert sorted(r["i"] for r in received) == [0, 1, 2]


@pytest.mark.asyncio
async def test_drainer_concurrent_no_duplicate_emits(
    session_factory: async_sessionmaker[AsyncSession], kafka_brokers: str
) -> None:
    repo = PostgresOutboxRepository(session_factory)
    topic = "sentinel.it.test-concurrent"
    for i in range(5):
        await repo.enqueue(topic=topic, key=f"k{i}", payload={"i": i})

    producer = KafkaProducer(kafka_brokers)
    await producer.start()
    d1 = OutboxDrainer(outbox_repo=repo, producer=producer, poll_interval=0.05)
    d2 = OutboxDrainer(outbox_repo=repo, producer=producer, poll_interval=0.05)
    t1 = asyncio.create_task(d1.run())
    t2 = asyncio.create_task(d2.run())

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_brokers,
        auto_offset_reset="earliest",
        group_id="it-test-concurrent",
    )
    await consumer.start()
    received: list[dict[str, int]] = []
    try:

        async def _collect() -> None:
            async for msg in consumer:
                received.append(json.loads(msg.value))
                if len(received) >= 5:
                    # Give time for a possible duplicate emit to land.
                    await asyncio.sleep(0.5)
                    return

        await asyncio.wait_for(_collect(), timeout=15.0)
    finally:
        await consumer.stop()
        d1.stop()
        d2.stop()
        await asyncio.wait_for(t1, timeout=5.0)
        await asyncio.wait_for(t2, timeout=5.0)
        await producer.stop()

    keys = sorted(r["i"] for r in received)
    assert keys == [0, 1, 2, 3, 4], f"got duplicates or missing: {keys}"
