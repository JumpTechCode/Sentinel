"""Outbox drainer — pulls unpublished rows and emits to Kafka.

Started by the FastAPI app `lifespan`. One task per process is sufficient
(SKIP LOCKED in the underlying repository's claim_batch makes multiple
instances safe).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from sentinel.ingestion.kafka_producer import KafkaProducer
from sentinel.observability.metrics import (
    outbox_event_stuck_total,
    outbox_events_failed_total,
    outbox_events_published_total,
)
from sentinel.persistence.repositories import OutboxRepository

log = logging.getLogger(__name__)


class OutboxDrainer:
    def __init__(
        self,
        *,
        outbox_repo: OutboxRepository,
        producer: KafkaProducer,
        poll_interval: float = 0.25,
        batch_size: int = 100,
        emit_timeout: float = 5.0,
        max_attempts: int = 10,
    ) -> None:
        self._repo = outbox_repo
        self._producer = producer
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._emit_timeout = emit_timeout
        self._max_attempts = max_attempts
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._tick()
            except Exception:
                log.exception("outbox_drainer_tick_failed")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval)

    async def _tick(self) -> None:
        async with self._repo.claim_batch(
            limit=self._batch_size, max_attempts=self._max_attempts
        ) as batch:
            for event in batch.events:
                if event.attempts >= self._max_attempts:
                    outbox_event_stuck_total.inc()
                    continue
                try:
                    await asyncio.wait_for(
                        self._producer.emit(
                            topic=event.topic,
                            key=event.key,
                            payload=event.payload,
                        ),
                        timeout=self._emit_timeout,
                    )
                    batch.mark_published(event.id)
                    outbox_events_published_total.labels(topic=event.topic).inc()
                except Exception as e:
                    batch.mark_failed(event.id, error=repr(e))
                    outbox_events_failed_total.labels(topic=event.topic).inc()
