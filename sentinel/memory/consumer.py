# sentinel/memory/consumer.py
"""Kafka consumer for incident.opened / incident.resolved → embeddings.

Mirrors EnrichmentConsumer/DiagnosisConsumer:
- subscribes to the single sentinel.incidents topic with a distinct group_id
- enable_auto_commit=False; manual commit only after successful handle_message
- at-least-once delivery; per-event idempotency via event_id
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError

from sentinel.memory.deps import MemoryConsumerDeps
from sentinel.observability.metrics import (
    memory_duplicates_total,
    memory_embedding_duration_seconds,
    memory_events_consumed_total,
    memory_events_failed_total,
    memory_invalid_events_total,
)

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, ConsumerRecord


_LOG = logging.getLogger("sentinel.memory.consumer")

_EVENTS_OF_INTEREST = frozenset({"incident.opened", "incident.resolved"})


class _EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore")
    event_id: UUID
    event: str
    incident_id: UUID


# Embedding-failure budget before we declare the message dead and commit.
# Two retries cover transient ONNX init races / fastembed cold-start hiccups;
# three is the budget before the wedge-clear cost outweighs further redelivery.
# Promote to `Settings` only if operator tuning becomes necessary.
_EMBED_POISON_PILL_THRESHOLD = 3


class MemoryConsumer:
    def __init__(
        self,
        *,
        consumer: AIOKafkaConsumer,
        deps: MemoryConsumerDeps,
    ) -> None:
        self._consumer = consumer
        self._deps = deps
        self._stop = asyncio.Event()
        # Embedding poison-pill state. A persistent fastembed failure (corrupt
        # ONNX model, missing weights) makes every embed() raise the same
        # non-TimeoutError exception. Without a bound, the run() loop hot-loops
        # on the same offset (no commit → Kafka redelivers → same failure).
        # After N consecutive failures on the SAME event_id, give up and
        # commit so the stream advances. Counter resets on success or when
        # a different event_id arrives.
        # In-memory only: on process restart the counter resets and the bound
        # restarts from zero. Worst case is (restarts * threshold) redeliveries
        # before the poison-pill commit — bounded, acceptable for V1.
        self._last_failed_event_id: UUID | None = None
        self._embed_consecutive_failures: int = 0

    def stop(self) -> None:
        self._stop.set()

    def _reset_embed_failure_state(self) -> None:
        self._last_failed_event_id = None
        self._embed_consecutive_failures = 0

    async def run(self) -> None:
        async for msg in self._consumer:
            if self._stop.is_set():
                break
            try:
                await self.handle_message(msg)
            except Exception:
                _LOG.exception(
                    "memory_event_failed",
                    extra={"offset": msg.offset, "partition": msg.partition},
                )
                # Do NOT commit; redelivery via at-least-once.
                continue
            # handle_message commits on its own for success paths.

    async def handle_message(self, msg: ConsumerRecord) -> None:
        # 1. Parse + validate envelope.
        try:
            payload = json.loads(msg.value)
            envelope = _EventEnvelope.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as e:
            _LOG.error(
                "memory_invalid_envelope",
                extra={"offset": msg.offset, "err": repr(e)},
            )
            memory_invalid_events_total.inc()
            await self._consumer.commit()
            return

        # 2. Branch on event type.
        if envelope.event not in _EVENTS_OF_INTEREST:
            _LOG.debug(
                "memory_skip_event_type",
                extra={"event": envelope.event, "incident_id": str(envelope.incident_id)},
            )
            await self._consumer.commit()
            return

        memory_events_consumed_total.labels(type=envelope.event).inc()

        # 3. Load incident (+ optional resolution).
        row = await self._deps.incident_repo.load_for_memory(envelope.incident_id)
        if row is None:
            _LOG.warning(
                "memory_unknown_incident",
                extra={"incident_id": str(envelope.incident_id)},
            )
            memory_events_failed_total.labels(reason="unknown_incident").inc()
            await self._consumer.commit()
            return

        # 4. Compose text per event type.
        if envelope.event == "incident.opened":
            text_input = self._deps.pipeline.compose_initial(row)
        else:  # incident.resolved
            if row.resolution is None:
                _LOG.warning(
                    "memory_resolution_missing",
                    extra={"incident_id": str(envelope.incident_id)},
                )
                memory_events_failed_total.labels(reason="resolution_missing").inc()
                await self._consumer.commit()
                return
            text_input = self._deps.pipeline.compose_resolved(row, row.resolution)

        # 5. Embed. Timeout = poison pill (commit). Other exceptions retry
        #    until the consecutive-failure bound trips for this event_id, at
        #    which point we treat it as a poison pill too (corrupt model file,
        #    OnnxRuntime init failure). Without the bound, a persistent error
        #    wedges the consumer on the first failing offset forever.
        start = time.monotonic()
        try:
            vector = await self._deps.embedding_provider.embed(text_input)
        except TimeoutError:
            _LOG.warning(
                "embedding_compute_timeout",
                extra={"len": len(text_input), "incident_id": str(envelope.incident_id)},
            )
            memory_events_failed_total.labels(reason="embedding_timeout").inc()
            self._reset_embed_failure_state()
            await self._consumer.commit()
            return
        except Exception as e:
            if envelope.event_id == self._last_failed_event_id:
                self._embed_consecutive_failures += 1
            else:
                self._last_failed_event_id = envelope.event_id
                self._embed_consecutive_failures = 1

            if self._embed_consecutive_failures >= _EMBED_POISON_PILL_THRESHOLD:
                _LOG.error(
                    "embedding_poison_pill",
                    extra={
                        "incident_id": str(envelope.incident_id),
                        "event_id": str(envelope.event_id),
                        "attempts": self._embed_consecutive_failures,
                        "exc_type": type(e).__name__,
                    },
                )
                memory_events_failed_total.labels(reason="embedding_poison_pill").inc()
                self._reset_embed_failure_state()
                await self._consumer.commit()
                return
            raise  # re-raise so run() doesn't commit; Kafka will redeliver
        else:
            self._reset_embed_failure_state()
        finally:
            memory_embedding_duration_seconds.labels(event_type=envelope.event).observe(
                time.monotonic() - start
            )

        # 6. Idempotent write. Repository raises on infra failure → propagates;
        #    the run() loop catches and does NOT commit, redelivery handles it.
        #    DB failures are NOT counted toward the embedding poison-pill bound:
        #    Postgres restarts / deadlocks / failovers are transient by design
        #    (the point of at-least-once is to ride them out), while embedding
        #    failures are deterministic-on-input (corrupt model → every call
        #    fails identically). Sharing the bound would poison-pill healthy
        #    incidents during a routine DB rotation.
        result = await self._deps.incident_repo.set_embedding(
            envelope.incident_id, vector, event_id=envelope.event_id
        )
        if result == "written":
            _LOG.info(
                "memory_written",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "event_id": str(envelope.event_id),
                    "event_type": envelope.event,
                },
            )
        elif result == "duplicate":
            _LOG.info(
                "memory_duplicate",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "event_id": str(envelope.event_id),
                },
            )
            memory_duplicates_total.inc()
        else:  # unknown_incident
            _LOG.warning(
                "memory_set_embedding_unknown_incident",
                extra={"incident_id": str(envelope.incident_id)},
            )
            memory_events_failed_total.labels(reason="unknown_incident").inc()

        await self._consumer.commit()
