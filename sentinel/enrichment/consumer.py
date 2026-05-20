# sentinel/enrichment/consumer.py
"""aiokafka consumer that triggers enrichment on incident.opened/recurred.

One process runs one consumer (the OutboxDrainer pattern). Idempotency is
enforced at the DB level via incidents.last_enrichment_event_id — re-delivery
of the same Kafka message is a no-op.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.observability.metrics import (
    enrichment_duplicates_total,
    enrichment_events_consumed_total,
    enrichment_events_failed_total,
    enrichment_invalid_events_total,
)
from sentinel.persistence.repositories import OutboxEvent
from sentinel.schemas.context import IncidentContext
from sentinel.schemas.enrichment_event import IncidentEvent

log = logging.getLogger(__name__)

_ACCEPTED_EVENTS = frozenset({"incident.opened", "incident.recurred"})

# The first parameter stays `Any`: the orchestrator only reads `.id` off the
# incident, so the consumer/orchestrator pair behaves structurally and a
# narrower nominal type would force the consumer to depend on the repository's
# IncidentDetailResponse just for this signature. The return type narrows
# from `Any` to `IncidentContext` — `sentinel.schemas.context` is a leaf
# module so this introduces no import cycle with `orchestrator`.
AssembleFn = Callable[[Any, EnrichmentDeps], Awaitable[IncidentContext]]


class EnrichmentConsumer:
    def __init__(
        self,
        *,
        consumer: Any,
        deps: EnrichmentDeps,
        assemble_fn: AssembleFn,
        topic: str,
        enriched_topic: str = "sentinel.incidents",
    ) -> None:
        self._consumer = consumer
        self._deps = deps
        self._assemble = assemble_fn
        self._topic = topic
        self._enriched_topic = enriched_topic
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async for msg in self._consumer:
            if self._stop_event.is_set():
                break
            try:
                await self.handle_message(msg)
            except Exception:
                log.exception(
                    "enricher_event_failed",
                    extra={
                        "offset": getattr(msg, "offset", None),
                        "partition": getattr(msg, "partition", None),
                    },
                )
                enrichment_events_failed_total.labels(reason="exception").inc()
                continue
            await self._consumer.commit()

    async def handle_message(self, msg: Any) -> None:
        """Process one Kafka message — public so unit tests can drive a single
        envelope without spinning the full `run()` loop.
        """
        try:
            payload = json.loads(msg.value)
        except ValueError:
            log.error(
                "enricher_invalid_envelope",
                extra={"offset": getattr(msg, "offset", None)},
            )
            enrichment_invalid_events_total.inc()
            return

        # Filter on event type BEFORE full envelope validation.
        # `incident.enriched` events round-trip through this same topic and
        # have a different payload shape; we filter them here so they don't
        # pollute the invalid-events metric. See ADR 0002.
        event_type = payload.get("event") if isinstance(payload, dict) else None
        if event_type not in _ACCEPTED_EVENTS:
            log.debug(
                "enricher_skip_event_type",
                extra={"event": event_type},
            )
            return

        try:
            envelope = IncidentEvent.model_validate(payload)
        except ValidationError:
            log.error(
                "enricher_invalid_envelope",
                extra={"offset": getattr(msg, "offset", None)},
            )
            enrichment_invalid_events_total.inc()
            return

        enrichment_events_consumed_total.labels(type=envelope.event).inc()

        incident = await self._deps.incident_repo.get(envelope.incident_id)
        if incident is None:
            log.warning(
                "enricher_unknown_incident",
                extra={"incident_id": str(envelope.incident_id)},
            )
            enrichment_events_failed_total.labels(reason="missing_incident").inc()
            return

        ctx = await self._assemble(incident, self._deps)
        assembled_at = datetime.now(UTC)

        outbox_event_id = uuid4()
        outbox_event = OutboxEvent(
            id=outbox_event_id,
            topic=self._enriched_topic,
            key=str(envelope.incident_id),
            payload={
                "event_id": str(outbox_event_id),
                "event": "incident.enriched",
                "incident_id": str(envelope.incident_id),
                # fingerprint + source must be propagated so downstream
                # consumers (e.g., DiagnosisConsumer's IncidentEvent envelope)
                # can validate the event. Without them, the diagnoser logs
                # `diagnoser_invalid_envelope` and never produces a diagnosis
                # in production — only the integration tests work because
                # they fake events with all fields populated. Surfaced by
                # PR 3c smoke testing.
                "fingerprint": envelope.fingerprint,
                "source": envelope.source,
                "ts": assembled_at.isoformat(),
            },
            attempts=0,
            created_at=assembled_at,
        )
        result = await self._deps.incident_repo.write_enrichment_context(
            incident_id=envelope.incident_id,
            event_id=envelope.event_id,
            context=ctx,
            assembled_at=assembled_at,
            outbox_event=outbox_event,
        )
        if result.status == "duplicate":
            log.info(
                "enricher_duplicate",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "event_id": str(envelope.event_id),
                },
            )
            enrichment_duplicates_total.inc()
        else:
            log.info(
                "enricher_wrote_context",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "version": result.version,
                },
            )
