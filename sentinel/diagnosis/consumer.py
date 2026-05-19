# sentinel/diagnosis/consumer.py
"""aiokafka consumer for ``incident.enriched`` events.

Same shape as ``EnrichmentConsumer`` — deliberate pattern reuse.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from sentinel.diagnosis.deps import ConsumerDeps
from sentinel.diagnosis.errors import DiagnosisInvalid, LLMError
from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.observability.metrics import (
    diagnoses_total,
    diagnosis_failures_total,
    diagnosis_invalid_events_total,
)
from sentinel.persistence.repositories import OutboxEvent
from sentinel.schemas.api import IncidentDetailResponse
from sentinel.schemas.context import IncidentContext
from sentinel.schemas.enrichment_event import IncidentEvent

_LOG = logging.getLogger("sentinel.diagnosis.consumer")

_ACCEPTED_EVENTS = frozenset({"incident.enriched"})

DiagnoseFn = Callable[
    [IncidentDetailResponse, IncidentContext, Any],
    Coroutine[Any, Any, PersistedDiagnosis],
]


class DiagnosisConsumer:
    def __init__(
        self,
        *,
        consumer: Any,
        deps: ConsumerDeps,
        agent_fn: DiagnoseFn,
        topic: str = "sentinel.incidents",
        downstream_topic: str = "sentinel.incidents",
    ) -> None:
        self._consumer = consumer
        self._deps = deps
        self._agent = agent_fn
        self._topic = topic
        self._downstream_topic = downstream_topic
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async for msg in self._consumer:
            if self._stop_event.is_set():
                break
            try:
                committed = await self._handle(msg)
            except Exception:
                _LOG.exception(
                    "diagnoser_event_failed",
                    extra={
                        "offset": getattr(msg, "offset", None),
                        "partition": getattr(msg, "partition", None),
                    },
                )
                diagnosis_failures_total.labels(reason="exception").inc()
                continue
            if committed:
                await self._consumer.commit()

    async def _handle(self, msg: Any) -> bool:
        """Returns True iff the offset should be committed."""
        try:
            payload = json.loads(msg.value)
        except ValueError:
            _LOG.error(
                "diagnoser_invalid_envelope",
                extra={"offset": getattr(msg, "offset", None)},
            )
            diagnosis_invalid_events_total.inc()
            return True

        event_type = payload.get("event") if isinstance(payload, dict) else None
        if event_type not in _ACCEPTED_EVENTS:
            _LOG.debug("diagnoser_skip_event_type", extra={"event": event_type})
            return True

        try:
            envelope = IncidentEvent.model_validate(payload)
        except ValidationError:
            _LOG.error(
                "diagnoser_invalid_envelope",
                extra={"offset": getattr(msg, "offset", None)},
            )
            diagnosis_invalid_events_total.inc()
            return True

        incident = await self._deps.incident_repo.get(envelope.incident_id)
        if incident is None:
            _LOG.warning(
                "diagnoser_missing_incident",
                extra={"incident_id": str(envelope.incident_id)},
            )
            diagnosis_failures_total.labels(reason="missing_incident").inc()
            return True

        stored = await self._deps.incident_repo.get_enrichment_context(envelope.incident_id)
        if stored is None:
            _LOG.warning(
                "diagnoser_missing_context",
                extra={"incident_id": str(envelope.incident_id)},
            )
            diagnosis_failures_total.labels(reason="missing_context").inc()
            return True

        try:
            record = await self._agent(incident, stored.context, self._deps.agent_deps())
        except DiagnosisInvalid as e:
            _LOG.warning("diagnoser_schema_invalid", extra={"error": str(e)[:200]})
            diagnosis_failures_total.labels(reason="schema").inc()
            return True
        except LLMError as e:
            # NB: `logging.LogRecord` reserves the `msg` attribute, so the
            # excerpt key must not collide with it — using `msg` here would
            # raise KeyError from the logger and fall through to the outer
            # catch as `reason="exception"`, hiding the real LLM failure.
            _LOG.error(
                "diagnoser_llm_error",
                extra={"error": type(e).__name__, "error_excerpt": str(e)[:200]},
            )
            reason = {
                "LLMTimeout": "timeout",
                "LLMTransport": "transport",
                "LLMNoToolCall": "no_tool_call",
            }.get(type(e).__name__, "transport")
            diagnosis_failures_total.labels(reason=reason).inc()
            return False  # Kafka redelivery

        outbox_id = uuid4()
        outbox_event = OutboxEvent(
            id=outbox_id,
            topic=self._downstream_topic,
            key=str(envelope.incident_id),
            payload={
                "event_id": str(outbox_id),
                "event": "incident.diagnosed",
                "incident_id": str(envelope.incident_id),
                "ts": datetime.now(UTC).isoformat(),
            },
            attempts=0,
            created_at=datetime.now(UTC),
        )
        diagnosis_id, status = await self._deps.diagnosis_repo.save_with_outbox(
            incident_id=envelope.incident_id,
            record=record,
            upstream_event_id=envelope.event_id,
            outbox_event=outbox_event,
        )
        diagnoses_total.labels(status=status).inc()
        _LOG.info(
            "diagnoser_completed",
            extra={
                "incident_id": str(envelope.incident_id),
                "diagnosis_id": str(diagnosis_id),
                "status": status,
                "confidence": str(record.confidence),
                "hallucinated": record.hallucinated_evidence,
                "latency_ms": record.latency_ms,
            },
        )
        return True
