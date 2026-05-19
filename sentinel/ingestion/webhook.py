"""Webhook handler — wiring of adapter / idempotency / repository.

The FastAPI route is a thin wrapper that calls WebhookHandler.handle().
"""

from __future__ import annotations

import enum
import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID

from sentinel.config.settings import Settings
from sentinel.ingestion.fingerprint import fingerprint, normalize_title
from sentinel.ingestion.idempotency import IdempotencyStore
from sentinel.integrations.base import (
    AdapterParseError,
    MissingSecretError,
)
from sentinel.integrations.registry import (
    ADAPTERS,
    get_adapter,
    get_secret_for_source,
)
from sentinel.observability.metrics import (
    outbox_events_enqueued_total,
    webhook_handler_duration_seconds,
    webhooks_total,
)
from sentinel.persistence.repositories import IncidentRepository

log = logging.getLogger(__name__)


class HandlerOutcome(str, enum.Enum):
    ACCEPTED = "accepted"
    RECURRED = "recurred"
    DUPLICATE = "duplicate"
    UNAUTHORIZED = "unauthorized"
    BAD_REQUEST = "bad_request"
    UNKNOWN_SOURCE = "unknown_source"
    INFRA_UNAVAILABLE = "infra_unavailable"


@dataclass(frozen=True, slots=True)
class HandlerResult:
    outcome: HandlerOutcome
    incident_id: UUID | None = None
    reason: str | None = None


class WebhookHandler:
    def __init__(
        self,
        *,
        incident_repo: IncidentRepository,
        idempotency: IdempotencyStore,
        settings: Settings,
        outbox_topic: str,
    ) -> None:
        self._repo = incident_repo
        self._idempotency = idempotency
        self._settings = settings
        self._outbox_topic = outbox_topic

    async def handle(
        self,
        *,
        source: str,
        headers: Mapping[str, str],
        body: bytes,
    ) -> HandlerResult:
        t0 = time.perf_counter()
        outcome: HandlerOutcome = HandlerOutcome.ACCEPTED
        incident_id: UUID | None = None
        try:
            # 1. Adapter lookup.
            if source not in ADAPTERS:
                outcome = HandlerOutcome.UNKNOWN_SOURCE
                return HandlerResult(outcome=outcome)
            adapter = get_adapter(source)

            # 2. Secret resolution.
            try:
                secret = get_secret_for_source(self._settings, source)
            except MissingSecretError:
                log.warning("webhook_secret_unset", extra={"source": source})
                outcome = HandlerOutcome.UNAUTHORIZED
                return HandlerResult(outcome=outcome, reason="missing_secret")

            # 3. Signature verification.
            if not adapter.verify_signature(headers, body, secret):
                outcome = HandlerOutcome.UNAUTHORIZED
                return HandlerResult(outcome=outcome, reason="bad_signature")

            # 4. Idempotency.
            try:
                is_dup = await self._idempotency.check_and_mark(source, body)
            except Exception:
                log.exception("idempotency_store_unavailable")
                outcome = HandlerOutcome.INFRA_UNAVAILABLE
                return HandlerResult(outcome=outcome, reason="idempotency_unavailable")
            if is_dup:
                outcome = HandlerOutcome.DUPLICATE
                return HandlerResult(outcome=outcome)

            # 5. Parse + normalize.
            try:
                payload: Any = json.loads(body)
            except json.JSONDecodeError:
                outcome = HandlerOutcome.BAD_REQUEST
                return HandlerResult(outcome=outcome, reason="bad_json")
            try:
                alert = adapter.normalize(payload)
            except AdapterParseError as e:
                outcome = HandlerOutcome.BAD_REQUEST
                return HandlerResult(outcome=outcome, reason=str(e))

            # 6. Fingerprint.
            fp = fingerprint(alert.service, normalize_title(alert.title), alert.severity)
            payload_hash = sha256(body).hexdigest()

            # 7. Persist + outbox in one tx.
            result = await self._repo.ingest(
                alert,
                fingerprint=fp,
                outbox_topic=self._outbox_topic,
                payload_hash=payload_hash,
            )
            incident_id = result.incident_id
            outcome = (
                HandlerOutcome.RECURRED
                if result.event_kind == "recurred"
                else HandlerOutcome.ACCEPTED
            )
            outbox_events_enqueued_total.labels(topic=self._outbox_topic).inc()
            return HandlerResult(outcome=outcome, incident_id=incident_id)
        finally:
            elapsed = time.perf_counter() - t0
            webhook_handler_duration_seconds.labels(source=source).observe(elapsed)
            # Reuse existing webhooks_total (labels: source, status); map outcome → status.
            webhooks_total.labels(source=source, status=outcome.value).inc()
            log.info(
                "webhook_processed",
                extra={
                    "source": source,
                    "outcome": outcome.value,
                    "incident_id": str(incident_id) if incident_id else None,
                    "latency_ms": round(elapsed * 1000, 2),
                },
            )
