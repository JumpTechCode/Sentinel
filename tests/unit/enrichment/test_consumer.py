# tests/unit/enrichment/test_consumer.py
"""EnrichmentConsumer message handling."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sentinel.enrichment.consumer import EnrichmentConsumer
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, IncidentContext


@dataclass
class _FakeMsg:
    value: bytes
    offset: int = 0
    partition: int = 0
    key: bytes = b""


def _envelope(event: str = "incident.opened") -> bytes:
    return json.dumps(
        {
            "event_id": str(uuid4()),
            "event": event,
            "incident_id": str(uuid4()),
            "fingerprint": "fp",
            "source": "generic",
            "ts": datetime.now(UTC).isoformat(),
        }
    ).encode()


class _FakeIncidentRepo:
    def __init__(self) -> None:
        self.get_calls = 0
        self.write_calls = 0
        self.write_status = "written"
        self.incident_present = True
        self.last_outbox_event: Any = None

    async def get(self, incident_id: UUID) -> Any:
        self.get_calls += 1
        if not self.incident_present:
            return None
        return MagicMock(
            id=incident_id,
            service="api",
            external_id="ext-1",
            title="boom",
            severity="SEV2",
        )

    async def write_enrichment_context(
        self,
        *,
        incident_id: UUID,
        event_id: UUID,
        context: Any,
        assembled_at: datetime,
        outbox_event: Any = None,
    ) -> Any:
        self.write_calls += 1
        self.last_outbox_event = outbox_event
        from sentinel.persistence.repositories import EnrichmentWriteResult

        return EnrichmentWriteResult(status=self.write_status, version=1)  # type: ignore[arg-type]


async def _passthrough_assemble(incident: Any, deps: EnrichmentDeps) -> IncidentContext:
    empty: FetcherResult[object] = FetcherResult(status="ok", data=[], fetched_at=datetime.now(UTC))
    return IncidentContext(
        incident_id=incident.id,
        assembled_at=datetime.now(UTC),
        recent_deploys=empty,
        related_alerts=empty,
        similar_incidents=empty,
        runbooks=empty,
        recent_logs=empty,
        active_alerts=empty,
    )


@pytest.mark.asyncio
async def test_handle_valid_event_writes_context_and_commits() -> None:
    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher.handle_message(_FakeMsg(value=_envelope()))
    assert repo.write_calls == 1


@pytest.mark.asyncio
async def test_handle_invalid_envelope_commits_and_counts() -> None:
    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher.handle_message(_FakeMsg(value=b"not json"))
    assert repo.write_calls == 0


@pytest.mark.asyncio
async def test_handle_unknown_event_type_skips() -> None:
    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher.handle_message(_FakeMsg(value=_envelope(event="incident.resolved")))
    assert repo.write_calls == 0


@pytest.mark.asyncio
async def test_handle_missing_incident_skips() -> None:
    repo = _FakeIncidentRepo()
    repo.incident_present = False
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher.handle_message(_FakeMsg(value=_envelope()))
    assert repo.write_calls == 0


@pytest.mark.asyncio
async def test_handle_enriched_message_does_not_pollute_invalid_metric() -> None:
    """`incident.enriched` round-trips through the same topic with a slimmer
    payload (no fingerprint/source). The early event-type filter must skip it
    BEFORE Pydantic validation, so it does not increment
    sentinel_enrichment_invalid_events_total.
    """
    from sentinel.observability.metrics import enrichment_invalid_events_total

    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    enriched = json.dumps(
        {
            "event_id": str(uuid4()),
            "event": "incident.enriched",
            "incident_id": str(uuid4()),
            "ts": datetime.now(UTC).isoformat(),
        }
    ).encode()

    before = enrichment_invalid_events_total._value.get()
    await enricher.handle_message(_FakeMsg(value=enriched))
    after = enrichment_invalid_events_total._value.get()

    assert repo.write_calls == 0
    assert after == before, "incident.enriched must not poison the invalid metric"


@pytest.mark.asyncio
async def test_enriched_outbox_payload_includes_fingerprint_and_source() -> None:
    # Regression: the diagnoser's IncidentEvent envelope requires `fingerprint`
    # and `source` on every `incident.enriched` message. Earlier the enricher
    # dropped both fields, so the diagnoser silently failed with
    # `diagnoser_invalid_envelope` for every real production event — masked in
    # CI because integration fixtures hand-built events with all fields
    # populated. Surfaced via the eval harness smoke run on 2026-05-20.
    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )

    envelope_id = uuid4()
    payload = json.dumps(
        {
            "event_id": str(envelope_id),
            "event": "incident.opened",
            "incident_id": str(uuid4()),
            "fingerprint": "fp-deadbeef",
            "source": "pagerduty",
            "ts": datetime.now(UTC).isoformat(),
        }
    ).encode()
    await enricher.handle_message(_FakeMsg(value=payload))

    assert repo.last_outbox_event is not None, "outbox event must be emitted"
    emitted = repo.last_outbox_event.payload
    assert emitted["event"] == "incident.enriched"
    assert emitted["fingerprint"] == "fp-deadbeef", "fingerprint must round-trip"
    assert emitted["source"] == "pagerduty", "source must round-trip"


@pytest.mark.asyncio
async def test_handle_duplicate_does_not_emit_outbox() -> None:
    repo = _FakeIncidentRepo()
    repo.write_status = "duplicate"
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer,
        deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher.handle_message(_FakeMsg(value=_envelope()))
    assert repo.write_calls == 1
