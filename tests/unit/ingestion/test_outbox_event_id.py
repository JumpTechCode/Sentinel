# tests/unit/ingestion/test_outbox_event_id.py
"""Outbox payload includes event_id matching the OutboxEventModel.id.

Covers both ingest() branches:
- opened path: dedup-window MISS → new incident + outbox row tagged
  `incident.opened`.
- recurred path: dedup-window HIT → existing incident updated + outbox row
  tagged `incident.recurred`.

The invariant is the same in both branches — `payload["event_id"] ==
str(outbox_row.id)` — but the construction sites are structurally distinct,
so a focused unit test per branch documents the contract and catches a
future regression cheaper than the integration suite.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from sentinel.persistence.repositories import PostgresIncidentRepository
from sentinel.schemas.alert import NormalizedAlert


@pytest.fixture()
def normalized_alert() -> NormalizedAlert:
    return NormalizedAlert(
        source="generic",
        external_id="ext-1",
        service="payments",
        severity="SEV2",
        title="db timeout",
        raw_payload={"hello": "world"},
        received_at=datetime.now(UTC),
    )


class _StubSessionFactory:
    captured: list[object]

    def __init__(self, *, existing_incident: object | None = None) -> None:
        self.captured = []
        self._existing_incident = existing_incident

    def __call__(self) -> _StubSession:
        return _StubSession(self.captured, existing_incident=self._existing_incident)


class _StubSession:
    def __init__(self, captured: list[object], *, existing_incident: object | None) -> None:
        self._captured = captured
        self._existing_incident = existing_incident
        self._exec_count = 0

    async def __aenter__(self) -> _StubSession:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    def add(self, obj: object) -> None:
        self._captured.append(obj)

    async def flush(self, *_args: object) -> None: ...

    async def commit(self) -> None: ...

    async def execute(self, *args: object, **kwargs: object) -> object:
        # First call is `SELECT now()` (scalar_one); subsequent calls are
        # the dedup-window lookup (scalar_one_or_none). Returning None forces
        # the MISS branch; returning a fake row forces the HIT branch.
        self._exec_count += 1
        if self._exec_count == 1:
            return SimpleNamespace(scalar_one=lambda: datetime.now(UTC))
        existing = self._existing_incident
        return SimpleNamespace(scalar_one_or_none=lambda: existing)


@pytest.mark.asyncio
async def test_outbox_payload_includes_event_id_matching_row_id(
    normalized_alert: NormalizedAlert,
) -> None:
    factory = _StubSessionFactory()
    repo = PostgresIncidentRepository(factory)  # type: ignore[arg-type]
    await repo.ingest(
        normalized_alert,
        fingerprint="fp-1",
        outbox_topic="sentinel.incidents",
        payload_hash="ph-1",
    )
    outbox_row = next(c for c in factory.captured if type(c).__name__ == "OutboxEventModel")
    payload = outbox_row.payload  # type: ignore[attr-defined]
    row_id = outbox_row.id  # type: ignore[attr-defined]
    assert isinstance(row_id, UUID)
    assert payload["event_id"] == str(row_id)
    assert payload["event"] == "incident.opened"


@pytest.mark.asyncio
async def test_outbox_payload_event_id_on_recurred_path(
    normalized_alert: NormalizedAlert,
) -> None:
    """Recurred path: dedup-window HIT updates the existing incident and emits
    an outbox row whose payload `event_id` equals the OutboxEventModel `id`.
    """
    existing_id = uuid4()
    existing = SimpleNamespace(id=existing_id, raw_payload={"_sentinel": {"occurrence_count": 1}})
    factory = _StubSessionFactory(existing_incident=existing)
    repo = PostgresIncidentRepository(factory)  # type: ignore[arg-type]

    result = await repo.ingest(
        normalized_alert,
        fingerprint="fp-1",
        outbox_topic="sentinel.incidents",
        payload_hash="ph-2",
    )

    assert result.incident_id == existing_id
    assert result.event_kind == "recurred"

    outbox_row = next(c for c in factory.captured if type(c).__name__ == "OutboxEventModel")
    payload = outbox_row.payload  # type: ignore[attr-defined]
    row_id = outbox_row.id  # type: ignore[attr-defined]
    assert isinstance(row_id, UUID)
    assert payload["event_id"] == str(row_id)
    assert payload["event"] == "incident.recurred"
    assert payload["incident_id"] == str(existing_id)
