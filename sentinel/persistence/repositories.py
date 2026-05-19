# sentinel/persistence/repositories.py
"""Repository layer — the only module that may write SQL.

Each repository exposes a `Protocol` (the interface other modules depend on)
plus a Postgres-backed concrete implementation. Methods accept and return
Pydantic schemas where they cross a module boundary.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol
from uuid import UUID

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sentinel.persistence.models import (
    DeployModel,
    IncidentModel,
    OutboxEventModel,
)
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.api import IncidentDetailResponse, IncidentListItem, ResolveIncidentRequest
from sentinel.schemas.diagnosis import Diagnosis

# --- Read DTOs -------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class DeployRow:
    id: UUID
    service: str
    sha: str
    pr_title: str | None
    deployed_at: datetime


# --- Outbox types ----------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    id: UUID
    topic: str
    key: str
    payload: dict[str, Any]
    attempts: int


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Returned by :meth:`IncidentRepository.ingest`.

    ``event_kind`` is ``"opened"`` for a new incident and ``"recurred"``
    when the fingerprint matched an existing open incident within the 1-hour
    dedup window.
    """

    incident_id: UUID
    event_kind: Literal["opened", "recurred"]


class OutboxBatch:
    """Context-managed batch of claimed outbox rows.

    mark_published / mark_failed register row state changes; __aexit__
    (driven by the repository's claim_batch async-cm) commits them as one
    transaction together with the FOR UPDATE SKIP LOCKED claim.
    """

    def __init__(self, session: AsyncSession, events: list[OutboxEvent]) -> None:
        self._session = session
        self.events: list[OutboxEvent] = events
        self._published: list[UUID] = []
        self._failed: list[tuple[UUID, str]] = []

    def mark_published(self, id_: UUID) -> None:
        self._published.append(id_)

    def mark_failed(self, id_: UUID, *, error: str) -> None:
        self._failed.append((id_, error))


# --- Outbox repository ------------------------------------------------------ #


class OutboxRepository(Protocol):
    async def enqueue(
        self,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
        session: AsyncSession | None = None,
    ) -> UUID: ...

    def claim_batch(
        self,
        *,
        limit: int,
        max_attempts: int = 10,
    ) -> AbstractAsyncContextManager[OutboxBatch]: ...


class PostgresOutboxRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def enqueue(
        self,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
        session: AsyncSession | None = None,
    ) -> UUID:
        row = OutboxEventModel(topic=topic, key=key, payload=payload)
        if session is None:
            async with self._session_factory() as s:
                s.add(row)
                await s.commit()
                await s.refresh(row)
                return row.id
        else:
            session.add(row)
            await session.flush([row])
            return row.id

    @asynccontextmanager
    async def claim_batch(
        self,
        *,
        limit: int,
        max_attempts: int = 10,
    ) -> AsyncIterator[OutboxBatch]:
        # Backoff window: LEAST(2^attempts, 300) seconds since last_attempt_at.
        # Built as a raw SQL fragment because SA's `func.make_interval` doesn't
        # accept kwargs the way Postgres's named-arg call does, and converting
        # an int seconds expression to interval requires `... * interval '1 second'`.
        backoff_interval = func.least(func.power(2, OutboxEventModel.attempts), 300) * text(
            "interval '1 second'"
        )
        async with self._session_factory() as s:
            stmt = (
                select(OutboxEventModel)
                .where(OutboxEventModel.published_at.is_(None))
                .where(OutboxEventModel.attempts < max_attempts)
                .where(
                    (OutboxEventModel.last_attempt_at.is_(None))
                    | (OutboxEventModel.last_attempt_at < func.now() - backoff_interval)
                )
                .order_by(OutboxEventModel.created_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            result = await s.execute(stmt)
            rows = list(result.scalars().all())
            events = [
                OutboxEvent(
                    id=r.id,
                    topic=r.topic,
                    key=r.key,
                    payload=r.payload,
                    attempts=r.attempts,
                )
                for r in rows
            ]
            batch = OutboxBatch(s, events)
            try:
                yield batch
            except Exception:
                await s.rollback()
                raise
            now = func.now()
            if batch._published:
                await s.execute(
                    update(OutboxEventModel)
                    .where(OutboxEventModel.id.in_(batch._published))
                    .values(published_at=now)
                )
            for id_, err in batch._failed:
                await s.execute(
                    update(OutboxEventModel)
                    .where(OutboxEventModel.id == id_)
                    .values(
                        attempts=OutboxEventModel.attempts + 1,
                        last_attempt_at=now,
                        last_error=err[:1000],
                    )
                )
            await s.commit()


# --- Incident repository ---------------------------------------------------- #


class IncidentRepository(Protocol):
    async def create_from_alert(self, alert: NormalizedAlert, *, fingerprint: str) -> UUID: ...

    async def get(self, incident_id: UUID) -> IncidentDetailResponse | None: ...

    async def list_recent(self, *, limit: int = 50) -> list[IncidentListItem]: ...

    async def mark_diagnosing(self, incident_id: UUID) -> None: ...

    async def ingest(
        self,
        alert: NormalizedAlert,
        *,
        fingerprint: str,
        outbox_topic: str,
        payload_hash: str,
    ) -> IngestResult: ...


class PostgresIncidentRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def create_from_alert(self, alert: NormalizedAlert, *, fingerprint: str) -> UUID:
        async with self._session_factory() as s:
            row = IncidentModel(
                external_id=alert.external_id,
                source=alert.source,
                service=alert.service,
                severity=alert.severity,
                title=alert.title,
                fingerprint=fingerprint,
                raw_payload=alert.raw_payload,
            )
            s.add(row)
            await s.commit()
            await s.refresh(row)
            return row.id

    async def get(self, incident_id: UUID) -> IncidentDetailResponse | None:
        async with self._session_factory() as s:
            row = await s.get(IncidentModel, incident_id)
            if row is None:
                return None
            return IncidentDetailResponse(
                id=row.id,
                source=row.source,
                external_id=row.external_id,
                service=row.service,
                severity=row.severity,
                status=row.status,
                title=row.title,
                fingerprint=row.fingerprint,
                opened_at=row.opened_at,
                resolved_at=row.resolved_at,
            )

    async def list_recent(self, *, limit: int = 50) -> list[IncidentListItem]:
        async with self._session_factory() as s:
            stmt = select(IncidentModel).order_by(IncidentModel.opened_at.desc()).limit(limit)
            result = await s.execute(stmt)
            return [
                IncidentListItem(
                    id=row.id,
                    service=row.service,
                    severity=row.severity,
                    status=row.status,
                    title=row.title,
                    opened_at=row.opened_at,
                    resolved_at=row.resolved_at,
                )
                for row in result.scalars()
            ]

    async def mark_diagnosing(self, incident_id: UUID) -> None:
        async with self._session_factory() as s:
            await s.execute(
                update(IncidentModel)
                .where(IncidentModel.id == incident_id)
                .values(status="diagnosing")
            )
            await s.commit()

    async def ingest(
        self,
        alert: NormalizedAlert,
        *,
        fingerprint: str,
        outbox_topic: str,
        payload_hash: str,
    ) -> IngestResult:
        from datetime import UTC
        from datetime import datetime as _datetime

        now_iso = _datetime.now(UTC).isoformat()

        async with self._session_factory() as s:
            # 1. Dedup-window lookup — open incidents with the same fingerprint
            #    within the last 1 hour.  FOR UPDATE SKIP LOCKED so concurrent
            #    webhooks for the same fingerprint don't race.
            existing_stmt = (
                select(IncidentModel)
                .where(IncidentModel.fingerprint == fingerprint)
                .where(IncidentModel.status.notin_(("resolved", "closed")))
                .where(IncidentModel.opened_at > func.now() - text("interval '1 hour'"))
                .order_by(IncidentModel.opened_at.desc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            existing = (await s.execute(existing_stmt)).scalar_one_or_none()

            new_event_log_entry: dict[str, Any] = {
                "ts": now_iso,
                "source": alert.source,
                "payload_hash": payload_hash,
            }

            if existing is not None:
                # 2. HIT: bump occurrence_count + append to event_log inside
                #    raw_payload._sentinel (no schema change needed).
                sentinel_meta: dict[str, Any] = (existing.raw_payload or {}).get("_sentinel", {})
                occurrence_count = int(sentinel_meta.get("occurrence_count", 1)) + 1
                event_log: list[dict[str, Any]] = list(sentinel_meta.get("event_log", []))
                event_log.append(new_event_log_entry)
                updated_payload: dict[str, Any] = dict(existing.raw_payload or {})
                updated_payload["_sentinel"] = {
                    "occurrence_count": occurrence_count,
                    "last_seen_at": now_iso,
                    "event_log": event_log,
                }
                existing.raw_payload = updated_payload

                outbox_row = OutboxEventModel(
                    topic=outbox_topic,
                    key=str(existing.id),
                    payload={
                        "event": "incident.recurred",
                        "incident_id": str(existing.id),
                        "fingerprint": fingerprint,
                        "source": alert.source,
                        "ts": now_iso,
                    },
                )
                s.add(outbox_row)
                await s.commit()
                return IngestResult(incident_id=existing.id, event_kind="recurred")

            # 3. MISS: insert new incident row, embedding the _sentinel metadata
            #    into the raw_payload JSONB column (no schema change to incidents).
            sentinel_meta = {
                "occurrence_count": 1,
                "last_seen_at": now_iso,
                "event_log": [new_event_log_entry],
            }
            merged_payload: dict[str, Any] = dict(alert.raw_payload)
            merged_payload["_sentinel"] = sentinel_meta

            new_incident = IncidentModel(
                external_id=alert.external_id,
                source=alert.source,
                service=alert.service,
                severity=alert.severity,
                title=alert.title,
                fingerprint=fingerprint,
                raw_payload=merged_payload,
            )
            s.add(new_incident)
            await s.flush([new_incident])

            # 4. In the same tx: insert outbox row for the Kafka producer.
            outbox_row = OutboxEventModel(
                topic=outbox_topic,
                key=str(new_incident.id),
                payload={
                    "event": "incident.opened",
                    "incident_id": str(new_incident.id),
                    "fingerprint": fingerprint,
                    "source": alert.source,
                    "ts": now_iso,
                },
            )
            s.add(outbox_row)
            await s.commit()
            return IngestResult(incident_id=new_incident.id, event_kind="opened")


# --- Deploy repository ------------------------------------------------------ #


class DeployRepository(Protocol):
    async def record(
        self,
        *,
        service: str,
        sha: str,
        deployed_at: datetime,
        pr_number: int | None = None,
        pr_title: str | None = None,
        pr_diff_summary: str | None = None,
        deployed_by: str | None = None,
    ) -> UUID: ...

    async def recent_for_service(self, service: str, *, limit: int = 20) -> list[DeployRow]: ...


class PostgresDeployRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def record(
        self,
        *,
        service: str,
        sha: str,
        deployed_at: datetime,
        pr_number: int | None = None,
        pr_title: str | None = None,
        pr_diff_summary: str | None = None,
        deployed_by: str | None = None,
    ) -> UUID:
        async with self._session_factory() as s:
            row = DeployModel(
                service=service,
                sha=sha,
                pr_number=pr_number,
                pr_title=pr_title,
                pr_diff_summary=pr_diff_summary,
                deployed_at=deployed_at,
                deployed_by=deployed_by,
            )
            s.add(row)
            await s.commit()
            await s.refresh(row)
            return row.id

    async def recent_for_service(self, service: str, *, limit: int = 20) -> list[DeployRow]:
        async with self._session_factory() as s:
            stmt = (
                select(DeployModel)
                .where(DeployModel.service == service)
                .order_by(DeployModel.deployed_at.desc())
                .limit(limit)
            )
            result = await s.execute(stmt)
            return [
                DeployRow(
                    id=row.id,
                    service=row.service,
                    sha=row.sha,
                    pr_title=row.pr_title,
                    deployed_at=row.deployed_at,
                )
                for row in result.scalars()
            ]


# --- Stubs for the rest (implemented as needed in their consuming work areas) ---


class DiagnosisRepository(Protocol):
    async def save(
        self,
        incident_id: UUID,
        diagnosis: Diagnosis,
        *,
        model: str,
        prompt_version: str,
        latency_ms: int,
        token_usage: dict[str, int],
        hallucinated_evidence: bool,
    ) -> UUID: ...


class ResolutionRepository(Protocol):
    async def record(self, incident_id: UUID, resolution: ResolveIncidentRequest) -> None: ...


class RunbookRepository(Protocol):
    # Return type is `RunbookItem` once Work Area H lands; using `Any` here avoids
    # forcing the import (and the dependency direction) before the concrete impl exists.
    async def search(self, embedding: list[float], *, k: int, min_cosine: float) -> list[Any]: ...


class EvalRunRepository(Protocol):
    async def start(self, *, model: str, prompt_version: str, corpus_version: str) -> UUID: ...

    async def complete(self, run_id: UUID, summary: dict[str, float]) -> None: ...


# Concrete classes for the stubbed Protocols above land with their consumers
# (Work Areas G, H, K). Keeping the Protocols here means downstream modules
# can depend on the interface without forcing the implementation now.
__all__ = [
    "DeployRepository",
    "DeployRow",
    "DiagnosisRepository",
    "EvalRunRepository",
    "IncidentRepository",
    "IngestResult",
    "OutboxBatch",
    "OutboxEvent",
    "OutboxRepository",
    "PostgresDeployRepository",
    "PostgresIncidentRepository",
    "PostgresOutboxRepository",
    "ResolutionRepository",
    "RunbookRepository",
]
