# sentinel/persistence/repositories.py
"""Repository layer — the only module that may write SQL.

Each repository exposes a `Protocol` (the interface other modules depend on)
plus a Postgres-backed concrete implementation. Methods accept and return
Pydantic schemas where they cross a module boundary.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol, cast
from uuid import UUID

from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.persistence.models import (
    DeployModel,
    DiagnosisModel,
    IncidentModel,
    OutboxEventModel,
)
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.api import IncidentDetailResponse, IncidentListItem, ResolveIncidentRequest
from sentinel.schemas.context import DeployItem, IncidentContext, RelatedAlertItem
from sentinel.schemas.enums import SeverityType

# --- Read DTOs -------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class DeployRow:
    id: UUID
    service: str
    sha: str
    pr_title: str | None
    deployed_at: datetime


@dataclass(frozen=True, slots=True)
class ResolutionData:
    root_cause: str
    remediation: str
    diagnosis_was_correct: bool | None


@dataclass(frozen=True, slots=True)
class MemoryIncidentRow:
    id: UUID
    service: str
    title: str
    status: str
    resolution: ResolutionData | None


@dataclass(frozen=True, slots=True)
class SimilarIncidentRow:
    id: UUID
    title: str
    root_cause: str
    remediation: str
    cosine_similarity: float


@dataclass(frozen=True, slots=True)
class ResolveRecordResult:
    incident_id: UUID
    resolved_at: datetime
    event_id: UUID


# --- Outbox types ----------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class OutboxEvent:
    id: UUID
    topic: str
    key: str
    payload: dict[str, Any]
    attempts: int
    created_at: datetime


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Returned by :meth:`IncidentRepository.ingest`.

    ``event_kind`` is ``"opened"`` for a new incident and ``"recurred"``
    when the fingerprint matched an existing open incident within the 1-hour
    dedup window.
    """

    incident_id: UUID
    event_kind: Literal["opened", "recurred"]


@dataclass(frozen=True, slots=True)
class EnrichmentWriteResult:
    """Returned by :meth:`IncidentRepository.write_enrichment_context`.

    ``status`` is ``"written"`` when the conditional UPDATE applied,
    ``"duplicate"`` when ``event_id`` matched ``last_enrichment_event_id``
    and the write was a no-op. ``version`` is the post-write
    ``context_version`` on a successful write, or the unchanged existing
    version on a duplicate.
    """

    status: Literal["written", "duplicate"]
    version: int


@dataclass(frozen=True, slots=True)
class StoredEnrichmentContext:
    """Read-back of the persisted enrichment context for an incident."""

    context: IncidentContext
    assembled_at: datetime
    version: int
    last_event_id: UUID


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
    ) -> UUID:
        """Insert one outbox row.

        Passing ``session=None`` opens a private transaction and commits it
        standalone — this path is for backfills and tests only, NOT for
        production webhook ingestion (it reintroduces the dual-write problem
        the outbox is meant to solve). Production callers pass the outer
        transaction's session so the outbox row commits atomically with the
        domain row that triggered it (see
        :meth:`IncidentRepository.ingest` for the canonical pattern).
        """
        ...

    def claim_batch(
        self,
        *,
        limit: int,
        max_attempts: int = 10,
    ) -> AbstractAsyncContextManager[OutboxBatch]: ...

    async def unpublished_stats(self) -> tuple[int, float]:
        """Return ``(count, oldest_age_seconds)`` for unpublished outbox rows.

        Drives the ``outbox_unpublished_count`` and
        ``outbox_oldest_unpublished_age_seconds`` gauges. Single aggregate
        query — cheap enough to poll on a 30s interval against the partial
        ``idx_outbox_unpublished`` index. Returns ``(0, 0.0)`` when empty.
        """
        ...

    async def stuck_count(self, *, max_attempts: int) -> int:
        """Return count of distinct unpublished outbox rows with attempts >= max_attempts.

        Drives the ``outbox_stuck_events_count`` gauge. The drainer treats
        these rows as permanently stuck (skipped each tick). A counter would
        inflate by 1 per scan per stuck row; this gauge gives the correct
        distinct-row count.
        """
        ...


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

    async def unpublished_stats(self) -> tuple[int, float]:
        # `EXTRACT(EPOCH FROM now() - MIN(created_at))` is NULL when the table is
        # empty; `COALESCE(..., 0)` handles that path. Single index scan on
        # `idx_outbox_unpublished`.
        stmt = select(
            func.count(OutboxEventModel.id),
            func.coalesce(
                func.extract("epoch", func.now() - func.min(OutboxEventModel.created_at)),
                0,
            ),
        ).where(OutboxEventModel.published_at.is_(None))
        async with self._session_factory() as s:
            row = (await s.execute(stmt)).first()
        if row is None:
            return (0, 0.0)
        count, age = row
        return (int(count or 0), float(age or 0.0))

    async def stuck_count(self, *, max_attempts: int) -> int:
        stmt = select(func.count(OutboxEventModel.id)).where(
            OutboxEventModel.published_at.is_(None),
            OutboxEventModel.attempts >= max_attempts,
        )
        async with self._session_factory() as s:
            row = (await s.execute(stmt)).first()
        if row is None:
            return 0
        return int(row[0] or 0)

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
                    created_at=r.created_at,
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

    async def write_enrichment_context(
        self,
        *,
        incident_id: UUID,
        event_id: UUID,
        context: IncidentContext,
        assembled_at: datetime,
        outbox_event: OutboxEvent | None = None,
    ) -> EnrichmentWriteResult: ...

    async def get_enrichment_context(
        self,
        incident_id: UUID,
    ) -> StoredEnrichmentContext | None: ...

    async def recent_for_service(
        self,
        *,
        service: str,
        since: datetime,
        exclude_incident_id: UUID,
    ) -> list[RelatedAlertItem]: ...

    async def set_embedding(
        self,
        incident_id: UUID,
        embedding: list[float],
        *,
        event_id: UUID,
    ) -> Literal["written", "duplicate", "unknown_incident"]: ...

    async def load_for_memory(
        self,
        incident_id: UUID,
    ) -> MemoryIncidentRow | None: ...

    async def similar_resolved_incidents(
        self,
        *,
        query_embedding: list[float],
        k: int,
        exclude_incident_id: UUID | None,
    ) -> list[SimilarIncidentRow]: ...


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

    # Cap to keep the JSONB column bounded under pathological recurrence
    # (a noisy alert firing every few seconds for an hour shouldn't grow the
    # row indefinitely). Truncated count is tracked so observability sees
    # the drop.
    _EVENT_LOG_CAP = 100

    async def ingest(
        self,
        alert: NormalizedAlert,
        *,
        fingerprint: str,
        outbox_topic: str,
        payload_hash: str,
    ) -> IngestResult:
        async with self._session_factory() as s:
            # Use Postgres server time consistently — `opened_at` defaults to
            # `now()` and the dedup window query uses `func.now()`. Pulling the
            # event_log timestamp from the same clock avoids cross-host skew.
            now = (await s.execute(select(func.now()))).scalar_one()
            now_iso = now.isoformat()

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
                truncated = int(sentinel_meta.get("truncated_entries", 0))
                if len(event_log) > self._EVENT_LOG_CAP:
                    drop = len(event_log) - self._EVENT_LOG_CAP
                    event_log = event_log[drop:]
                    truncated += drop
                updated_payload: dict[str, Any] = dict(existing.raw_payload or {})
                updated_payload["_sentinel"] = {
                    "occurrence_count": occurrence_count,
                    "last_seen_at": now_iso,
                    "event_log": event_log,
                    "truncated_entries": truncated,
                }
                existing.raw_payload = updated_payload

                outbox_id = uuid.uuid4()
                outbox_row = OutboxEventModel(
                    id=outbox_id,
                    topic=outbox_topic,
                    key=str(existing.id),
                    payload={
                        "event_id": str(outbox_id),
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
                "truncated_entries": 0,
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
            outbox_id = uuid.uuid4()
            outbox_row = OutboxEventModel(
                id=outbox_id,
                topic=outbox_topic,
                key=str(new_incident.id),
                payload={
                    "event_id": str(outbox_id),
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

    async def write_enrichment_context(
        self,
        *,
        incident_id: UUID,
        event_id: UUID,
        context: IncidentContext,
        assembled_at: datetime,
        outbox_event: OutboxEvent | None = None,
    ) -> EnrichmentWriteResult:
        """Idempotently persist enrichment context for an incident.

        The conditional ``WHERE last_enrichment_event_id IS DISTINCT FROM
        :event_id`` makes a re-delivery of the same enrichment event a no-op:
        Postgres returns zero rows from ``RETURNING`` and we report
        ``duplicate`` without mutating anything. The optional ``outbox_event``
        is INSERTed in the same session so the domain write and the
        downstream-event publish commit atomically.
        """
        ctx_json = context.model_dump(mode="json")
        async with self._session_factory() as s:
            result = await s.execute(
                text(
                    "UPDATE incidents "
                    "SET context_json = CAST(:ctx AS jsonb), "
                    "    context_assembled_at = :assembled_at, "
                    "    context_version = context_version + 1, "
                    "    last_enrichment_event_id = :event_id "
                    "WHERE id = :incident_id "
                    "  AND (last_enrichment_event_id IS DISTINCT FROM :event_id) "
                    "RETURNING context_version"
                ),
                {
                    "ctx": json.dumps(ctx_json),
                    "assembled_at": assembled_at,
                    "event_id": event_id,
                    "incident_id": incident_id,
                },
            )
            row = result.first()
            if row is None:
                # Either the incident does not exist, or the event_id matched
                # the last-applied one — duplicate delivery. Roll back the
                # implicit tx and read back the current version for the caller.
                await s.rollback()
                existing = await s.execute(
                    text("SELECT context_version FROM incidents WHERE id = :id"),
                    {"id": incident_id},
                )
                existing_row = existing.first()
                version = int(existing_row[0]) if existing_row is not None else 0
                return EnrichmentWriteResult(status="duplicate", version=version)

            new_version = int(row[0])
            if outbox_event is not None:
                s.add(
                    OutboxEventModel(
                        id=outbox_event.id,
                        topic=outbox_event.topic,
                        key=outbox_event.key,
                        payload=outbox_event.payload,
                    )
                )
            await s.commit()
            return EnrichmentWriteResult(status="written", version=new_version)

    async def get_enrichment_context(
        self,
        incident_id: UUID,
    ) -> StoredEnrichmentContext | None:
        async with self._session_factory() as s:
            row = (
                await s.execute(
                    text(
                        "SELECT context_json, context_assembled_at, "
                        "       context_version, last_enrichment_event_id "
                        "FROM incidents WHERE id = :id"
                    ),
                    {"id": incident_id},
                )
            ).first()
        if row is None:
            return None
        ctx_json, assembled_at, version, last_event_id = row
        if ctx_json is None or last_event_id is None:
            return None
        return StoredEnrichmentContext(
            context=IncidentContext.model_validate(ctx_json),
            assembled_at=assembled_at,
            version=int(version),
            last_event_id=last_event_id,
        )

    async def recent_for_service(
        self,
        *,
        service: str,
        since: datetime,
        exclude_incident_id: UUID,
    ) -> list[RelatedAlertItem]:
        from sentinel.schemas.ids import related_id

        async with self._session_factory() as s:
            rows = (
                (
                    await s.execute(
                        select(IncidentModel)
                        .where(
                            IncidentModel.service == service,
                            IncidentModel.opened_at >= since,
                            IncidentModel.id != exclude_incident_id,
                        )
                        .order_by(IncidentModel.opened_at.desc())
                    )
                )
                .scalars()
                .all()
            )
            return [
                RelatedAlertItem(
                    id=related_id(row.id),
                    service=row.service,
                    # severity is constrained at the DB level to SEVERITY_VALUES
                    # (CHECK constraint); narrow the str -> SeverityType for the
                    # Pydantic Literal-typed field.
                    severity=cast(SeverityType, row.severity),
                    title=row.title,
                    opened_at=row.opened_at,
                )
                for row in rows
            ]

    async def set_embedding(
        self,
        incident_id: UUID,
        embedding: list[float],
        *,
        event_id: UUID,
    ) -> Literal["written", "duplicate", "unknown_incident"]:
        """Idempotently set incidents.embedding from a memory event.

        Conditional UPDATE: writes only when last_embedding_event_id != :event_id.
        Returns three states so the consumer can pick the right log level / metric.
        """
        # asyncpg does not know how to encode list[float] as a vector literal;
        # serialise to the pgvector wire format ('[f0,f1,...]') before binding.
        vec_str = "[" + ",".join(str(f) for f in embedding) + "]"
        async with self._session_factory() as s:
            stmt = text(
                "UPDATE incidents "
                "SET embedding = CAST(:vec AS vector), "
                "    last_embedding_event_id = :event_id "
                "WHERE id = :incident_id "
                "  AND (last_embedding_event_id IS DISTINCT FROM :event_id) "
                "RETURNING id"
            )
            updated = (
                await s.execute(
                    stmt, {"vec": vec_str, "event_id": event_id, "incident_id": incident_id}
                )
            ).first()
            if updated is not None:
                await s.commit()
                return "written"
            # No row updated: either incident missing or event_id already applied.
            exists = (
                await s.execute(
                    text("SELECT 1 FROM incidents WHERE id = :id"),
                    {"id": incident_id},
                )
            ).first()
            await s.rollback()
            return "duplicate" if exists is not None else "unknown_incident"

    async def load_for_memory(
        self,
        incident_id: UUID,
    ) -> MemoryIncidentRow | None:
        """Fetch service+title for opened path; optionally join resolution for resolved path."""
        async with self._session_factory() as s:
            row = (
                await s.execute(
                    text(
                        "SELECT i.id, i.service, i.title, i.status, "
                        "       r.root_cause, r.remediation, r.diagnosis_was_correct "
                        "FROM incidents i "
                        "LEFT JOIN resolutions r ON r.incident_id = i.id "
                        "WHERE i.id = :id"
                    ),
                    {"id": incident_id},
                )
            ).first()
        if row is None:
            return None
        resolution: ResolutionData | None = None
        if row.root_cause is not None:
            resolution = ResolutionData(
                root_cause=row.root_cause,
                remediation=row.remediation,
                diagnosis_was_correct=row.diagnosis_was_correct,
            )
        return MemoryIncidentRow(
            id=row.id,
            service=row.service,
            title=row.title,
            status=row.status,
            resolution=resolution,
        )

    async def similar_resolved_incidents(
        self,
        *,
        query_embedding: list[float],
        k: int,
        exclude_incident_id: UUID | None,
    ) -> list[SimilarIncidentRow]:
        """Cosine top-k of resolved incidents with a usable resolution.

        Spec filters (§H acceptance):
          - i.status IN ('resolved', 'closed')
          - r.diagnosis_was_correct IS NULL OR r.diagnosis_was_correct = TRUE
          - exclude_incident_id excluded if provided
        Uses pgvector cosine distance; HNSW index serves the ORDER BY.
        """
        qvec_lit = "[" + ",".join(str(f) for f in query_embedding) + "]"
        stmt = text(
            "SELECT i.id, i.title, r.root_cause, r.remediation, "
            "       1 - (i.embedding <=> CAST(:qvec AS vector)) AS cosine_similarity "
            "FROM incidents i "
            "JOIN resolutions r ON r.incident_id = i.id "
            "WHERE i.embedding IS NOT NULL "
            "  AND i.status IN ('resolved', 'closed') "
            "  AND (r.diagnosis_was_correct IS NULL OR r.diagnosis_was_correct = TRUE) "
            "  AND (CAST(:exclude_id AS uuid) IS NULL "
            "       OR i.id <> CAST(:exclude_id AS uuid)) "
            "ORDER BY i.embedding <=> CAST(:qvec AS vector) "
            "LIMIT :k"
        )
        async with self._session_factory() as s:
            rows = (
                await s.execute(
                    stmt,
                    {
                        "qvec": qvec_lit,
                        "exclude_id": str(exclude_incident_id) if exclude_incident_id else None,
                        "k": k,
                    },
                )
            ).all()
        return [
            SimilarIncidentRow(
                id=r.id,
                title=r.title,
                root_cause=r.root_cause,
                remediation=r.remediation,
                cosine_similarity=float(r.cosine_similarity),
            )
            for r in rows
        ]


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

    async def recent_for_service_window(
        self,
        *,
        service: str,
        since: datetime,
    ) -> list[DeployItem]: ...


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

    async def recent_for_service_window(
        self,
        *,
        service: str,
        since: datetime,
    ) -> list[DeployItem]:
        from sentinel.schemas.ids import deploy_id

        async with self._session_factory() as s:
            rows = (
                (
                    await s.execute(
                        select(DeployModel)
                        .where(DeployModel.service == service, DeployModel.deployed_at >= since)
                        .order_by(DeployModel.deployed_at.desc())
                    )
                )
                .scalars()
                .all()
            )
            return [
                DeployItem(
                    id=deploy_id(row.sha),
                    service=row.service,
                    sha=row.sha,
                    pr_number=row.pr_number,
                    pr_title=row.pr_title,
                    pr_diff_summary=row.pr_diff_summary,
                    deployed_at=row.deployed_at,
                    deployed_by=row.deployed_by,
                )
                for row in rows
            ]


# --- Stubs for the rest (implemented as needed in their consuming work areas) ---


class DiagnosisRepository(Protocol):
    async def save_with_outbox(
        self,
        *,
        incident_id: UUID,
        record: PersistedDiagnosis,
        upstream_event_id: UUID,
        outbox_event: OutboxEvent,
    ) -> tuple[UUID, Literal["new", "duplicate"]]: ...


class ResolutionRepository(Protocol):
    async def record(
        self,
        incident_id: UUID,
        resolution: ResolveIncidentRequest,
        *,
        outbox_topic: str,
    ) -> ResolveRecordResult: ...


class RunbookRepository(Protocol):
    # Return type is `RunbookItem` once Work Area H lands; using `Any` here avoids
    # forcing the import (and the dependency direction) before the concrete impl exists.
    async def search(self, embedding: list[float], *, k: int, min_cosine: float) -> list[Any]: ...


class EvalRunRepository(Protocol):
    async def start(self, *, model: str, prompt_version: str, corpus_version: str) -> UUID: ...

    async def complete(self, run_id: UUID, summary: dict[str, float]) -> None: ...


class PostgresDiagnosisRepository:
    """Concrete ``DiagnosisRepository`` — INSERT ... ON CONFLICT DO NOTHING + atomic outbox."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def save_with_outbox(
        self,
        *,
        incident_id: UUID,
        record: PersistedDiagnosis,
        upstream_event_id: UUID,
        outbox_event: OutboxEvent,
    ) -> tuple[UUID, Literal["new", "duplicate"]]:
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                pg_insert(DiagnosisModel)
                .values(
                    incident_id=incident_id,
                    hypothesis=record.hypothesis,
                    confidence=record.confidence,
                    reasoning=record.reasoning,
                    evidence=[ref.model_dump(mode="json") for ref in record.evidence],
                    suggested_actions=[a.model_dump(mode="json") for a in record.suggested_actions],
                    likely_category=record.likely_category,
                    hallucinated_evidence=record.hallucinated_evidence,
                    model=record.model,
                    prompt_version=record.prompt_version,
                    latency_ms=record.latency_ms,
                    token_usage=record.token_usage,
                )
                .on_conflict_do_nothing(constraint="uq_diagnoses_incident_prompt_model")
                .returning(DiagnosisModel.id)
            )
            diagnosis_id = result.scalar_one_or_none()
            if diagnosis_id is None:
                existing = await session.execute(
                    select(DiagnosisModel.id).where(
                        DiagnosisModel.incident_id == incident_id,
                        DiagnosisModel.prompt_version == record.prompt_version,
                        DiagnosisModel.model == record.model,
                    )
                )
                return existing.scalar_one(), "duplicate"

            payload = {**outbox_event.payload, "diagnosis_id": str(diagnosis_id)}
            await session.execute(
                pg_insert(OutboxEventModel).values(
                    id=outbox_event.id,
                    topic=outbox_event.topic,
                    key=outbox_event.key,
                    payload=payload,
                    attempts=0,
                    created_at=outbox_event.created_at,
                )
            )
            return diagnosis_id, "new"


class PostgresResolutionRepository:
    """Concrete ResolutionRepository — atomic INSERT resolution + UPDATE incident + stage outbox."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def record(
        self,
        incident_id: UUID,
        body: ResolveIncidentRequest,
        *,
        outbox_topic: str,
    ) -> ResolveRecordResult:
        from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
        from sentinel.persistence.models import ResolutionModel

        async with self._session_factory() as session, session.begin():
            incident = (
                await session.execute(
                    select(IncidentModel).where(IncidentModel.id == incident_id).with_for_update()
                )
            ).scalar_one_or_none()
            if incident is None:
                raise IncidentNotFound(incident_id)
            if incident.status in ("resolved", "closed"):
                raise IncidentAlreadyResolved(incident_id)

            now = (await session.execute(select(func.now()))).scalar_one()
            now_iso = now.isoformat()

            session.add(
                ResolutionModel(
                    incident_id=incident_id,
                    root_cause=body.root_cause,
                    remediation=body.remediation,
                    category=body.category,
                    diagnosis_was_correct=body.diagnosis_was_correct,
                    notes=body.notes,
                    resolved_by=body.resolved_by,
                )
            )

            incident.status = "resolved"
            incident.resolved_at = now

            event_id = uuid.uuid4()
            session.add(
                OutboxEventModel(
                    id=event_id,
                    topic=outbox_topic,
                    key=str(incident_id),
                    payload={
                        "event_id": str(event_id),
                        "event": "incident.resolved",
                        "incident_id": str(incident_id),
                        "ts": now_iso,
                    },
                )
            )

        return ResolveRecordResult(incident_id=incident_id, resolved_at=now, event_id=event_id)


# Concrete classes for the stubbed Protocols above land with their consumers
# (Work Areas G, H, K). Keeping the Protocols here means downstream modules
# can depend on the interface without forcing the implementation now.
__all__ = [
    "DeployRepository",
    "DeployRow",
    "DiagnosisRepository",
    "EnrichmentWriteResult",
    "EvalRunRepository",
    "IncidentRepository",
    "IngestResult",
    "MemoryIncidentRow",
    "OutboxBatch",
    "OutboxEvent",
    "OutboxRepository",
    "PostgresDeployRepository",
    "PostgresDiagnosisRepository",
    "PostgresIncidentRepository",
    "PostgresOutboxRepository",
    "PostgresResolutionRepository",
    "ResolutionData",
    "ResolutionRepository",
    "ResolveRecordResult",
    "RunbookRepository",
    "SimilarIncidentRow",
    "StoredEnrichmentContext",
]
