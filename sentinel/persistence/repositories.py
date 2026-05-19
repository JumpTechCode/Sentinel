# sentinel/persistence/repositories.py
"""Repository layer — the only module that may write SQL.

Each repository exposes a `Protocol` (the interface other modules depend on)
plus a Postgres-backed concrete implementation. Methods accept and return
Pydantic schemas where they cross a module boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sentinel.persistence.models import (
    DeployModel,
    IncidentModel,
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


# --- Incident repository ---------------------------------------------------- #


class IncidentRepository(Protocol):
    async def create_from_alert(self, alert: NormalizedAlert, *, fingerprint: str) -> UUID: ...

    async def get(self, incident_id: UUID) -> IncidentDetailResponse | None: ...

    async def list_recent(self, *, limit: int = 50) -> list[IncidentListItem]: ...

    async def mark_diagnosing(self, incident_id: UUID) -> None: ...


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
    "PostgresDeployRepository",
    "PostgresIncidentRepository",
    "ResolutionRepository",
    "RunbookRepository",
]
