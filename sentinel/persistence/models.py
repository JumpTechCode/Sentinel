# sentinel/persistence/models.py
"""SQLAlchemy 2.x async ORM models, exact match of the spec's DDL.

Conventions:
- All columns are typed via `Mapped[...]`.
- Check constraints mirror the spec verbatim — autogenerate picks them up.
- Vector columns use pgvector's SA type; index creation (HNSW) lives in the
  migration, not here (HNSW is not first-class in SA's autogenerate).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sentinel.schemas.enums import (
    CATEGORY_VALUES,
    INCIDENT_STATUS_VALUES,
    SEVERITY_VALUES,
    SOURCE_VALUES,
)


def _check_in(col: str, values: tuple[str, ...]) -> CheckConstraint:
    quoted = ",".join(f"'{v}'" for v in values)
    return CheckConstraint(f"{col} IN ({quoted})", name=f"ck_{col}_valid")


class Base(DeclarativeBase):
    pass


class IncidentModel(Base):
    __tablename__ = "incidents"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    service: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="open")
    title: Mapped[str] = mapped_column(Text, nullable=False)
    fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    opened_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    resolved_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    last_embedding_event_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), nullable=True
    )
    context_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    context_assembled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    context_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_enrichment_event_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), nullable=True
    )

    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_incidents_source_external"),
        _check_in("source", SOURCE_VALUES),
        _check_in("severity", SEVERITY_VALUES),
        _check_in("status", INCIDENT_STATUS_VALUES),
        Index("idx_incidents_service_opened", "service", text("opened_at DESC")),
        Index("idx_incidents_fingerprint", "fingerprint"),
        Index(
            "ix_incidents_last_enrichment_event_id",
            "last_enrichment_event_id",
            postgresql_where=text("last_enrichment_event_id IS NOT NULL"),
        ),
        Index(
            "ix_incidents_last_embedding_event_id",
            "last_embedding_event_id",
            postgresql_where=text("last_embedding_event_id IS NOT NULL"),
        ),
    )


class OutboxEventModel(Base):
    __tablename__ = "outbox_events"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class DiagnosisModel(Base):
    __tablename__ = "diagnoses"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    incident_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("incidents.id", ondelete="CASCADE"),
        nullable=False,
    )
    hypothesis: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[Decimal] = mapped_column(Numeric(3, 2), nullable=False)
    reasoning: Mapped[str] = mapped_column(Text, nullable=False)
    evidence: Mapped[Any] = mapped_column(JSONB, nullable=False)
    suggested_actions: Mapped[Any] = mapped_column(JSONB, nullable=False)
    likely_category: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_version: Mapped[str] = mapped_column(Text, nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    token_usage: Mapped[Any] = mapped_column(JSONB, nullable=False)
    hallucinated_evidence: Mapped[bool] = mapped_column(Boolean, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("confidence BETWEEN 0 AND 1", name="ck_diagnoses_confidence_unit"),
        _check_in("likely_category", CATEGORY_VALUES),
        Index("idx_diagnoses_incident", "incident_id", text("created_at DESC")),
        UniqueConstraint(
            "incident_id",
            "prompt_version",
            "model",
            name="uq_diagnoses_incident_prompt_model",
        ),
    )


class ResolutionModel(Base):
    __tablename__ = "resolutions"

    incident_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("incidents.id", ondelete="CASCADE"),
        primary_key=True,
    )
    root_cause: Mapped[str] = mapped_column(Text, nullable=False)
    remediation: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    diagnosis_was_correct: Mapped[bool | None] = mapped_column(nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            f"category IN ({','.join(repr(v) for v in CATEGORY_VALUES)})",
            name="ck_resolutions_category_valid",
        ),
    )


class DeployModel(Base):
    __tablename__ = "deploys"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    service: Mapped[str] = mapped_column(Text, nullable=False)
    sha: Mapped[str] = mapped_column(Text, nullable=False)
    pr_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pr_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    pr_diff_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    deployed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    deployed_by: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (Index("idx_deploys_service_time", "service", text("deployed_at DESC")),)


class RunbookModel(Base):
    __tablename__ = "runbooks"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    service: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class EvalRunModel(Base):
    __tablename__ = "eval_runs"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    trigger: Mapped[str] = mapped_column(Text, nullable=False)
    git_sha: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    prompt_version: Mapped[str] = mapped_column(Text, nullable=False)
    embedding_model_id: Mapped[str] = mapped_column(Text, nullable=False)
    corpus_version: Mapped[str] = mapped_column(Text, nullable=False)
    corpus_size: Mapped[int] = mapped_column(Integer, nullable=False)
    shots_per_case: Mapped[int] = mapped_column(Integer, nullable=False)
    fetcher_fixture_hash: Mapped[str] = mapped_column(Text, nullable=False)
    metrics: Mapped[Any] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    metrics_stability: Mapped[Any] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    regression_baseline_sha: Mapped[str | None] = mapped_column(Text, nullable=True)
    regression_passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    regression_detail: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    extra: Mapped[Any] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    __table_args__ = (
        CheckConstraint(
            "status IN ('running','ok','failed','partial')",
            name="ck_eval_runs_status_valid",
        ),
        CheckConstraint(
            "trigger IN ('local','ci-smoke','ci-nightly','baseline','manual')",
            name="ck_eval_runs_trigger_valid",
        ),
        Index("ix_eval_runs_started_at", text("started_at DESC")),
        Index("ix_eval_runs_status", "status"),
    )


class EvalCaseResultModel(Base):
    __tablename__ = "eval_case_results"

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("eval_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    case_id: Mapped[str] = mapped_column(Text, nullable=False)
    shot_index: Mapped[int] = mapped_column(Integer, nullable=False)
    case_status: Mapped[str] = mapped_column(Text, nullable=False)
    metrics: Mapped[Any] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    raw_response: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    diagnosis: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    # Intentionally NO ForeignKey to incidents — see migration 0006 docstring.
    incident_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    # Denormalised incident snapshot — survives the runner's truncation of
    # the incidents table between cases (Braintrust / Inspect AI pattern).
    incident_fingerprint: Mapped[str | None] = mapped_column(Text, nullable=True)
    incident_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    incident_severity: Mapped[str | None] = mapped_column(Text, nullable=True)
    token_usage: Mapped[Any | None] = mapped_column(JSONB, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "run_id", "case_id", "shot_index", name="uq_eval_case_results_run_case_shot"
        ),
        CheckConstraint(
            "case_status IN ('ok','timeout','ingest_failed','schema_failed','rate_limited')",
            name="ck_eval_case_results_status_valid",
        ),
        Index("ix_eval_case_results_run_id", "run_id"),
        Index("ix_eval_case_results_case_run", "case_id", "run_id"),
    )
