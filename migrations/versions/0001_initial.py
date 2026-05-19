# migrations/versions/0001_initial.py
"""initial schema: incidents, diagnoses, resolutions, deploys, runbooks, eval_runs

Revision ID: 0001_initial
Revises:
Create Date: 2026-05-18

This migration creates the entire V1 schema in one shot. We intentionally do
not split per-table — they are mutually-referencing and HNSW indexes need raw
SQL, so a single file keeps the operation auditable.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    op.create_table(
        "incidents",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'open'")),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "opened_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("resolved_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.UniqueConstraint("source", "external_id", name="uq_incidents_source_external"),
        sa.CheckConstraint(
            "source IN ('sentry','pagerduty','datadog','generic')",
            name="ck_source_valid",
        ),
        sa.CheckConstraint("severity IN ('SEV1','SEV2','SEV3','SEV4')", name="ck_severity_valid"),
        sa.CheckConstraint(
            "status IN ('open','diagnosing','mitigated','resolved','closed')",
            name="ck_status_valid",
        ),
    )
    op.create_index(
        "idx_incidents_service_opened",
        "incidents",
        ["service", sa.text("opened_at DESC")],
    )
    op.create_index("idx_incidents_fingerprint", "incidents", ["fingerprint"])
    op.execute(
        "CREATE INDEX idx_incidents_embedding ON incidents "
        "USING hnsw (embedding vector_cosine_ops)"
    )

    op.create_table(
        "diagnoses",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("hypothesis", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Numeric(3, 2), nullable=False),
        sa.Column("reasoning", sa.Text(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column("suggested_actions", postgresql.JSONB(), nullable=False),
        sa.Column("likely_category", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.Column("token_usage", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("confidence BETWEEN 0 AND 1", name="ck_diagnoses_confidence_unit"),
        sa.CheckConstraint(
            "likely_category IN ('deploy','config','dependency','capacity','data','external')",
            name="ck_likely_category_valid",
        ),
    )
    op.create_index(
        "idx_diagnoses_incident",
        "diagnoses",
        ["incident_id", sa.text("created_at DESC")],
    )

    op.create_table(
        "resolutions",
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("root_cause", sa.Text(), nullable=False),
        sa.Column("remediation", sa.Text(), nullable=False),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("diagnosis_was_correct", sa.Boolean(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("resolved_by", sa.Text(), nullable=True),
        sa.Column(
            "resolved_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "category IN ('deploy','config','dependency','capacity','data','external')",
            name="ck_resolutions_category_valid",
        ),
    )

    op.create_table(
        "deploys",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("sha", sa.Text(), nullable=False),
        sa.Column("pr_number", sa.Integer(), nullable=True),
        sa.Column("pr_title", sa.Text(), nullable=True),
        sa.Column("pr_diff_summary", sa.Text(), nullable=True),
        sa.Column("deployed_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("deployed_by", sa.Text(), nullable=True),
    )
    op.create_index(
        "idx_deploys_service_time",
        "deploys",
        ["service", sa.text("deployed_at DESC")],
    )

    op.create_table(
        "runbooks",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("service", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(1536), nullable=True),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.execute(
        "CREATE INDEX idx_runbooks_embedding ON runbooks "
        "USING hnsw (embedding vector_cosine_ops)"
    )

    op.create_table(
        "eval_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "started_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("completed_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("corpus_version", sa.Text(), nullable=False),
        sa.Column("results", postgresql.JSONB(), nullable=True),
        sa.Column("summary", postgresql.JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("eval_runs")
    op.execute("DROP INDEX IF EXISTS idx_runbooks_embedding")
    op.drop_table("runbooks")
    op.drop_table("deploys")
    op.drop_table("resolutions")
    op.drop_index("idx_diagnoses_incident", table_name="diagnoses")
    op.drop_table("diagnoses")
    op.execute("DROP INDEX IF EXISTS idx_incidents_embedding")
    op.drop_index("idx_incidents_fingerprint", table_name="incidents")
    op.drop_index("idx_incidents_service_opened", table_name="incidents")
    op.drop_table("incidents")
    # Do NOT drop extensions — other migrations / databases may depend on them
    # and pgcrypto is harmless to leave in place.
