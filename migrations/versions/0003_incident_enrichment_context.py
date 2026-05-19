# migrations/versions/0003_incident_enrichment_context.py
"""incident enrichment context columns

Revision ID: 0003_incident_enrichment_context
Revises: 0002_outbox_events
Create Date: 2026-05-19

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID

revision = "0003_incident_enrichment_context"
down_revision = "0002_outbox_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "incidents",
        sa.Column("context_json", JSONB, nullable=True),
    )
    op.add_column(
        "incidents",
        sa.Column("context_assembled_at", TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column(
        "incidents",
        sa.Column(
            "context_version",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "incidents",
        sa.Column("last_enrichment_event_id", PgUUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_incidents_last_enrichment_event_id",
        "incidents",
        ["last_enrichment_event_id"],
        postgresql_where=sa.text("last_enrichment_event_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_incidents_last_enrichment_event_id", table_name="incidents")
    op.drop_column("incidents", "last_enrichment_event_id")
    op.drop_column("incidents", "context_version")
    op.drop_column("incidents", "context_assembled_at")
    op.drop_column("incidents", "context_json")
