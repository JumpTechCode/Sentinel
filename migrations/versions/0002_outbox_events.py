# migrations/versions/0002_outbox_events.py
"""outbox_events

Revision ID: 0002_outbox_events
Revises: 0001_initial
Create Date: 2026-05-18

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID

revision = "0002_outbox_events"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column(
            "id",
            PgUUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("topic", sa.Text, nullable=False),
        sa.Column("key", sa.Text, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column(
            "created_at",
            TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("published_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "attempts",
            sa.Integer,
            nullable=False,
            server_default="0",
        ),
        sa.Column("last_attempt_at", TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text, nullable=True),
    )
    op.create_index(
        "idx_outbox_unpublished",
        "outbox_events",
        [sa.text("created_at")],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_outbox_unpublished", table_name="outbox_events")
    op.drop_table("outbox_events")
