# migrations/versions/0005_embedding_1024_event_id.py
"""switch embedding columns to 1024 dim (bge-large) and add last_embedding_event_id

Revision ID: 0005_embedding_1024_event_id
Revises: 0004_diagnoses_idempotency
Create Date: 2026-05-19

The existing Vector(1536) columns were aspirational and never populated.
Switching to Vector(1024) to match BAAI/bge-large-en-v1.5 dimensions.
pgvector does not support ALTER COLUMN dim; drop+recreate is required.

Reversibility (invariant 8): both upgrade() and downgrade() are implemented
and tested round-trip in tests/integration/memory/test_migration_0005.py.
**Both directions are destructive of embedding data**: pgvector requires
drop+recreate to change column dimensions, so any 1024-dim embeddings in
incidents/runbooks are lost on downgrade (recreated as Vector(1536) NULL),
and any 1536-dim embeddings would be lost on a subsequent re-upgrade. The
column was empty pre-0005 so the upgrade itself is data-safe; operators
running downgrade on a populated DB should accept the loss explicitly.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

revision = "0005_embedding_1024_event_id"
down_revision = "0004_diagnoses_idempotency"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_incidents_embedding")
    op.execute("DROP INDEX IF EXISTS idx_runbooks_embedding")

    op.drop_column("incidents", "embedding")
    op.drop_column("runbooks", "embedding")

    op.add_column("incidents", sa.Column("embedding", Vector(1024), nullable=True))
    op.add_column("runbooks", sa.Column("embedding", Vector(1024), nullable=True))

    op.add_column(
        "incidents",
        sa.Column("last_embedding_event_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_incidents_last_embedding_event_id",
        "incidents",
        ["last_embedding_event_id"],
        postgresql_where=sa.text("last_embedding_event_id IS NOT NULL"),
    )

    op.execute(
        "CREATE INDEX idx_incidents_embedding ON incidents "
        "USING hnsw (embedding vector_cosine_ops)"
    )
    op.execute(
        "CREATE INDEX idx_runbooks_embedding ON runbooks "
        "USING hnsw (embedding vector_cosine_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_incidents_embedding")
    op.execute("DROP INDEX IF EXISTS idx_runbooks_embedding")
    op.drop_index("ix_incidents_last_embedding_event_id", table_name="incidents")
    op.drop_column("incidents", "last_embedding_event_id")
    op.drop_column("incidents", "embedding")
    op.drop_column("runbooks", "embedding")
    op.add_column("incidents", sa.Column("embedding", Vector(1536), nullable=True))
    op.add_column("runbooks", sa.Column("embedding", Vector(1536), nullable=True))
    op.execute(
        "CREATE INDEX idx_incidents_embedding ON incidents "
        "USING hnsw (embedding vector_cosine_ops)"
    )
    op.execute(
        "CREATE INDEX idx_runbooks_embedding ON runbooks "
        "USING hnsw (embedding vector_cosine_ops)"
    )
