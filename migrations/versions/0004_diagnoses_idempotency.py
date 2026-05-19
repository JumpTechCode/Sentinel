# migrations/versions/0004_diagnoses_idempotency.py
"""diagnoses idempotency: hallucinated_evidence column + uniq(incident_id, prompt_version, model)

Revision ID: 0004_diagnoses_idempotency
Revises: 0003_incident_enrichment_context
Create Date: 2026-05-19

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004_diagnoses_idempotency"
down_revision = "0003_incident_enrichment_context"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "diagnoses",
        sa.Column(
            "hallucinated_evidence",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    # Drop the server default; future inserts pass the value explicitly.
    op.alter_column("diagnoses", "hallucinated_evidence", server_default=None)
    op.create_unique_constraint(
        "uq_diagnoses_incident_prompt_model",
        "diagnoses",
        ["incident_id", "prompt_version", "model"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_diagnoses_incident_prompt_model", "diagnoses", type_="unique")
    op.drop_column("diagnoses", "hallucinated_evidence")
