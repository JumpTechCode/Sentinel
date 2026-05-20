# migrations/versions/0006_eval_runs_and_case_results.py
"""eval harness schema: replace placeholder eval_runs, add eval_case_results

Revision ID: 0006_eval_runs_and_case_results
Revises: 0005_embedding_1024_event_id
Create Date: 2026-05-20

PR 1 of Work Area K (eval harness). Replaces the placeholder eval_runs shape
landed in 0001_initial (id, started_at, completed_at, model, prompt_version,
corpus_version, results, summary) with the production shape (reproducibility
metadata, metrics, regression result). Also adds eval_case_results — one row
per (run, case, shot) including the raw LLM response, parsed Diagnosis, and
a denormalised snapshot of the incident under test (so results survive the
runner truncating the incidents table between cases).

The placeholder eval_runs is empty in any DB — no producer code writes to it
(verified by grep across sentinel/ and tests/). drop_table + create_table is
the cleanest path; multi-step ALTER would be net-equivalent but much harder
to review.

Reversibility (invariant 8): both upgrade() and downgrade() implemented and
exercised by tests/integration/persistence/test_migration_0006.py. Downgrade
recreates the placeholder eval_runs shape verbatim so 0001..0005 remain a
valid bound on a downgraded DB.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0006_eval_runs_and_case_results"
down_revision = "0005_embedding_1024_event_id"
branch_labels = None
depends_on = None


_STATUS_VALUES = ("running", "ok", "failed", "partial")
_TRIGGER_VALUES = ("local", "ci-smoke", "ci-nightly", "baseline", "manual")
_CASE_STATUS_VALUES = (
    "ok",
    "timeout",
    "ingest_failed",
    "schema_failed",
    "rate_limited",
)


def _in_clause(col: str, values: tuple[str, ...]) -> str:
    # repr() on string-of-no-special-chars produces valid single-quoted SQL literals.
    # Values are a module-level whitelist, not user input — safe.
    # Same pattern used by sentinel/persistence/models.py:_check_in for category enums.
    return f"{col} IN ({','.join(repr(v) for v in values)})"


def upgrade() -> None:
    # --- eval_runs: drop placeholder, create production shape ---
    op.drop_table("eval_runs")
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
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("trigger", sa.Text(), nullable=False),
        sa.Column("git_sha", sa.Text(), nullable=False),
        sa.Column("model", sa.Text(), nullable=False),
        sa.Column("prompt_version", sa.Text(), nullable=False),
        sa.Column("embedding_model_id", sa.Text(), nullable=False),
        sa.Column("corpus_version", sa.Text(), nullable=False),
        sa.Column("corpus_size", sa.Integer(), nullable=False),
        sa.Column("shots_per_case", sa.Integer(), nullable=False),
        sa.Column("fetcher_fixture_hash", sa.Text(), nullable=False),
        sa.Column(
            "metrics",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "metrics_stability",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("regression_baseline_sha", sa.Text(), nullable=True),
        sa.Column("regression_passed", sa.Boolean(), nullable=True),
        sa.Column("regression_detail", postgresql.JSONB(), nullable=True),
        sa.Column(
            "extra",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.CheckConstraint(_in_clause("status", _STATUS_VALUES), name="ck_eval_runs_status_valid"),
        sa.CheckConstraint(
            _in_clause("trigger", _TRIGGER_VALUES), name="ck_eval_runs_trigger_valid"
        ),
    )
    op.create_index("ix_eval_runs_started_at", "eval_runs", [sa.text("started_at DESC")])
    op.create_index("ix_eval_runs_status", "eval_runs", ["status"])

    # --- eval_case_results: create ---
    op.create_table(
        "eval_case_results",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("eval_runs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("case_id", sa.Text(), nullable=False),
        sa.Column("shot_index", sa.Integer(), nullable=False),
        sa.Column("case_status", sa.Text(), nullable=False),
        sa.Column(
            "metrics",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column("raw_response", postgresql.JSONB(), nullable=True),
        sa.Column("diagnosis", postgresql.JSONB(), nullable=True),
        # Intentionally NO FK to incidents: the eval runner truncates the incidents
        # table between cases, which would cascade-delete every prior
        # eval_case_result row if a FK existed. incident_id is stored as a bare
        # UUID for forensic correlation only — the row stays readable even after
        # the incident is gone, thanks to the denormalised columns below.
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("incident_fingerprint", sa.Text(), nullable=True),
        sa.Column("incident_title", sa.Text(), nullable=True),
        sa.Column("incident_severity", sa.Text(), nullable=True),
        sa.Column("token_usage", postgresql.JSONB(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.UniqueConstraint(
            "run_id", "case_id", "shot_index", name="uq_eval_case_results_run_case_shot"
        ),
        sa.CheckConstraint(
            _in_clause("case_status", _CASE_STATUS_VALUES),
            name="ck_eval_case_results_status_valid",
        ),
    )
    op.create_index("ix_eval_case_results_run_id", "eval_case_results", ["run_id"])
    op.create_index("ix_eval_case_results_case_run", "eval_case_results", ["case_id", "run_id"])


def downgrade() -> None:
    # --- drop eval_case_results ---
    op.drop_index("ix_eval_case_results_case_run", table_name="eval_case_results")
    op.drop_index("ix_eval_case_results_run_id", table_name="eval_case_results")
    op.drop_table("eval_case_results")

    # --- restore placeholder eval_runs shape verbatim from 0001_initial ---
    op.drop_index("ix_eval_runs_status", table_name="eval_runs")
    op.drop_index("ix_eval_runs_started_at", table_name="eval_runs")
    op.drop_table("eval_runs")
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
