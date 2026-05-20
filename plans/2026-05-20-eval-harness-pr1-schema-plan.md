# Eval Harness PR 1 — Schema + Repository Plumbing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the persistence layer for the eval harness — alembic migration that replaces the placeholder `eval_runs` table with the production shape and adds `eval_case_results` (with denormalised incident snapshots so results survive cross-case truncation), the matching SQLAlchemy ORM models, the concrete `EvalRunRepository` (finalize raises on zero-row matches to surface programming bugs), and a new `DiagnosisRepository.get_by_incident_id()` method needed by the eval runner in PR 3. No runner, no scoring, no CI changes — pure persistence-layer plumbing.

**Architecture:** Migration 0006 DROPs the placeholder `eval_runs` table (empty in any DB — verified no producer exists) and recreates it in production shape, then CREATEs `eval_case_results` (one row per run × case × shot, with raw LLM response + token usage + denormalised incident snapshot). Both tables live in the default (public) schema. The `EvalRunRepository` Protocol stub at `repositories.py:975-979` is replaced; `PostgresEvalRunRepository` is added alongside the other Postgres* classes. `DiagnosisRepository.get_by_incident_id` lands on the same Protocol+concrete to support the runner's diagnosis-polling loop. A new `EvalRunNotFoundOrAlreadyFinalized` exception is added to `persistence/errors.py`.

**Tech Stack:** Python 3.12, SQLAlchemy 2.0 async, asyncpg, Alembic 1.14, pgvector/pgvector:pg16, pytest + testcontainers-postgres.

**Spec reference:** `plans/2026-05-20-eval-harness-design.md` §6 (Persistence) + §8 PR 1 (Schema + repository plumbing). This plan covers PR 1 only; PRs 2–4 each get their own brainstorm/plan cycle.

**Review-pass adjustments folded in (2026-05-20):**
- Migration uses `drop_table + create_table` instead of multi-step ALTER (placeholder is empty; net result is identical, dramatically simpler)
- `eval_case_results` gains denormalised `incident_fingerprint`, `incident_title`, `incident_severity` columns so results outlive the truncation between cases (Braintrust / Inspect AI pattern)
- `finalize_run` raises `EvalRunNotFoundOrAlreadyFinalized` on zero rows (silent no-op was hiding programming bugs — idempotency belongs at retry boundaries)
- Plus several code-correctness fixes caught in the verification pass (see inline)

---

## File Structure

**Migrations:**
- Create: `migrations/versions/0006_eval_runs_and_case_results.py`

**Errors:**
- Modify: `sentinel/persistence/errors.py` (append `EvalRunNotFoundOrAlreadyFinalized`)

**ORM models (single file convention — all models live in `models.py`):**
- Modify: `sentinel/persistence/models.py` (replace `EvalRunModel`; append `EvalCaseResultModel`)

**Repositories (single file convention — flat `repositories.py`, not a package):**
- Modify: `sentinel/persistence/repositories.py`
  - Top-level imports: add `EvalCaseResultModel`, `EvalRunModel` (alongside existing `DiagnosisModel`/`IncidentModel`/etc.)
  - Add `EvalRunRecord` + `EvalCaseResultRecord` dataclasses near other value-objects
  - Replace `EvalRunRepository` Protocol stub
  - Append concrete `PostgresEvalRunRepository`
  - Add `get_by_incident_id` method to `DiagnosisRepository` Protocol + `PostgresDiagnosisRepository` concrete
  - Update `__all__` exports

**Module exports:**
- Modify: `sentinel/persistence/__init__.py` (export `EvalCaseResultModel`, `EvalRunRecord`, `EvalCaseResultRecord`, `PostgresEvalRunRepository`, `EvalRunNotFoundOrAlreadyFinalized`)

**Tests:**
- Create: `tests/unit/persistence/test_eval_run_repository.py` (dataclass + Protocol shape)
- Create: `tests/integration/persistence/test_eval_run_repository_e2e.py` (real Postgres round-trip + finalize-raises + duplicate-shot)
- Create: `tests/integration/persistence/test_migration_0006.py` (upgrade/downgrade round-trip)
- Create: `tests/unit/persistence/test_diagnosis_repository.py` (Protocol shape for `get_by_incident_id`)
- Create: `tests/integration/persistence/test_diagnosis_repository_e2e.py` (real Postgres round-trip for the read path)
- Modify: `tests/integration/persistence/conftest.py` (add autouse function-scoped truncate fixture for eval tables)
- Modify: `tests/unit/persistence/test_models_metadata.py` (update `EvalRunModel` assertions; add `EvalCaseResultModel`; extend any "all tables registered" check)

---

## Task 0: Create feature branch

**Files:** none

- [ ] **Step 1: Confirm current branch state**

Run: `git status && git branch --show-current`
Expected: working tree clean; on `main` (or whatever long-lived branch). If dirty, stash or commit before continuing.

- [ ] **Step 2: Create and check out feature branch**

Run: `git checkout -b feat/eval-harness-pr1-schema`
Expected: switched to new branch.

---

## Task 1: Write the failing migration round-trip test

**Files:**
- Create: `tests/integration/persistence/test_migration_0006.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/persistence/test_migration_0006.py
"""Migration 0006 produces the expected eval schema and is reversible."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


async def _column_exists(engine, table: str, column: str) -> bool:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_name = :table AND column_name = :column"
                ),
                {"table": table, "column": column},
            )
        ).first()
    return row is not None


async def _table_exists(engine, table: str) -> bool:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("SELECT 1 FROM information_schema.tables WHERE table_name = :t"),
                {"t": table},
            )
        ).first()
    return row is not None


async def _constraint_exists(engine, table: str, constraint: str) -> bool:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT 1 FROM information_schema.table_constraints "
                    "WHERE table_name = :t AND constraint_name = :c"
                ),
                {"t": table, "c": constraint},
            )
        ).first()
    return row is not None


def test_upgrade_produces_full_eval_schema(pg_dsn: str) -> None:
    async def _check() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            # eval_runs production columns
            for col in (
                "id",
                "started_at",
                "completed_at",
                "status",
                "trigger",
                "git_sha",
                "model",
                "prompt_version",
                "embedding_model_id",
                "corpus_version",
                "corpus_size",
                "shots_per_case",
                "fetcher_fixture_hash",
                "metrics",
                "metrics_stability",
                "regression_baseline_sha",
                "regression_passed",
                "regression_detail",
                "extra",
            ):
                assert await _column_exists(engine, "eval_runs", col), f"eval_runs.{col} missing"
            # placeholder columns gone
            assert not await _column_exists(engine, "eval_runs", "results")
            assert not await _column_exists(engine, "eval_runs", "summary")

            # eval_case_results table + columns (including denormalised incident snapshot)
            assert await _table_exists(engine, "eval_case_results")
            for col in (
                "id",
                "run_id",
                "case_id",
                "shot_index",
                "case_status",
                "metrics",
                "raw_response",
                "diagnosis",
                "incident_id",
                "incident_fingerprint",
                "incident_title",
                "incident_severity",
                "token_usage",
                "latency_ms",
                "error_detail",
            ):
                assert await _column_exists(
                    engine, "eval_case_results", col
                ), f"eval_case_results.{col} missing"

            # CHECK constraints
            assert await _constraint_exists(engine, "eval_runs", "ck_eval_runs_status_valid")
            assert await _constraint_exists(engine, "eval_runs", "ck_eval_runs_trigger_valid")
            assert await _constraint_exists(
                engine, "eval_case_results", "ck_eval_case_results_status_valid"
            )

            # UNIQUE constraint
            assert await _constraint_exists(
                engine, "eval_case_results", "uq_eval_case_results_run_case_shot"
            )
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_check())


def test_downgrade_restores_placeholder_eval_runs(pg_dsn: str) -> None:
    async def _check_post_downgrade() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            # eval_case_results gone
            assert not await _table_exists(engine, "eval_case_results")
            # eval_runs back to placeholder shape: results + summary restored, prod cols gone
            assert await _column_exists(engine, "eval_runs", "results")
            assert await _column_exists(engine, "eval_runs", "summary")
            assert not await _column_exists(engine, "eval_runs", "status")
            assert not await _column_exists(engine, "eval_runs", "metrics")
            assert not await _column_exists(engine, "eval_runs", "fetcher_fixture_hash")
            # placeholder shape is also reachable via the model column
            assert await _column_exists(engine, "eval_runs", "model")
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0005_embedding_1024_event_id")
    asyncio.run(_check_post_downgrade())
    # Restore head so subsequent tests start in a known state.
    command.upgrade(cfg, "head")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/integration/persistence/test_migration_0006.py -v -m integration`
Expected: FAIL — "no such revision 0006_eval_runs_and_case_results" or "FileNotFoundError" (migration doesn't exist yet).

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/integration/persistence/test_migration_0006.py
git commit -m "test(persistence): failing round-trip test for migration 0006"
```

---

## Task 2: Write migration 0006 (drop+create, not ALTER)

**Files:**
- Create: `migrations/versions/0006_eval_runs_and_case_results.py`

The placeholder `eval_runs` table is empty (no producer code exists — verified via `grep -rn "INSERT.*eval_runs\|EvalRunModel(\|EvalRunRepository.start" sentinel/ tests/`). A multi-step ALTER chain (add 13 nullable, flip 7 to NOT NULL, drop 2) would be net-equivalent but much harder to read and review. Drop + recreate is the cleaner pattern when there's no data to preserve.

- [ ] **Step 1: Write the migration**

```python
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
        sa.CheckConstraint(
            _in_clause("status", _STATUS_VALUES), name="ck_eval_runs_status_valid"
        ),
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
    op.create_index(
        "ix_eval_case_results_case_run", "eval_case_results", ["case_id", "run_id"]
    )


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
```

- [ ] **Step 2: Run the migration test to verify it passes**

Run: `pytest tests/integration/persistence/test_migration_0006.py -v -m integration`
Expected: PASS — both upgrade and downgrade round-trips green.

- [ ] **Step 3: Verify `make migrate` works locally against the dev DB**

Run: `make compose-up && make migrate`
Expected: alembic upgrades through `0006_eval_runs_and_case_results`. No errors.

- [ ] **Step 4: Verify downgrade works**

Run: `make migrate-down`
Expected: rolls back to `0005_embedding_1024_event_id` cleanly.

- [ ] **Step 5: Re-upgrade to leave the DB at head**

Run: `make migrate`
Expected: re-applies 0006 cleanly.

- [ ] **Step 6: Commit the migration**

```bash
git add migrations/versions/0006_eval_runs_and_case_results.py
git commit -m "feat(persistence): migration 0006 — eval_runs production shape + eval_case_results"
```

---

## Task 3: Update SQLAlchemy ORM models

**Files:**
- Modify: `sentinel/persistence/models.py` (replace `EvalRunModel`; append `EvalCaseResultModel`)
- Modify: `tests/unit/persistence/test_models_metadata.py` (TDD — update assertions first)

- [ ] **Step 1: Read the current test to find what needs updating**

Run: `cat tests/unit/persistence/test_models_metadata.py`

Note any existing `EvalRunModel`-related assertions and any "all expected tables" set assertion (call it `test_all_tables_registered_on_base` or similar) — both need updating.

- [ ] **Step 2: Update the metadata test (TDD — test first)**

Replace any existing block that asserts only `EvalRunModel.__tablename__ == "eval_runs"`. Add new assertions:

```python
# tests/unit/persistence/test_models_metadata.py — additions

def test_eval_run_model_has_production_columns() -> None:
    cols = {c.name for c in EvalRunModel.__table__.columns}
    expected = {
        "id",
        "started_at",
        "completed_at",
        "status",
        "trigger",
        "git_sha",
        "model",
        "prompt_version",
        "embedding_model_id",
        "corpus_version",
        "corpus_size",
        "shots_per_case",
        "fetcher_fixture_hash",
        "metrics",
        "metrics_stability",
        "regression_baseline_sha",
        "regression_passed",
        "regression_detail",
        "extra",
    }
    assert expected <= cols, f"missing columns: {expected - cols}"
    assert "results" not in cols, "placeholder column should be gone"
    assert "summary" not in cols, "placeholder column should be gone"


def test_eval_case_result_model_exists() -> None:
    from sentinel.persistence.models import EvalCaseResultModel  # noqa: PLC0415 — lazy

    cols = {c.name for c in EvalCaseResultModel.__table__.columns}
    assert EvalCaseResultModel.__tablename__ == "eval_case_results"
    expected = {
        "id",
        "run_id",
        "case_id",
        "shot_index",
        "case_status",
        "metrics",
        "raw_response",
        "diagnosis",
        "incident_id",
        "incident_fingerprint",
        "incident_title",
        "incident_severity",
        "token_usage",
        "latency_ms",
        "error_detail",
    }
    assert expected <= cols, f"missing columns: {expected - cols}"
```

If the file has an "all expected tables registered on Base.metadata" check (any test that asserts `{...} <= {t.name for t in Base.metadata.tables.values()}`), add `"eval_case_results"` to its expected set.

- [ ] **Step 3: Run the test — confirm it fails for the right reason**

Run: `pytest tests/unit/persistence/test_models_metadata.py -v`
Expected: FAIL — `test_eval_run_model_has_production_columns` fails (missing columns), `test_eval_case_result_model_exists` fails (ImportError or missing columns).

- [ ] **Step 4: Replace `EvalRunModel` in `sentinel/persistence/models.py`**

Replace the existing class at lines ~220-234 with:

```python
# sentinel/persistence/models.py — replace EvalRunModel

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
```

- [ ] **Step 5: Append `EvalCaseResultModel` (with denormalised incident snapshot)**

```python
# sentinel/persistence/models.py — append after EvalRunModel

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
    metrics: Mapped[Any] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
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
```

- [ ] **Step 6: Re-run the metadata test**

Run: `pytest tests/unit/persistence/test_models_metadata.py -v`
Expected: PASS — new tests + all existing tests in the file.

- [ ] **Step 7: Run mypy strict on the persistence module**

Run: `mypy --strict sentinel/persistence/`
Expected: no errors.

- [ ] **Step 8: Commit**

```bash
git add sentinel/persistence/models.py tests/unit/persistence/test_models_metadata.py
git commit -m "feat(persistence): EvalRunModel production shape + EvalCaseResultModel with incident snapshot"
```

---

## Task 4: Add the EvalRunNotFoundOrAlreadyFinalized exception

**Files:**
- Modify: `sentinel/persistence/errors.py`

- [ ] **Step 1: Append the new exception**

```python
# sentinel/persistence/errors.py — append at end

class EvalRunNotFoundOrAlreadyFinalized(Exception):
    """Raised by EvalRunRepository.finalize_run when the UPDATE matched zero rows.

    Means either the run_id is wrong (programming bug) or the run was already
    finalized (status != 'running' — concurrent finalize, also a bug in a
    single-process runner). Surface to caller; do not swallow silently.
    """

    def __init__(self, run_id: UUID) -> None:
        super().__init__(f"eval run not found or already finalized: {run_id}")
        self.run_id = run_id
```

- [ ] **Step 2: Commit**

```bash
git add sentinel/persistence/errors.py
git commit -m "feat(persistence): EvalRunNotFoundOrAlreadyFinalized exception"
```

---

## Task 5: Add value-object dataclasses

**Files:**
- Modify: `sentinel/persistence/repositories.py` (add dataclasses near other value-objects, around lines 80-130)
- Create: `tests/unit/persistence/test_eval_run_repository.py`

Repository methods return frozen dataclasses, not raw ORM instances. Convention: `@dataclass(frozen=True, slots=True)` (matches `OutboxEvent`, `DeployRow`, `MemoryIncidentRow` — see `repositories.py:82+`). Note: `dataclass` is imported (`from dataclasses import dataclass` at line 15), not the `dataclasses` module — do NOT write `@dataclasses.dataclass`.

- [ ] **Step 1: Write failing unit test**

```python
# tests/unit/persistence/test_eval_run_repository.py
"""Unit tests for EvalRunRepository value objects + Protocol shape."""

from __future__ import annotations

import dataclasses
from uuid import uuid4

import pytest


def test_eval_run_record_is_frozen() -> None:
    from sentinel.persistence.repositories import EvalRunRecord

    rec = EvalRunRecord(
        id=uuid4(),
        status="ok",
        trigger="local",
        git_sha="abc123",
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-large-en-v1.5",
        corpus_version="1",
        corpus_size=10,
        shots_per_case=3,
        fetcher_fixture_hash="deadbeef",
        metrics={"category_match": 0.9},
        metrics_stability={"category_match": 0.0},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail=None,
        extra={},
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.status = "failed"  # type: ignore[misc]


def test_eval_case_result_record_is_frozen_and_carries_incident_snapshot() -> None:
    from sentinel.persistence.repositories import EvalCaseResultRecord

    rec = EvalCaseResultRecord(
        run_id=uuid4(),
        case_id="cloudflare-2022-06-21-bgp",
        shot_index=0,
        case_status="ok",
        metrics={"category_match": 1.0},
        raw_response={"id": "msg_x", "content": []},
        diagnosis={"hypothesis": "BGP misconfig"},
        incident_id=uuid4(),
        incident_fingerprint="fp-abc",
        incident_title="Elevated 5xx errors",
        incident_severity="SEV1",
        token_usage={"input": 1200, "output": 800},
        latency_ms=2400,
        error_detail=None,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        rec.case_status = "timeout"  # type: ignore[misc]
    assert rec.incident_fingerprint == "fp-abc"
    assert rec.incident_title == "Elevated 5xx errors"


def test_repository_protocol_exposes_expected_methods() -> None:
    from sentinel.persistence.repositories import EvalRunRepository

    proto_attrs = set(dir(EvalRunRepository))
    expected = {
        "start_run",
        "persist_shot",
        "finalize_run",
        "get_run",
        "get_latest_ok_run",
        "list_recent",
    }
    assert expected <= proto_attrs, f"missing methods: {expected - proto_attrs}"
```

- [ ] **Step 2: Run — confirm it fails**

Run: `pytest tests/unit/persistence/test_eval_run_repository.py -v`
Expected: FAIL — ImportError on `EvalRunRecord` / `EvalCaseResultRecord`.

- [ ] **Step 3: Add dataclasses to `repositories.py` (near other value-objects at lines 80-130)**

```python
# sentinel/persistence/repositories.py — add near OutboxEvent / DeployRow / MemoryIncidentRow

@dataclass(frozen=True, slots=True)
class EvalRunRecord:
    """A persisted eval run as exposed to callers (immutable snapshot).

    Note: status/trigger fields are Literal-typed for mypy, but the CHECK
    constraint on the table is the runtime authority. Reading a row with a
    value outside the literal set indicates the DB schema and code drifted.
    """

    id: UUID
    status: Literal["running", "ok", "failed", "partial"]
    trigger: Literal["local", "ci-smoke", "ci-nightly", "baseline", "manual"]
    git_sha: str
    model: str
    prompt_version: str
    embedding_model_id: str
    corpus_version: str
    corpus_size: int
    shots_per_case: int
    fetcher_fixture_hash: str
    metrics: dict[str, float]
    metrics_stability: dict[str, float]
    regression_baseline_sha: str | None
    regression_passed: bool | None
    regression_detail: dict[str, Any] | None
    extra: dict[str, Any]


@dataclass(frozen=True, slots=True)
class EvalCaseResultRecord:
    """A persisted per-shot case result as exposed to callers (immutable snapshot).

    incident_fingerprint/title/severity are denormalised from the incident at
    write time so results remain self-contained after the runner truncates
    the incidents table between cases.
    """

    run_id: UUID
    case_id: str
    shot_index: int
    case_status: Literal["ok", "timeout", "ingest_failed", "schema_failed", "rate_limited"]
    metrics: dict[str, float]
    raw_response: dict[str, Any] | None
    diagnosis: dict[str, Any] | None
    incident_id: UUID | None
    incident_fingerprint: str | None
    incident_title: str | None
    incident_severity: str | None
    token_usage: dict[str, Any] | None
    latency_ms: int | None
    error_detail: str | None
```

- [ ] **Step 4: Re-run — dataclass tests pass; Protocol test still fails**

Run: `pytest tests/unit/persistence/test_eval_run_repository.py -v`
Expected: `test_eval_run_record_is_frozen` + `test_eval_case_result_record_is_frozen_and_carries_incident_snapshot` PASS. `test_repository_protocol_exposes_expected_methods` FAILs (Protocol not yet revised).

- [ ] **Step 5: Commit**

```bash
git add sentinel/persistence/repositories.py tests/unit/persistence/test_eval_run_repository.py
git commit -m "feat(persistence): EvalRunRecord + EvalCaseResultRecord value objects"
```

---

## Task 6: Replace the EvalRunRepository Protocol stub

**Files:**
- Modify: `sentinel/persistence/repositories.py` (replace lines ~975-979)

- [ ] **Step 1: Replace the stub**

Find:

```python
class EvalRunRepository(Protocol):
    async def start(self, *, model: str, prompt_version: str, corpus_version: str) -> UUID: ...

    async def complete(self, run_id: UUID, summary: dict[str, float]) -> None: ...
```

Replace with the production Protocol. Each method body is a docstring followed by `...` to match existing Protocol style (see `OutboxRepository`):

```python
class EvalRunRepository(Protocol):
    """Persistence interface for eval harness runs and per-shot case results.

    Used by sentinel.evals.runner. Two-table model:
      - eval_runs: one row per run; metadata + aggregated metrics + regression result
      - eval_case_results: one row per (run, case, shot); per-shot metrics + raw LLM output

    All methods are async (asyncpg under the hood). Methods that mutate state
    use a single session.begin() block; partial failures roll back.
    """

    async def start_run(
        self,
        *,
        status: Literal["running"],
        trigger: Literal["local", "ci-smoke", "ci-nightly", "baseline", "manual"],
        git_sha: str,
        model: str,
        prompt_version: str,
        embedding_model_id: str,
        corpus_version: str,
        corpus_size: int,
        shots_per_case: int,
        fetcher_fixture_hash: str,
        extra: dict[str, Any] | None = None,
    ) -> UUID:
        """Insert a new run row (status='running'). Returns the new run_id."""
        ...

    async def persist_shot(self, shot: EvalCaseResultRecord) -> None:
        """Insert a single (run, case, shot) row including the denormalised incident
        snapshot from `shot`. Idempotent on the UNIQUE (run_id, case_id, shot_index)
        constraint — re-persisting a shot raises IntegrityError; callers should
        treat that as a programming bug, not a retry signal."""
        ...

    async def finalize_run(
        self,
        run_id: UUID,
        *,
        status: Literal["ok", "failed", "partial"],
        metrics: dict[str, float],
        metrics_stability: dict[str, float],
        regression_baseline_sha: str | None,
        regression_passed: bool | None,
        regression_detail: dict[str, Any] | None,
    ) -> None:
        """Update the run row: set completed_at=now(), terminal status, final
        metrics + regression verdict. Raises EvalRunNotFoundOrAlreadyFinalized
        if zero rows match (status != 'running' or run_id absent) — that's a
        programming bug, not a retry signal. Idempotency belongs at the caller's
        retry boundary, not silently here."""
        ...

    async def get_run(self, run_id: UUID) -> EvalRunRecord | None:
        """Fetch a run by id, or None if absent."""
        ...

    async def get_latest_ok_run(
        self, *, trigger: Literal["baseline", "ci-nightly"] | None = None
    ) -> EvalRunRecord | None:
        """Most recent status='ok' run, optionally filtered by trigger.
        Used by `make readme-numbers` (trigger='baseline') and the regression
        gate (no filter). Returns None if no matching run exists."""
        ...

    async def list_recent(self, *, limit: int = 50) -> list[EvalRunRecord]:
        """Recent runs by started_at DESC. Pagination via limit only — eval
        volume is low enough that offset-style paging is unnecessary."""
        ...
```

- [ ] **Step 2: Re-run the Protocol contract test**

Run: `pytest tests/unit/persistence/test_eval_run_repository.py -v`
Expected: all three tests PASS.

- [ ] **Step 3: Confirm no consumer broke**

Run: `grep -rn "EvalRunRepository\|\.start(\|\.complete(" sentinel/ tests/ | grep -i eval`
Expected: no production references to the old `.start()` / `.complete()` shape — only the Protocol definition itself and the new test.

- [ ] **Step 4: Commit**

```bash
git add sentinel/persistence/repositories.py
git commit -m "feat(persistence): replace EvalRunRepository stub with production Protocol"
```

---

## Task 7: Implement PostgresEvalRunRepository

**Files:**
- Modify: `sentinel/persistence/repositories.py`
  - Top-level imports (~lines 24-29): add `EvalCaseResultModel`, `EvalRunModel`
  - Append concrete `PostgresEvalRunRepository` near other Postgres* implementations

- [ ] **Step 1: Add top-level model imports**

Locate the model-import block at lines ~24-29 (currently imports `DiagnosisModel`, `IncidentModel`, `OutboxEventModel`, `DeployModel`, etc.). Add `EvalCaseResultModel` and `EvalRunModel` in alphabetical order. After the edit the block should look like:

```python
from sentinel.persistence.models import (
    DeployModel,
    DiagnosisModel,
    EvalCaseResultModel,
    EvalRunModel,
    IncidentModel,
    # ... existing entries continue in alphabetical order
)
```

(`update` is already imported at `repositories.py:20` — no additional sqlalchemy import needed.)

- [ ] **Step 2: Add the concrete implementation**

Append after `PostgresResolutionRepository` (~line 1102):

```python
# sentinel/persistence/repositories.py — append after PostgresResolutionRepository

class PostgresEvalRunRepository:
    """Concrete EvalRunRepository over asyncpg.

    All writes use a single session.begin() block so partial failures roll back.
    No outbox involvement — eval data is internal-only and not published to Kafka.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def start_run(
        self,
        *,
        status: Literal["running"],
        trigger: Literal["local", "ci-smoke", "ci-nightly", "baseline", "manual"],
        git_sha: str,
        model: str,
        prompt_version: str,
        embedding_model_id: str,
        corpus_version: str,
        corpus_size: int,
        shots_per_case: int,
        fetcher_fixture_hash: str,
        extra: dict[str, Any] | None = None,
    ) -> UUID:
        async with self._session_factory() as session, session.begin():
            row = EvalRunModel(
                status=status,
                trigger=trigger,
                git_sha=git_sha,
                model=model,
                prompt_version=prompt_version,
                embedding_model_id=embedding_model_id,
                corpus_version=corpus_version,
                corpus_size=corpus_size,
                shots_per_case=shots_per_case,
                fetcher_fixture_hash=fetcher_fixture_hash,
                extra=extra or {},
            )
            session.add(row)
            # flush() emits the INSERT and populates row.id from the server default
            # (SQLAlchemy 2.0 uses RETURNING for server-default UUIDs on PG).
            await session.flush()
            return row.id

    async def persist_shot(self, shot: EvalCaseResultRecord) -> None:
        async with self._session_factory() as session, session.begin():
            session.add(
                EvalCaseResultModel(
                    run_id=shot.run_id,
                    case_id=shot.case_id,
                    shot_index=shot.shot_index,
                    case_status=shot.case_status,
                    metrics=shot.metrics,
                    raw_response=shot.raw_response,
                    diagnosis=shot.diagnosis,
                    incident_id=shot.incident_id,
                    incident_fingerprint=shot.incident_fingerprint,
                    incident_title=shot.incident_title,
                    incident_severity=shot.incident_severity,
                    token_usage=shot.token_usage,
                    latency_ms=shot.latency_ms,
                    error_detail=shot.error_detail,
                )
            )

    async def finalize_run(
        self,
        run_id: UUID,
        *,
        status: Literal["ok", "failed", "partial"],
        metrics: dict[str, float],
        metrics_stability: dict[str, float],
        regression_baseline_sha: str | None,
        regression_passed: bool | None,
        regression_detail: dict[str, Any] | None,
    ) -> None:
        from sentinel.persistence.errors import EvalRunNotFoundOrAlreadyFinalized

        async with self._session_factory() as session, session.begin():
            result = await session.execute(
                update(EvalRunModel)
                .where(
                    EvalRunModel.id == run_id,
                    EvalRunModel.status == "running",
                )
                .values(
                    completed_at=func.now(),
                    status=status,
                    metrics=metrics,
                    metrics_stability=metrics_stability,
                    regression_baseline_sha=regression_baseline_sha,
                    regression_passed=regression_passed,
                    regression_detail=regression_detail,
                )
            )
            if result.rowcount == 0:
                raise EvalRunNotFoundOrAlreadyFinalized(run_id)

    async def get_run(self, run_id: UUID) -> EvalRunRecord | None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(EvalRunModel).where(EvalRunModel.id == run_id)
            )
            row = result.scalar_one_or_none()
            return _eval_run_record_from_model(row) if row else None

    async def get_latest_ok_run(
        self, *, trigger: Literal["baseline", "ci-nightly"] | None = None
    ) -> EvalRunRecord | None:
        async with self._session_factory() as session:
            stmt = (
                select(EvalRunModel)
                .where(EvalRunModel.status == "ok")
                .order_by(EvalRunModel.started_at.desc())
                .limit(1)
            )
            if trigger is not None:
                stmt = stmt.where(EvalRunModel.trigger == trigger)
            result = await session.execute(stmt)
            row = result.scalar_one_or_none()
            return _eval_run_record_from_model(row) if row else None

    async def list_recent(self, *, limit: int = 50) -> list[EvalRunRecord]:
        async with self._session_factory() as session:
            result = await session.execute(
                select(EvalRunModel)
                .order_by(EvalRunModel.started_at.desc())
                .limit(limit)
            )
            return [_eval_run_record_from_model(row) for row in result.scalars().all()]


def _eval_run_record_from_model(row: EvalRunModel) -> EvalRunRecord:
    """ORM → frozen dataclass mapping. Keeps the repository's return type stable
    even if the ORM grows additional columns. CHECK constraints enforce the
    Literal narrowing on `status`/`trigger`; the type: ignore[arg-type] comments
    document that the DB schema is the runtime authority for those enums."""
    return EvalRunRecord(
        id=row.id,
        status=row.status,  # type: ignore[arg-type]  # ck_eval_runs_status_valid enforces
        trigger=row.trigger,  # type: ignore[arg-type]  # ck_eval_runs_trigger_valid enforces
        git_sha=row.git_sha,
        model=row.model,
        prompt_version=row.prompt_version,
        embedding_model_id=row.embedding_model_id,
        corpus_version=row.corpus_version,
        corpus_size=row.corpus_size,
        shots_per_case=row.shots_per_case,
        fetcher_fixture_hash=row.fetcher_fixture_hash,
        metrics=dict(row.metrics) if row.metrics else {},
        metrics_stability=dict(row.metrics_stability) if row.metrics_stability else {},
        regression_baseline_sha=row.regression_baseline_sha,
        regression_passed=row.regression_passed,
        regression_detail=dict(row.regression_detail) if row.regression_detail else None,
        extra=dict(row.extra) if row.extra else {},
    )
```

- [ ] **Step 3: Update `__all__` exports**

Find the `__all__` list (~line 1108) and add in alphabetical order:

```python
"EvalCaseResultRecord",
"EvalRunRecord",
"PostgresEvalRunRepository",
```

- [ ] **Step 4: Run mypy strict on the persistence module**

Run: `mypy --strict sentinel/persistence/`
Expected: no errors.

- [ ] **Step 5: Run ruff format + check**

Run: `ruff format sentinel/persistence/repositories.py && ruff check sentinel/persistence/repositories.py`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add sentinel/persistence/repositories.py
git commit -m "feat(persistence): PostgresEvalRunRepository concrete implementation"
```

---

## Task 8: Add per-test truncate fixture for eval tables

**Files:**
- Modify: `tests/integration/persistence/conftest.py`

The `pg_container` fixture is session-scoped — data from prior tests persists across the test session. Without a truncate fixture, `test_eval_run_repository_e2e.py` and `test_diagnosis_repository_e2e.py` can interfere across runs. Truncating only the eval tables (and only when at the migration head) keeps the scope tight.

- [ ] **Step 1: Add the autouse fixture**

```python
# tests/integration/persistence/conftest.py — append

import asyncio as _asyncio

from sqlalchemy import text as _text
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine


@pytest.fixture(autouse=True)
def _truncate_eval_tables_between_tests(pg_dsn: str) -> Iterator[None]:
    """Function-scoped: truncate eval_runs + eval_case_results before each test.

    Skips silently if the tables don't exist yet (migration tests transition
    the DB across these states). CASCADE handles the FK from eval_case_results.
    """

    async def _truncate() -> None:
        engine = _create_async_engine(pg_dsn)
        try:
            async with engine.begin() as conn:
                # Check table existence; skip if either is missing (migration tests).
                existing = (
                    await conn.execute(
                        _text(
                            "SELECT table_name FROM information_schema.tables "
                            "WHERE table_name IN ('eval_runs','eval_case_results')"
                        )
                    )
                ).scalars().all()
                if "eval_runs" in existing and "eval_case_results" in existing:
                    await conn.execute(
                        _text(
                            "TRUNCATE eval_case_results, eval_runs RESTART IDENTITY CASCADE"
                        )
                    )
        finally:
            await engine.dispose()

    _asyncio.run(_truncate())
    yield
```

(If the existing conftest doesn't already import `pytest` or `Iterator`, add them — `pytest` at module level, `from collections.abc import Iterator` near other imports.)

- [ ] **Step 2: Quick smoke — does the fixture run without error against current head?**

Run: `pytest tests/integration/persistence/test_migration_0006.py::test_upgrade_produces_full_eval_schema -v -m integration`
Expected: PASS (the fixture skips silently before the migration upgrades, then runs after).

- [ ] **Step 3: Commit**

```bash
git add tests/integration/persistence/conftest.py
git commit -m "test(persistence): autouse fixture — truncate eval tables between tests"
```

---

## Task 9: Integration test — full repository round-trip + finalize-raises + duplicate-shot

**Files:**
- Create: `tests/integration/persistence/test_eval_run_repository_e2e.py`

- [ ] **Step 1: Write the test file**

```python
# tests/integration/persistence/test_eval_run_repository_e2e.py
"""End-to-end repository tests against real Postgres (testcontainers).

Covers:
- Happy-path lifecycle (start → persist shots → finalize → reads)
- finalize_run raises EvalRunNotFoundOrAlreadyFinalized on double-finalize
- finalize_run raises on unknown run_id
- persist_shot raises IntegrityError on duplicate (run, case, shot)
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sentinel.persistence.errors import EvalRunNotFoundOrAlreadyFinalized
from sentinel.persistence.repositories import (
    EvalCaseResultRecord,
    PostgresEvalRunRepository,
)

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def test_full_run_lifecycle(pg_dsn: str) -> None:
    """start_run → persist_shot ×3 → finalize_run → get_run round-trip."""

    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            run_id = await repo.start_run(
                status="running",
                trigger="local",
                git_sha="abc1234",
                model="claude-sonnet-4-5-20250929",
                prompt_version="v1",
                embedding_model_id="BAAI/bge-large-en-v1.5",
                corpus_version="1",
                corpus_size=10,
                shots_per_case=3,
                fetcher_fixture_hash="deadbeef",
            )
            assert run_id is not None

            for i in range(3):
                await repo.persist_shot(
                    EvalCaseResultRecord(
                        run_id=run_id,
                        case_id="cloudflare-bgp",
                        shot_index=i,
                        case_status="ok",
                        metrics={"category_match": 1.0, "evidence_quality": 0.9},
                        raw_response={"id": f"msg_{i}", "content": []},
                        diagnosis={"hypothesis": "BGP misconfig", "confidence": 0.85},
                        incident_id=uuid4(),
                        incident_fingerprint="fp-bgp",
                        incident_title="Elevated 5xx errors across multiple POPs",
                        incident_severity="SEV1",
                        token_usage={"input": 1200, "output": 800},
                        latency_ms=2400 + i,
                        error_detail=None,
                    )
                )

            await repo.finalize_run(
                run_id,
                status="ok",
                metrics={"category_match": 1.0, "evidence_quality": 0.9},
                metrics_stability={"category_match": 0.0, "evidence_quality": 0.05},
                regression_baseline_sha=None,
                regression_passed=None,
                regression_detail=None,
            )

            row = await repo.get_run(run_id)
            assert row is not None
            assert row.status == "ok"
            assert row.metrics["category_match"] == 1.0
            assert row.metrics_stability["evidence_quality"] == 0.05

            latest = await repo.get_latest_ok_run()
            assert latest is not None
            assert latest.id == run_id

            latest_baseline = await repo.get_latest_ok_run(trigger="baseline")
            assert latest_baseline is None  # this run had trigger='local'

            recents = await repo.list_recent(limit=10)
            assert any(r.id == run_id for r in recents)
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())


def test_finalize_run_raises_on_double_finalize(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            run_id = await repo.start_run(
                status="running",
                trigger="local",
                git_sha="x",
                model="m",
                prompt_version="v1",
                embedding_model_id="e",
                corpus_version="1",
                corpus_size=1,
                shots_per_case=1,
                fetcher_fixture_hash="h",
            )

            # First finalize succeeds
            await repo.finalize_run(
                run_id,
                status="ok",
                metrics={"x": 1.0},
                metrics_stability={"x": 0.0},
                regression_baseline_sha=None,
                regression_passed=None,
                regression_detail=None,
            )

            # Second finalize raises (status is already terminal)
            with pytest.raises(EvalRunNotFoundOrAlreadyFinalized) as exc:
                await repo.finalize_run(
                    run_id,
                    status="failed",
                    metrics={"x": 0.5},
                    metrics_stability={"x": 0.0},
                    regression_baseline_sha=None,
                    regression_passed=None,
                    regression_detail=None,
                )
            assert exc.value.run_id == run_id

            # First-finalize state preserved
            row = await repo.get_run(run_id)
            assert row is not None and row.status == "ok"
            assert row.metrics["x"] == 1.0
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())


def test_finalize_run_raises_on_unknown_run_id(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            bogus = uuid4()
            with pytest.raises(EvalRunNotFoundOrAlreadyFinalized) as exc:
                await repo.finalize_run(
                    bogus,
                    status="ok",
                    metrics={},
                    metrics_stability={},
                    regression_baseline_sha=None,
                    regression_passed=None,
                    regression_detail=None,
                )
            assert exc.value.run_id == bogus
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())


def test_persist_shot_duplicate_raises_integrity_error(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            repo = PostgresEvalRunRepository(session_factory)

            run_id = await repo.start_run(
                status="running",
                trigger="local",
                git_sha="x",
                model="m",
                prompt_version="v1",
                embedding_model_id="e",
                corpus_version="1",
                corpus_size=1,
                shots_per_case=1,
                fetcher_fixture_hash="h",
            )

            shot = EvalCaseResultRecord(
                run_id=run_id,
                case_id="case-a",
                shot_index=0,
                case_status="ok",
                metrics={},
                raw_response=None,
                diagnosis=None,
                incident_id=None,
                incident_fingerprint=None,
                incident_title=None,
                incident_severity=None,
                token_usage=None,
                latency_ms=None,
                error_detail=None,
            )
            await repo.persist_shot(shot)
            with pytest.raises(IntegrityError):
                await repo.persist_shot(shot)
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())
```

- [ ] **Step 2: Run all four tests**

Run: `pytest tests/integration/persistence/test_eval_run_repository_e2e.py -v -m integration`
Expected: all four PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/persistence/test_eval_run_repository_e2e.py
git commit -m "test(persistence): e2e — PostgresEvalRunRepository lifecycle + finalize-raises + duplicate"
```

---

## Task 10: Add DiagnosisRepository.get_by_incident_id()

**Files:**
- Modify: `sentinel/persistence/repositories.py` (extend `DiagnosisRepository` Protocol ~line 948 + `PostgresDiagnosisRepository` concrete)
- Create: `tests/unit/persistence/test_diagnosis_repository.py`
- Create: `tests/integration/persistence/test_diagnosis_repository_e2e.py`

- [ ] **Step 1: Write failing unit test (Protocol shape)**

```python
# tests/unit/persistence/test_diagnosis_repository.py
"""Unit tests for DiagnosisRepository surface (Protocol shape)."""

from __future__ import annotations


def test_diagnosis_repository_exposes_get_by_incident_id() -> None:
    from sentinel.persistence.repositories import DiagnosisRepository

    assert "get_by_incident_id" in dir(DiagnosisRepository), (
        "DiagnosisRepository.get_by_incident_id is required by the eval runner"
    )
```

- [ ] **Step 2: Run — confirm it fails**

Run: `pytest tests/unit/persistence/test_diagnosis_repository.py -v`
Expected: FAIL — assertion error.

- [ ] **Step 3: Extend the `DiagnosisRepository` Protocol**

Find (~line 948):

```python
class DiagnosisRepository(Protocol):
    async def save_with_outbox(
        self,
        *,
        incident_id: UUID,
        record: PersistedDiagnosis,
        upstream_event_id: UUID,
        outbox_event: OutboxEvent,
    ) -> tuple[UUID, Literal["new", "duplicate"]]: ...
```

Add a new method below `save_with_outbox`:

```python
    async def get_by_incident_id(self, incident_id: UUID) -> PersistedDiagnosis | None:
        """Most recent diagnosis for an incident (by created_at DESC), or None.

        Used by the eval harness runner to poll for the diagnosis after firing
        a synthetic webhook. The UNIQUE constraint on (incident_id, prompt_version,
        model) means multiple diagnoses per incident only happen across prompt
        or model changes — the runner pins both, so 'most recent' is deterministic
        within a single run.
        """
        ...
```

- [ ] **Step 4: Extend `PostgresDiagnosisRepository` with the concrete implementation**

In the `PostgresDiagnosisRepository` class (~line 981), append after `save_with_outbox`:

```python
    async def get_by_incident_id(self, incident_id: UUID) -> PersistedDiagnosis | None:
        # Local imports because EvidenceRef/SuggestedAction aren't needed by any
        # other method in this class; keeps top-of-file imports lean.
        from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction

        async with self._session_factory() as session:
            result = await session.execute(
                select(DiagnosisModel)
                .where(DiagnosisModel.incident_id == incident_id)
                .order_by(DiagnosisModel.created_at.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return PersistedDiagnosis(
                hypothesis=row.hypothesis,
                # row.confidence is already Decimal (Numeric(3,2)) — matches
                # PersistedDiagnosis.confidence: Decimal. Do NOT cast to float;
                # mypy --strict would reject and Decimal precision would be lost.
                confidence=row.confidence,
                reasoning=row.reasoning,
                evidence=[EvidenceRef.model_validate(e) for e in row.evidence],
                suggested_actions=[
                    SuggestedAction.model_validate(a) for a in row.suggested_actions
                ],
                likely_category=row.likely_category,  # type: ignore[arg-type]  # CHECK enforces
                hallucinated_evidence=row.hallucinated_evidence,
                model=row.model,
                prompt_version=row.prompt_version,
                latency_ms=row.latency_ms,
                token_usage=row.token_usage,
            )
```

(Verify `PersistedDiagnosis` is imported near the top of `repositories.py` — confirmed at `repositories.py:23` from `sentinel.diagnosis.persisted`. No new import needed.)

- [ ] **Step 5: Re-run the unit test**

Run: `pytest tests/unit/persistence/test_diagnosis_repository.py -v`
Expected: PASS.

- [ ] **Step 6: Write the integration test**

```python
# tests/integration/persistence/test_diagnosis_repository_e2e.py
"""End-to-end: save a diagnosis via save_with_outbox, read via get_by_incident_id."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from sentinel.diagnosis.persisted import PersistedDiagnosis
from sentinel.persistence.repositories import (
    OutboxEvent,
    PostgresDiagnosisRepository,
    PostgresIncidentRepository,
)
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.diagnosis import EvidenceRef, SuggestedAction

pytestmark = pytest.mark.integration


def _alembic_cfg(dsn: str) -> Config:
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def _build_alert() -> NormalizedAlert:
    # Field names verified against sentinel/schemas/alert.py:14-32.
    # NormalizedAlert has model_config = ConfigDict(frozen=True, extra="forbid"),
    # so passing unknown kwargs (e.g. fingerprint, opened_at) raises.
    return NormalizedAlert(
        source="generic",
        external_id="ext-eval-1",
        service="svc",
        severity="SEV2",
        title="test alert",
        received_at=datetime.now(UTC),
        raw_payload={},
    )


def test_get_by_incident_id_returns_most_recent(pg_dsn: str) -> None:
    async def _run() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            incidents = PostgresIncidentRepository(session_factory)
            diagnoses = PostgresDiagnosisRepository(session_factory)

            alert = _build_alert()
            incident_id = await incidents.create_from_alert(alert, fingerprint="fp-eval-1")

            diag = PersistedDiagnosis(
                hypothesis="cause X",
                confidence=Decimal("0.80"),
                reasoning="because Y",
                evidence=[EvidenceRef(kind="deploy", id="deploy:abc", note="recent")],
                suggested_actions=[
                    SuggestedAction(
                        description="rollback",
                        command=None,
                        risk="low",
                        rationale="safe",
                        requires_human_approval=True,
                    )
                ],
                likely_category="deploy",
                hallucinated_evidence=False,
                model="claude-sonnet-4-5",
                prompt_version="v1",
                latency_ms=1500,
                token_usage={"input": 100, "output": 50},
            )
            outbox_event = OutboxEvent(
                id=uuid4(),
                topic="diagnoses",
                key=str(incident_id),
                payload={"incident_id": str(incident_id)},
                attempts=0,
                created_at=datetime.now(UTC),
            )
            await diagnoses.save_with_outbox(
                incident_id=incident_id,
                record=diag,
                upstream_event_id=uuid4(),
                outbox_event=outbox_event,
            )

            got = await diagnoses.get_by_incident_id(incident_id)
            assert got is not None
            assert got.hypothesis == "cause X"
            assert got.confidence == Decimal("0.80")
            assert len(got.evidence) == 1
            assert got.evidence[0].id == "deploy:abc"

            assert await diagnoses.get_by_incident_id(uuid4()) is None
        finally:
            await engine.dispose()

    cfg = _alembic_cfg(pg_dsn)
    command.upgrade(cfg, "head")
    asyncio.run(_run())
```

If `tests/unit/persistence/test_diagnosis_repository.py` (the existing one mentioned by the verification report) has an `_alert()` helper, mirror its NormalizedAlert construction to stay consistent.

- [ ] **Step 7: Run the integration test**

Run: `pytest tests/integration/persistence/test_diagnosis_repository_e2e.py -v -m integration`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add sentinel/persistence/repositories.py tests/unit/persistence/test_diagnosis_repository.py tests/integration/persistence/test_diagnosis_repository_e2e.py
git commit -m "feat(persistence): DiagnosisRepository.get_by_incident_id (needed by eval runner)"
```

---

## Task 11: Update module exports

**Files:**
- Modify: `sentinel/persistence/__init__.py`

- [ ] **Step 1: Read the current `__init__.py`**

Run: `cat sentinel/persistence/__init__.py`

- [ ] **Step 2: Extend the imports + `__all__`**

Add to the `from sentinel.persistence.models import (...)` block (alphabetical):

```python
    EvalCaseResultModel,
```

Add to the `from sentinel.persistence.repositories import (...)` block (alphabetical):

```python
    EvalCaseResultRecord,
    EvalRunRecord,
    PostgresEvalRunRepository,
```

Add to the `from sentinel.persistence.errors import (...)` block (or create one if it doesn't exist):

```python
    EvalRunNotFoundOrAlreadyFinalized,
```

Add to `__all__` (alphabetical):

```python
"EvalCaseResultModel",
"EvalCaseResultRecord",
"EvalRunNotFoundOrAlreadyFinalized",
"EvalRunRecord",
"PostgresEvalRunRepository",
```

- [ ] **Step 3: Verify imports work**

Run: `python -c "from sentinel.persistence import EvalCaseResultModel, EvalCaseResultRecord, EvalRunNotFoundOrAlreadyFinalized, EvalRunRecord, PostgresEvalRunRepository; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Run full unit test suite for persistence**

Run: `pytest tests/unit/persistence/ -v`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add sentinel/persistence/__init__.py
git commit -m "feat(persistence): export eval-run + diagnosis-read additions"
```

---

## Task 12: Full lint + typecheck + test sweep

- [ ] **Step 1: Lint**

Run: `make lint`
Expected: no errors.

- [ ] **Step 2: Typecheck**

Run: `make typecheck`
Expected: no errors. If mypy complains about `Literal` narrowing on CHECK-constrained columns, the `# type: ignore[arg-type]` comments in `_eval_run_record_from_model` should suffice; if a fresh complaint surfaces, add a similar comment with a pointer to the enforcing constraint.

- [ ] **Step 3: Run full unit suite**

Run: `make test`
Expected: all green; coverage at or above current main.

- [ ] **Step 4: Run integration suite**

Run: `make compose-up && make test-integration`
Expected: all green, including the new integration tests in `tests/integration/persistence/`.

- [ ] **Step 5: If anything is red, fix it**

Never commit while CI is red. The PR only touches eval-* tables and the diagnosis read method, so collateral damage in other suites is unexpected — investigate before patching.

---

## Task 13: Mandatory code review (per project memory)

Per `MEMORY.md` → `sentinel-review-before-commit.md`: mandatory subagent code review before any push.

- [ ] **Step 1: Invoke `superpowers:requesting-code-review`**

Brief the reviewer on:
- Scope: PR 1 of Work Area K (eval harness schema + repository plumbing)
- Spec: `plans/2026-05-20-eval-harness-design.md` §6, §8 PR 1
- Plan: `plans/2026-05-20-eval-harness-pr1-schema-plan.md`
- Specific concerns to validate:
  - Migration 0006 round-trip is clean against a populated DB (the placeholder eval_runs has no data anywhere, but verify no data-loss surprises)
  - `PostgresEvalRunRepository.finalize_run` raises on zero rows — exception type + message useful to operators?
  - `DiagnosisRepository.get_by_incident_id` returns the most recent diagnosis when multiple exist (only possible across prompt/model changes due to UNIQUE constraint)
  - Denormalized `incident_*` columns on `eval_case_results` — naming clear? all callers will set them at write time (no orphan paths that forget)?
  - No raw SQL slipped into non-persistence modules (the `_sql_guard` test should already enforce this)

- [ ] **Step 2: Address review feedback**

Per `superpowers:receiving-code-review`: technical rigor, push back on suggestions that conflict with spec rationale, fix what's right.

- [ ] **Step 3: Re-run the full sweep after review changes**

Run: `make lint typecheck test`
Run: `make compose-up && make test-integration`
Expected: all green.

---

## Task 14: Push + open PR

Per `MEMORY.md` → `feedback-commits-by-user.md`: each task in this plan commits its own slice; final step is push + PR.

- [ ] **Step 1: Verify clean working tree**

Run: `git status`
Expected: no uncommitted changes.

- [ ] **Step 2: Verify the commit log shows the expected slice**

Run: `git log --oneline -15`
Expected: ~12 commits prefixed `feat(persistence):` / `test(persistence):` covering branch creation, migration, model, errors, value objects, Protocol, concrete repo, truncate fixture, integration tests, diagnosis read method, and exports.

- [ ] **Step 3: Push to the feature branch**

```bash
git push -u origin feat/eval-harness-pr1-schema
```

- [ ] **Step 4: Open the PR**

Use `gh pr create` per `CLAUDE.md` instructions. Title: `feat(persistence): eval_runs production shape + eval_case_results + diagnosis read path (PR 1 of Work Area K)`.

Body should:
- Link `plans/2026-05-20-eval-harness-design.md` and this plan
- Note this is PR 1 of 4 in Work Area K — runner/scoring/CI follow
- Highlight: migration 0006 drops + recreates `eval_runs` (placeholder is empty everywhere — no data loss); adds `eval_case_results` with denormalised incident snapshot so results survive cross-case truncation
- Confirm: `make lint typecheck test test-integration` all green locally
