# Phase 4 — Memory & Feedback Loop (Work Area H) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Work Area H end-to-end: a local-CPU embedding provider (fastembed + bge-large-en-v1.5, 1024-dim), a pgvector-backed `SimilarIncidentRetrieval` store that replaces `NotConfiguredSimilarIncidents`, a `POST /incidents/{id}/resolve` route that persists resolution + status + outbox event atomically, and a `MemoryConsumer` that subscribes to both `incident.opened` and the new `incident.resolved` events and writes `incidents.embedding` idempotently. The "gets smarter from use" loop comes online with a single wiring change in `app.py`.

**Architecture:** New module `sentinel/memory/` with `embeddings.py` (FastEmbedProvider — lazy ONNX load, thread-pool execution, 5s timeout, no breaker), `store.py` (PgVectorIncidentStore — cosine top-k with the two spec-required filters), `pipeline.py` (pure text composition: `service + title` opened, `title + root_cause + remediation` resolved), `consumer.py` (aiokafka consumer for `sentinel.incidents`, mirroring EnrichmentConsumer/DiagnosisConsumer), `deps.py` (MemoryConsumerDeps dataclass). Persistence gets a new `PostgresResolutionRepository`, two new methods on `IncidentRepository` (`set_embedding`, `load_for_memory`), a new `last_embedding_event_id` column, and a column-dim change `Vector(1536) → Vector(1024)` via migration 0005. New file `sentinel/persistence/errors.py` for `IncidentNotFound`/`IncidentAlreadyResolved`. New route at `sentinel/api/routes/resolve.py`. Lifespan gates the consumer on `memory_consumer_enabled`, mirroring the existing `diagnosis_consumer_enabled` pattern.

**Tech Stack:** Python 3.12, asyncio, Pydantic v2, SQLAlchemy 2.x async, asyncpg, alembic, pgvector, aiokafka, fastembed (new runtime dep — ONNX, no torch), prometheus-client, OpenTelemetry SDK, structlog (config) + stdlib logging (call sites), testcontainers-postgres/-kafka, pytest, pytest-asyncio.

**Conventions for this repo (do not violate):**
- The user performs all `git commit` / `git push` — Claude must not commit. Tasks end with "stage and pause for review", never with `git commit`. One subagent code review (via `superpowers:requesting-code-review`) runs at the end of the whole plan before the user commits.
- `mypy --strict` and `ruff` are gating; both must be clean before review.
- New env vars go into `Settings`, `.env.example`, and `config/dev.yaml` in the same task that introduces them.
- No `dict[str, Any]` on Pydantic API boundaries. JSONB columns may be `dict[str, Any]` internally; that does not leak into wire types.
- Every external call has a documented timeout. The embedding provider is not "external" (in-process ONNX), but still gets a timeout for parity with the invariant.
- ADRs go in `docs/adr/`; two new ADRs in this plan (0005 embedding-on-resolve, 0006 local embeddings).
- Designs and plans go in `plans/`.
- Migrations are reversible — `make migrate-down && make migrate` is round-tripped in CI.

---

## File Structure

### Dependency + schema changes

| File | Responsibility |
|---|---|
| `pyproject.toml` (modify) | Add `fastembed>=0.4,<0.5` runtime dep. |
| `migrations/versions/0005_embedding_dim_1024_and_event_id.py` (new) | Drop existing 1536-dim `embedding` columns + HNSW indexes; add `Vector(1024)` columns + new HNSW indexes; add `incidents.last_embedding_event_id UUID` + partial index. Reversible. |
| `sentinel/persistence/models.py` (modify) | `IncidentModel.embedding`: `Vector(1536)` → `Vector(1024)`. Add `last_embedding_event_id` mapped column. `RunbookModel.embedding`: `Vector(1536)` → `Vector(1024)`. |

### Memory module (Work Area H)

| File | Responsibility |
|---|---|
| `sentinel/memory/__init__.py` | Public exports: `FastEmbedProvider`, `PgVectorIncidentStore`, `MemoryPipeline`, `MemoryConsumer`, `MemoryConsumerDeps`. |
| `sentinel/memory/embeddings.py` | `FastEmbedProvider` — concrete `EmbeddingProvider` impl (Protocol lives in `enrichment/protocols.py`). Lazy ONNX load, asyncio Lock, `run_in_executor`, 5s timeout. |
| `sentinel/memory/pipeline.py` | `MemoryPipeline` with `compose_initial(row) -> str` and `compose_resolved(row, resolution) -> str`. Pure text composition. |
| `sentinel/memory/store.py` | `PgVectorIncidentStore` — implements `SimilarIncidentRetrieval.top_k`. Embeds query, runs cosine top-k SQL with the two spec filters. |
| `sentinel/memory/deps.py` | `MemoryConsumerDeps` dataclass: `incident_repo`, `embedding_provider`, `pipeline`. |
| `sentinel/memory/consumer.py` | `MemoryConsumer` aiokafka consumer for `sentinel.incidents`. Branches on `incident.opened` / `incident.resolved`; manual offset commit; idempotent via `set_embedding`. |

### Persistence extensions

| File | Responsibility |
|---|---|
| `sentinel/persistence/errors.py` (new) | `IncidentNotFound(Exception)`, `IncidentAlreadyResolved(Exception)`. |
| `sentinel/persistence/repositories.py` (modify) | Add `MemoryIncidentRow`, `ResolutionData`, `ResolveRecordResult` dataclasses. Add `IncidentRepository.set_embedding` and `IncidentRepository.load_for_memory` to Protocol + Postgres impl. Update `ResolutionRepository` Protocol signature to take `outbox_topic` kwarg. Add `PostgresResolutionRepository` concrete impl. |

### API surface

| File | Responsibility |
|---|---|
| `sentinel/schemas/api.py` (modify) | Add `ResolveIncidentResponse` model (reuse existing `ResolveIncidentRequest`). |
| `sentinel/api/routes/resolve.py` (new) | `POST /incidents/{incident_id}/resolve` thin route. Maps repo exceptions to 404/409. |

### Observability extension

| File | Responsibility |
|---|---|
| `sentinel/observability/metrics.py` (modify) | Register memory metrics: `sentinel_memory_events_consumed_total{type}`, `_events_failed_total{reason}`, `_duplicates_total`, `_invalid_events_total`, `_embedding_duration_seconds{event_type}`, `sentinel_similar_incidents_query_duration_seconds`, `sentinel_similar_incidents_returned_total`. |

### App wiring + config

| File | Responsibility |
|---|---|
| `sentinel/config/settings.py` (modify) | Add `embedding_model_name`, `embedding_model_cache_dir`, `embedding_compute_timeout_seconds`, `kafka_consumer_group_memory`, `memory_consumer_enabled`. |
| `.env.example` (modify) | Add the five new settings keys with defaults. |
| `config/dev.yaml` (modify) | Add overrides if needed for dev (likely empty — defaults are fine). |
| `sentinel/api/app.py` (modify) | Construct `FastEmbedProvider`, `PgVectorIncidentStore`, `PostgresResolutionRepository`; stash `resolution_repo` + `outbox_topic` on `app.state`; start `MemoryConsumer` task gated on `memory_consumer_enabled`; replace `NotConfiguredSimilarIncidents()` in `enrich_deps`; register `resolve_router`; orderly shutdown. |
| `Dockerfile` (modify) | Pre-download fastembed model at build time. |

### ADRs

| File | Responsibility |
|---|---|
| `docs/adr/0005-embedding-on-resolve.md` (new) | Why embed `title + root_cause + remediation` at resolve time. |
| `docs/adr/0006-local-embeddings.md` (new) | Why fastembed + bge-large-en-v1.5 over remote providers; column dim change rationale. |

### Tests

| File | Responsibility |
|---|---|
| `tests/unit/memory/__init__.py` (new) | Empty marker. |
| `tests/unit/memory/test_embeddings.py` (new) | `FastEmbedProvider`: dim, determinism, lazy load idempotency, timeout. |
| `tests/unit/memory/test_pipeline.py` (new) | `MemoryPipeline.compose_initial` / `compose_resolved` text shape. |
| `tests/unit/memory/test_store.py` (new) | `PgVectorIncidentStore.top_k` SQL shape, exclude_incident_id handling, embed-failure path. (Schema-level only; DB integration in `tests/integration/test_memory.py`.) |
| `tests/unit/memory/test_consumer.py` (new) | `MemoryConsumer.handle_message`: envelope validation, branching, timeouts, idempotency, error paths. |
| `tests/unit/persistence/test_resolution_repo.py` (new) | `PostgresResolutionRepository`: exception paths (uses fake session). Atomicity covered by integration. |
| `tests/unit/api/test_resolve_route.py` (new) | Route maps `IncidentNotFound` → 404, `IncidentAlreadyResolved` → 409, success → 200. |
| `tests/integration/memory/__init__.py` (new) | Empty marker. |
| `tests/integration/memory/conftest.py` (new) | Mirrors `tests/integration/diagnosis/conftest.py`: `pg_container` (session), `pg_dsn`, autouse `_migrate` (alembic upgrade head session-scope), autouse `_reset_state` (TRUNCATE per-test, includes `resolutions` via CASCADE). |
| `tests/integration/memory/test_migration_0005.py` (new) | Round-trip migration: down then up; verify column dims and new column. |
| `tests/integration/memory/test_incident_repo_memory.py` (new) | `IncidentRepository.set_embedding` + `load_for_memory` against real Postgres. |
| `tests/integration/memory/test_store_e2e.py` (new) | `PgVectorIncidentStore.top_k`: status filter, diagnosis_was_correct filter, exclude_incident_id, ID format. |
| `tests/integration/memory/test_resolution_repo_e2e.py` (new) | `PostgresResolutionRepository`: atomic success, 404 missing, 409 already-resolved. |
| `tests/integration/memory/test_memory_e2e.py` (new) | End-to-end: opened path writes embedding; resolved path recomputes; cosine distance changes; replay idempotency; HNSW EXPLAIN. |
| `tests/integration/memory/test_resolve_route_e2e.py` (new) | E2E POST /resolve through TestClient against real Postgres (minimal app inline, not full lifespan). |

---

## Tasks

### Task 1: Add `fastembed` dep + write migration 0005 + update models

**Files:**
- Modify: `pyproject.toml`
- Create: `migrations/versions/0005_embedding_dim_1024_and_event_id.py`
- Modify: `sentinel/persistence/models.py`
- Create: `tests/integration/memory/__init__.py`
- Create: `tests/integration/memory/test_migration_0005.py`

- [ ] **Step 1: Add `fastembed` to `pyproject.toml` dependencies**

Open `pyproject.toml`. In the `[project] dependencies` list, append:

```toml
  "fastembed>=0.4,<0.5",
```

Place it after `pgvector>=0.3.0,<0.4.0,` to keep the grouping logical (data/ML deps).

- [ ] **Step 2: Install the new dep**

Run:
```bash
make bootstrap
```
Expected: pip resolves `fastembed` + `onnxruntime` + small transitive deps. ~150MB additional install. Output ends with "Successfully installed ...".

If `make bootstrap` is a no-op (already up to date marker), force a reinstall:
```bash
.venv/bin/pip install -e '.[dev]'
```

- [ ] **Step 3: Verify `fastembed` imports**

Run:
```bash
.venv/bin/python -c "from fastembed import TextEmbedding; print('ok')"
```
Expected: `ok` printed. (First import does not download a model.)

- [ ] **Step 4: Create the memory integration conftest (mirrors diagnosis/enrichment)**

Create `tests/integration/memory/__init__.py` (empty file).

Create `tests/integration/memory/conftest.py` — mirrors `tests/integration/diagnosis/conftest.py` exactly (testcontainers Postgres, session-scope `_migrate`, per-test `_reset_state`). We do NOT need Kafka — the consumer tests invoke `consumer.handle_message` directly with fabricated messages.

```python
# tests/integration/memory/conftest.py
"""Testcontainers-backed Postgres for memory integration tests.

Mirrors tests/integration/diagnosis/conftest.py. Memory tests invoke
MemoryConsumer.handle_message directly with fabricated ConsumerRecords,
so no Kafka container is required.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    image = os.environ.get("SENTINEL_TEST_PG_IMAGE", "pgvector/pgvector:pg16")
    with PostgresContainer(image, driver="asyncpg") as pg:
        yield pg


@pytest.fixture()
def pg_dsn(pg_container: PostgresContainer) -> str:
    raw: str = pg_container.get_connection_url()
    return raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )


@pytest.fixture(scope="session", autouse=True)
def _migrate(pg_container: PostgresContainer) -> None:
    """Apply all migrations once per session via in-process alembic.

    Sync fixture on purpose: alembic env.py uses asyncio.run itself, which
    fails if called from inside a running event loop.
    """
    from alembic import command
    from alembic.config import Config

    raw = pg_container.get_connection_url()
    async_dsn = raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )
    root = Path(__file__).resolve().parents[3]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", async_dsn)
    command.upgrade(cfg, "head")


@pytest.fixture(autouse=True)
def _reset_state(pg_dsn: str) -> Iterator[None]:
    """Reset Postgres per-test. CASCADE handles resolutions/diagnoses FK refs."""

    async def _truncate() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text("TRUNCATE incidents, outbox_events CASCADE")
                )
        finally:
            await engine.dispose()

    asyncio.run(_truncate())
    yield


@pytest.fixture()
def session_factory(pg_dsn: str):  # type: ignore[no-untyped-def]
    """Builds a session_factory for a fresh engine per test.

    Tests should close the engine if they care; for typical short tests the
    engine is GC'd at fixture teardown.
    """
    from sentinel.persistence.session import make_session_factory

    engine = create_async_engine(pg_dsn)
    yield make_session_factory(engine)
    asyncio.run(engine.dispose())
```

- [ ] **Step 5: Write the failing migration round-trip test**

Create `tests/integration/memory/test_migration_0005.py`. Mirrors `tests/integration/persistence/test_migration_0003.py` pattern (sync alembic helpers, async query). The session-scope `_migrate` autouse fixture already ran `upgrade head` so columns/indexes should be present; this test additionally exercises the downgrade/upgrade round-trip.

```python
# tests/integration/memory/test_migration_0005.py
"""Migration 0005 produces the expected schema and is reversible."""

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


async def _vector_dim(engine, table: str, column: str) -> int:  # type: ignore[no-untyped-def]
    """Return pgvector column dim via pg_attribute.atttypmod (which encodes dim)."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT a.atttypmod "
                    "FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid "
                    "WHERE c.relname = :table AND a.attname = :column"
                ),
                {"table": table, "column": column},
            )
        ).first()
    assert row is not None, f"{table}.{column} missing"
    return int(row[0])


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


async def _index_names(engine) -> set[str]:  # type: ignore[no-untyped-def]
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT indexname FROM pg_indexes "
                    "WHERE indexname IN ("
                    "  'idx_incidents_embedding', 'idx_runbooks_embedding', "
                    "  'ix_incidents_last_embedding_event_id'"
                    ")"
                )
            )
        ).all()
    return {r[0] for r in rows}


def test_upgrade_produces_1024_dim_columns_and_event_id(pg_dsn: str) -> None:
    """Session-level _migrate already ran upgrade head. Verify the result."""
    async def _check() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            assert await _vector_dim(engine, "incidents", "embedding") == 1024
            assert await _vector_dim(engine, "runbooks", "embedding") == 1024
            assert await _column_exists(engine, "incidents", "last_embedding_event_id")
            assert await _index_names(engine) == {
                "idx_incidents_embedding",
                "idx_runbooks_embedding",
                "ix_incidents_last_embedding_event_id",
            }
        finally:
            await engine.dispose()

    asyncio.run(_check())


def test_round_trip_downgrade_then_upgrade(pg_dsn: str) -> None:
    """Downgrade one revision then upgrade back; schema returns to head."""
    cfg = _alembic_cfg(pg_dsn)
    command.downgrade(cfg, "-1")

    async def _check_downgraded() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            # After downgrade, columns are 1536-dim (the pre-0005 state).
            assert await _vector_dim(engine, "incidents", "embedding") == 1536
            assert not await _column_exists(engine, "incidents", "last_embedding_event_id")
        finally:
            await engine.dispose()

    asyncio.run(_check_downgraded())

    command.upgrade(cfg, "head")

    async def _check_upgraded_again() -> None:
        engine = create_async_engine(pg_dsn)
        try:
            assert await _vector_dim(engine, "incidents", "embedding") == 1024
            assert await _column_exists(engine, "incidents", "last_embedding_event_id")
        finally:
            await engine.dispose()

    asyncio.run(_check_upgraded_again())
```

- [ ] **Step 6: Run the failing test**

Run:
```bash
.venv/bin/pytest tests/integration/memory/test_migration_0005.py -v -m integration
```
Expected: fails — migration not present (and `_migrate` autouse fixture will fail to find revision 0005).

- [ ] **Step 7: Write the migration**

Create `migrations/versions/0005_embedding_dim_1024_and_event_id.py`:

```python
# migrations/versions/0005_embedding_dim_1024_and_event_id.py
"""switch embedding columns to 1024 dim (bge-large) and add last_embedding_event_id

Revision ID: 0005_embedding_dim_1024_and_event_id
Revises: 0004_diagnoses_idempotency
Create Date: 2026-05-19

The existing Vector(1536) columns were aspirational and never populated.
Switching to Vector(1024) to match BAAI/bge-large-en-v1.5 dimensions.
pgvector does not support ALTER COLUMN dim; drop+recreate is required.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from pgvector.sqlalchemy import Vector
from sqlalchemy.dialects import postgresql

revision = "0005_embedding_dim_1024_and_event_id"
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
```

Verify the `down_revision` matches the latest existing migration. Run `ls migrations/versions/` and confirm `0004_diagnoses_idempotency.py` is the latest. If a different file is latest, update `down_revision` to its `revision` field.

- [ ] **Step 8: Update SQLAlchemy models**

Open `sentinel/persistence/models.py`. Locate `IncidentModel.embedding`:

```python
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1536), nullable=True)
```

Replace with:

```python
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    last_embedding_event_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), nullable=True
    )
```

Locate `RunbookModel.embedding`:

```python
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1536), nullable=True)
```

Replace with:

```python
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
```

Add a partial-index `__table_args__` entry on `IncidentModel` for `last_embedding_event_id` to mirror the migration index (so autogenerate detects no drift). Locate the existing `__table_args__` tuple on `IncidentModel`. The current last index entry is:

```python
        Index(
            "ix_incidents_last_enrichment_event_id",
            "last_enrichment_event_id",
            postgresql_where=text("last_enrichment_event_id IS NOT NULL"),
        ),
```

Append a sibling:

```python
        Index(
            "ix_incidents_last_embedding_event_id",
            "last_embedding_event_id",
            postgresql_where=text("last_embedding_event_id IS NOT NULL"),
        ),
```

- [ ] **Step 9: Run the migration test**

Run:
```bash
.venv/bin/pytest tests/integration/memory/test_migration_0005.py -v -m integration
```
Expected: both tests pass. First run takes ~30s (pulls pgvector image + applies all migrations).

- [ ] **Step 10: Run migrate-down/migrate round-trip locally**

Run (requires `make compose-up` first):
```bash
make migrate
make migrate-down
make migrate
```
Expected: all three commands succeed. Last `make migrate` returns to head with no schema drift.

- [ ] **Step 11: Lint + typecheck**

Run:
```bash
make lint && make typecheck
```
Expected: both clean.

- [ ] **Step 12: Stage and pause for review**

Run:
```bash
git add pyproject.toml migrations/versions/0005_embedding_dim_1024_and_event_id.py sentinel/persistence/models.py tests/integration/memory/__init__.py tests/integration/memory/conftest.py tests/integration/memory/test_migration_0005.py
git status
```
Expected: 6 files staged. Pause here for review before continuing to Task 2.

---

### Task 2: FastEmbedProvider

**Files:**
- Create: `sentinel/memory/__init__.py`
- Create: `sentinel/memory/embeddings.py`
- Create: `tests/unit/memory/__init__.py`
- Create: `tests/unit/memory/test_embeddings.py`

- [ ] **Step 1: Create the package marker files**

Create `sentinel/memory/__init__.py`:

```python
# sentinel/memory/__init__.py
"""Memory & feedback loop — embedding provider, retrieval store, pipeline, consumer."""

from sentinel.memory.embeddings import FastEmbedProvider
from sentinel.memory.pipeline import MemoryPipeline

__all__ = ["FastEmbedProvider", "MemoryPipeline"]
```

Note: `PgVectorIncidentStore`, `MemoryConsumer`, `MemoryConsumerDeps` get added to `__all__` in later tasks. We add `MemoryPipeline` here even though `pipeline.py` doesn't exist yet — Task 3 creates it. If you want to keep this task self-contained, comment-out the `MemoryPipeline` line and re-enable in Task 3. Either way works.

Create `tests/unit/memory/__init__.py` (empty file).

- [ ] **Step 2: Write failing tests for FastEmbedProvider**

Create `tests/unit/memory/test_embeddings.py`:

```python
# tests/unit/memory/test_embeddings.py
"""FastEmbedProvider unit tests — lazy load, dim, determinism, timeout."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from sentinel.memory.embeddings import FastEmbedProvider

pytestmark = pytest.mark.asyncio


@pytest.fixture
def cache_dir(tmp_path: Path) -> Path:
    return tmp_path / "fastembed-cache"


async def test_embed_returns_1024_dim_vector(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    vec = await provider.embed("hello world")
    assert isinstance(vec, list)
    assert len(vec) == 1024
    assert all(isinstance(x, float) for x in vec)


async def test_embed_is_deterministic(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    a = await provider.embed("same input")
    b = await provider.embed("same input")
    assert a == b


async def test_embed_different_inputs_differ(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    a = await provider.embed("first input text")
    b = await provider.embed("second different text")
    assert a != b


async def test_dim_class_attribute_matches_output(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    vec = await provider.embed("test")
    assert FastEmbedProvider.DIM == len(vec)


async def test_timeout_raises_timeouterror(cache_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the underlying compute exceeds compute_timeout_s, embed raises TimeoutError."""
    provider = FastEmbedProvider(model_cache_dir=cache_dir, compute_timeout_s=0.001)

    # Force the executor call to block.
    real_loop = asyncio.get_running_loop()

    def slow_executor(_executor, _func, _arg):
        async def _hang() -> list[float]:
            await asyncio.sleep(5)
            return [0.0] * 1024
        return asyncio.ensure_future(_hang())

    monkeypatch.setattr(real_loop, "run_in_executor", slow_executor)

    with pytest.raises(asyncio.TimeoutError):
        await provider.embed("anything")
```

Note: the timeout test patches `run_in_executor` on the running loop. If that proves fragile, an alternate approach is to inject a custom executor or a mocked sync `_embed_sync` method — the production impl exposes `_embed_sync` for this purpose (see Step 3).

- [ ] **Step 3: Run failing tests**

Run:
```bash
.venv/bin/pytest tests/unit/memory/test_embeddings.py -v
```
Expected: fails — `sentinel.memory.embeddings` does not exist.

- [ ] **Step 4: Implement FastEmbedProvider**

Create `sentinel/memory/embeddings.py`:

```python
# sentinel/memory/embeddings.py
"""Local CPU embedding provider via fastembed (ONNX, no torch, no API keys).

Concrete impl of EmbeddingProvider Protocol (declared in enrichment/protocols.py).
Lazy-loads the bge-large-en-v1.5 model on first call; subsequent calls are
~80-150ms on CPU. Wrapped in run_in_executor so the model call doesn't block
the event loop; bounded by an asyncio timeout so a pathological input can't
stall a consumer.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastembed import TextEmbedding

_LOG = logging.getLogger("sentinel.memory.embeddings")


class FastEmbedProvider:
    DIM: int = 1024
    MODEL_NAME: str = "BAAI/bge-large-en-v1.5"

    def __init__(
        self,
        *,
        model_cache_dir: Path,
        compute_timeout_s: float = 5.0,
    ) -> None:
        self._cache_dir = Path(model_cache_dir)
        self._timeout_s = compute_timeout_s
        self._model: TextEmbedding | None = None
        self._lock = asyncio.Lock()

    async def _ensure_loaded(self) -> None:
        if self._model is not None:
            return
        async with self._lock:
            if self._model is not None:
                return
            from fastembed import TextEmbedding

            _LOG.info(
                "loading_embedding_model",
                extra={"model": self.MODEL_NAME, "cache_dir": str(self._cache_dir)},
            )
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            self._model = TextEmbedding(
                model_name=self.MODEL_NAME, cache_dir=str(self._cache_dir)
            )

    def _embed_sync(self, text: str) -> list[float]:
        assert self._model is not None
        # fastembed returns a generator yielding numpy arrays.
        for arr in self._model.embed([text]):
            return [float(x) for x in arr]
        raise RuntimeError("fastembed produced no output")

    async def embed(self, text: str) -> list[float]:
        await self._ensure_loaded()
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(None, self._embed_sync, text),
            timeout=self._timeout_s,
        )
```

- [ ] **Step 5: Run tests**

Run:
```bash
.venv/bin/pytest tests/unit/memory/test_embeddings.py -v
```
Expected: passes. First test downloads the ~400MB model (one-time, into `tmp_path` — slow on first run, fast after). Subsequent test runs reuse the model.

If the timeout test fails because the monkeypatch approach is too fragile, replace it with:

```python
async def test_timeout_raises_timeouterror(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir, compute_timeout_s=0.0001)
    # First call has to load the model — that won't fit in 0.1ms either.
    with pytest.raises(asyncio.TimeoutError):
        await provider.embed("anything")
```

This is a less precise test (it might also catch a load timeout), but it does verify the wait_for boundary.

- [ ] **Step 6: Lint + typecheck**

Run:
```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 7: Stage and pause for review**

```bash
git add sentinel/memory/__init__.py sentinel/memory/embeddings.py tests/unit/memory/__init__.py tests/unit/memory/test_embeddings.py
git status
```

---

### Task 3: MemoryPipeline (text composition)

**Files:**
- Create: `sentinel/memory/pipeline.py`
- Create: `tests/unit/memory/test_pipeline.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/memory/test_pipeline.py`:

```python
# tests/unit/memory/test_pipeline.py
"""MemoryPipeline — text composition for embedding inputs."""

from __future__ import annotations

from uuid import uuid4

from sentinel.memory.pipeline import MemoryPipeline
from sentinel.persistence.repositories import MemoryIncidentRow, ResolutionData


def _row(*, service: str = "api", title: str = "5xx surge") -> MemoryIncidentRow:
    return MemoryIncidentRow(
        id=uuid4(), service=service, title=title, status="open", resolution=None,
    )


def test_compose_initial_is_service_then_title() -> None:
    pipeline = MemoryPipeline()
    row = _row(service="checkout", title="latency p99 above SLO")
    assert pipeline.compose_initial(row) == "checkout latency p99 above SLO"


def test_compose_resolved_joins_title_root_cause_remediation_with_newlines() -> None:
    pipeline = MemoryPipeline()
    row = _row(title="db deadlock")
    resolution = ResolutionData(
        root_cause="long transaction on users table during migration",
        remediation="killed migration; switched to online schema change",
        diagnosis_was_correct=True,
    )
    expected = (
        "db deadlock\n"
        "long transaction on users table during migration\n"
        "killed migration; switched to online schema change"
    )
    assert pipeline.compose_resolved(row, resolution) == expected


def test_compose_initial_strips_no_input() -> None:
    """We do not strip — empty fields produce visible empties (caller's responsibility)."""
    pipeline = MemoryPipeline()
    row = _row(service="", title="")
    assert pipeline.compose_initial(row) == " "
```

Note: `MemoryIncidentRow` and `ResolutionData` are defined in Task 4 (in `repositories.py`). This test will fail at import time until then. That's expected — Task 4 unblocks it.

- [ ] **Step 2: Run failing test**

Run:
```bash
.venv/bin/pytest tests/unit/memory/test_pipeline.py -v
```
Expected: fails — `MemoryIncidentRow` not importable.

- [ ] **Step 3: Implement MemoryPipeline (stub the dataclass imports temporarily)**

Create `sentinel/memory/pipeline.py`:

```python
# sentinel/memory/pipeline.py
"""Text composition for embedding inputs.

Pure functions over MemoryIncidentRow / ResolutionData. The opened path embeds
'service title' (symmetric with how SimilarIncidentsFetcher embeds its query);
the resolved path embeds 'title\\nroot_cause\\nremediation' (the richer signal
that makes similar-incident retrieval get better with use).
"""

from __future__ import annotations

from sentinel.persistence.repositories import MemoryIncidentRow, ResolutionData


class MemoryPipeline:
    def compose_initial(self, row: MemoryIncidentRow) -> str:
        return f"{row.service} {row.title}"

    def compose_resolved(self, row: MemoryIncidentRow, resolution: ResolutionData) -> str:
        return f"{row.title}\n{resolution.root_cause}\n{resolution.remediation}"
```

- [ ] **Step 4: Don't run tests yet**

The dataclasses don't exist; Task 4 creates them. Skip to lint/typecheck — both will also fail until Task 4 lands. Note the failure mode and proceed.

- [ ] **Step 5: Stage and pause for review**

```bash
git add sentinel/memory/pipeline.py tests/unit/memory/test_pipeline.py
git status
```

(The pipeline test stays red until Task 4 introduces the dataclasses; this is intentional TDD ordering — the dataclass owners (repo) come next.)

---

### Task 4: IncidentRepository — `set_embedding` and `load_for_memory`

**Files:**
- Modify: `sentinel/persistence/repositories.py`
- Create: `tests/integration/memory/test_incident_repo_memory.py`

- [ ] **Step 1: Add dataclasses + Protocol methods**

Open `sentinel/persistence/repositories.py`. Near the top, in the `# --- Read DTOs ----` section (after the existing `DeployRow` dataclass), add:

```python
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
class ResolveRecordResult:
    incident_id: UUID
    resolved_at: datetime
    event_id: UUID
```

(The `ResolveRecordResult` is for Task 7 but we add it now to keep DTOs grouped.)

Locate the `IncidentRepository` Protocol class. Add two methods after `recent_for_service`:

```python
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
```

- [ ] **Step 2: Implement on `PostgresIncidentRepository`**

Locate `PostgresIncidentRepository`. After `recent_for_service`, add:

```python
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
                    stmt, {"vec": embedding, "event_id": event_id, "incident_id": incident_id}
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
```

Also export the new dataclasses from `__all__` at the bottom of the file:

```python
__all__ = [
    ...
    "MemoryIncidentRow",
    "ResolutionData",
    "ResolveRecordResult",
    ...
]
```

- [ ] **Step 3: Now the pipeline test from Task 3 should pass**

Run:
```bash
.venv/bin/pytest tests/unit/memory/test_pipeline.py -v
```
Expected: 3 passed.

- [ ] **Step 4: Write integration tests for the repo methods**

Create `tests/integration/memory/test_incident_repo_memory.py`:

```python
# tests/integration/memory/test_incident_repo_memory.py
"""Integration tests for IncidentRepository.set_embedding and load_for_memory."""

from __future__ import annotations

from uuid import uuid4

import pytest

from sentinel.persistence.repositories import (
    MemoryIncidentRow,
    PostgresIncidentRepository,
    ResolutionData,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture
def incident_repo(session_factory) -> PostgresIncidentRepository:  # type: ignore[no-untyped-def]
    return PostgresIncidentRepository(session_factory)


# Helper to insert an incident row directly (no webhook plumbing in this test).
async def _insert_incident(session_factory, **kwargs) -> str:  # type: ignore[no-untyped-def]
    from sqlalchemy import text
    defaults = {
        "external_id": "ext-1", "source": "generic", "service": "api",
        "severity": "SEV3", "title": "test incident", "fingerprint": "fp1",
        "raw_payload": '{"_sentinel": {"occurrence_count": 1}}',
    }
    defaults.update(kwargs)
    async with session_factory() as s:
        row = (await s.execute(
            text(
                "INSERT INTO incidents (external_id, source, service, severity, title, "
                "fingerprint, raw_payload) "
                "VALUES (:external_id, :source, :service, :severity, :title, "
                ":fingerprint, CAST(:raw_payload AS jsonb)) RETURNING id"
            ),
            defaults,
        )).scalar_one()
        await s.commit()
    return str(row)


async def test_set_embedding_first_call_writes(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_incident(session_factory)
    event_id = uuid4()
    vec = [0.1] * 1024
    result = await incident_repo.set_embedding(incident_id, vec, event_id=event_id)
    assert result == "written"


async def test_set_embedding_duplicate_event_id_is_noop(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_incident(session_factory)
    event_id = uuid4()
    vec = [0.1] * 1024
    first = await incident_repo.set_embedding(incident_id, vec, event_id=event_id)
    second = await incident_repo.set_embedding(incident_id, vec, event_id=event_id)
    assert first == "written"
    assert second == "duplicate"


async def test_set_embedding_unknown_incident(incident_repo) -> None:  # type: ignore[no-untyped-def]
    result = await incident_repo.set_embedding(uuid4(), [0.0] * 1024, event_id=uuid4())
    assert result == "unknown_incident"


async def test_load_for_memory_returns_none_for_missing(incident_repo) -> None:  # type: ignore[no-untyped-def]
    assert await incident_repo.load_for_memory(uuid4()) is None


async def test_load_for_memory_no_resolution(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_incident(session_factory, service="svc", title="hello")
    row = await incident_repo.load_for_memory(incident_id)
    assert isinstance(row, MemoryIncidentRow)
    assert row.service == "svc"
    assert row.title == "hello"
    assert row.resolution is None


async def test_load_for_memory_with_resolution(incident_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    from sqlalchemy import text
    incident_id = await _insert_incident(session_factory, service="db", title="deadlock")
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category, "
                "diagnosis_was_correct) "
                "VALUES (:id, 'long tx', 'killed', 'data', TRUE)"
            ),
            {"id": incident_id},
        )
        await s.commit()
    row = await incident_repo.load_for_memory(incident_id)
    assert row is not None
    assert row.resolution is not None
    assert row.resolution.root_cause == "long tx"
    assert row.resolution.remediation == "killed"
    assert row.resolution.diagnosis_was_correct is True
```

The `session_factory` and autouse `_reset_state` fixtures come from `tests/integration/memory/conftest.py` (created in Task 1, Step 4).

- [ ] **Step 5: Run integration tests**

Run:
```bash
.venv/bin/pytest tests/integration/memory/test_incident_repo_memory.py -v -m integration
```
Expected: 6 passed.

- [ ] **Step 6: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 7: Stage and pause for review**

```bash
git add sentinel/persistence/repositories.py tests/integration/memory/test_incident_repo_memory.py
git status
```

---

### Task 5: PgVectorIncidentStore

**Files:**
- Create: `sentinel/memory/store.py`
- Create: `tests/unit/memory/test_store.py`
- Create: `tests/integration/memory/test_store_e2e.py`

- [ ] **Step 1: Write failing unit tests**

Create `tests/unit/memory/test_store.py`:

```python
# tests/unit/memory/test_store.py
"""PgVectorIncidentStore unit tests — embed-failure path; session-level mocking only.

DB-level top_k correctness is exercised in tests/integration/memory/test_store_e2e.py.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from sentinel.memory.store import PgVectorIncidentStore

pytestmark = pytest.mark.asyncio


class _FakeEmbedder:
    def __init__(self, *, raises: Exception | None = None) -> None:
        self._raises = raises

    async def embed(self, text: str) -> list[float]:
        if self._raises:
            raise self._raises
        return [0.1] * 1024


async def test_top_k_returns_failed_on_embed_exception() -> None:
    embedder = _FakeEmbedder(raises=RuntimeError("model crashed"))
    # session_factory is not called because embed fails first; an AsyncMock suffices.
    store = PgVectorIncidentStore(
        session_factory=AsyncMock(),
        embedding_provider=embedder,
    )
    result = await store.top_k(query_text="anything", k=5, exclude_incident_id=None)
    assert result.status == "failed"
    assert result.error is not None
    assert "RuntimeError" in result.error
    assert result.data == []


async def test_top_k_returns_failed_on_embed_timeout() -> None:
    import asyncio
    embedder = _FakeEmbedder(raises=asyncio.TimeoutError())
    store = PgVectorIncidentStore(
        session_factory=AsyncMock(),
        embedding_provider=embedder,
    )
    result = await store.top_k(query_text="anything", k=5, exclude_incident_id=None)
    assert result.status == "failed"
    assert result.error is not None
    assert "TimeoutError" in result.error
```

- [ ] **Step 2: Run failing test**

Run:
```bash
.venv/bin/pytest tests/unit/memory/test_store.py -v
```
Expected: fails — `sentinel.memory.store` does not exist.

- [ ] **Step 3: Implement PgVectorIncidentStore**

Create `sentinel/memory/store.py`:

```python
# sentinel/memory/store.py
"""pgvector-backed SimilarIncidentRetrieval — cosine top-k with spec filters.

Filters (from spec §H acceptance):
  - i.status IN ('resolved', 'closed')
  - r.diagnosis_was_correct IS NULL OR r.diagnosis_was_correct = TRUE
  - exclude the query incident if provided

JOIN on resolutions is the correctness gate AND fills the response shape
(SimilarIncidentItem requires root_cause + remediation).
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from sentinel.observability.metrics import (
    similar_incidents_query_duration_seconds,
    similar_incidents_returned_total,
)
from sentinel.schemas.context import FetcherResult, SimilarIncidentItem
from sentinel.schemas.ids import similar_id

if TYPE_CHECKING:
    from sentinel.enrichment.protocols import EmbeddingProvider

_LOG = logging.getLogger("sentinel.memory.store")


class PgVectorIncidentStore:
    """Concrete SimilarIncidentRetrieval — pgvector cosine top-k."""

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        embedding_provider: EmbeddingProvider,
    ) -> None:
        self._session_factory = session_factory
        self._embedder = embedding_provider

    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        exclude_incident_id: UUID | None,
    ) -> FetcherResult[SimilarIncidentItem]:
        try:
            query_vec = await self._embedder.embed(query_text)
        except Exception as e:
            _LOG.warning(
                "similar_incidents_embed_failed",
                extra={"err": repr(e), "exc_type": type(e).__name__},
            )
            return FetcherResult(
                status="failed",
                data=[],
                error=f"embed_failed: {type(e).__name__}",
                fetched_at=datetime.now(UTC),
            )

        sql = text(
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
        start = time.monotonic()
        try:
            async with self._session_factory() as s:
                rows = (
                    await s.execute(
                        sql,
                        {
                            "qvec": query_vec,
                            "exclude_id": str(exclude_incident_id) if exclude_incident_id else None,
                            "k": k,
                        },
                    )
                ).all()
        except Exception as e:
            _LOG.warning(
                "similar_incidents_query_failed",
                extra={"err": repr(e), "exc_type": type(e).__name__},
            )
            return FetcherResult(
                status="failed",
                data=[],
                error=f"query_failed: {type(e).__name__}",
                fetched_at=datetime.now(UTC),
            )
        finally:
            similar_incidents_query_duration_seconds.observe(time.monotonic() - start)

        items = [
            SimilarIncidentItem(
                id=similar_id(r.id),
                title=r.title,
                root_cause=r.root_cause,
                remediation=r.remediation,
                cosine_similarity=float(r.cosine_similarity),
            )
            for r in rows
        ]
        similar_incidents_returned_total.observe(len(items))
        return FetcherResult(
            status="ok",
            data=items,
            error=None,
            fetched_at=datetime.now(UTC),
        )
```

- [ ] **Step 4: Register the metrics**

Open `sentinel/observability/metrics.py`. Find the existing metric declarations. Add (at an appropriate grouping point):

```python
similar_incidents_query_duration_seconds = Histogram(
    "sentinel_similar_incidents_query_duration_seconds",
    "Wall time of the pgvector top-k query in PgVectorIncidentStore.top_k.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

similar_incidents_returned_total = Histogram(
    "sentinel_similar_incidents_returned_total",
    "Number of SimilarIncidentItems returned per top_k call.",
    buckets=(0, 1, 2, 3, 5, 10, 25),
)
```

Confirm `Histogram` is already imported at the top of the file; if not, add `from prometheus_client import Histogram` (or extend the existing import).

- [ ] **Step 5: Update `sentinel/memory/__init__.py`**

Append:

```python
from sentinel.memory.store import PgVectorIncidentStore
```

Update `__all__` to include `"PgVectorIncidentStore"`.

- [ ] **Step 6: Run unit tests**

```bash
.venv/bin/pytest tests/unit/memory/test_store.py -v
```
Expected: 2 passed.

- [ ] **Step 7: Write E2E (integration) tests**

Create `tests/integration/memory/test_store_e2e.py`:

```python
# tests/integration/memory/test_store_e2e.py
"""PgVectorIncidentStore against a real Postgres with pgvector + HNSW."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text

from sentinel.memory.embeddings import FastEmbedProvider
from sentinel.memory.store import PgVectorIncidentStore

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture
def embedder(tmp_path: Path) -> FastEmbedProvider:
    return FastEmbedProvider(model_cache_dir=tmp_path / "fastembed-cache")


@pytest.fixture
def store(session_factory, embedder) -> PgVectorIncidentStore:  # type: ignore[no-untyped-def]
    return PgVectorIncidentStore(session_factory=session_factory, embedding_provider=embedder)


async def _seed_resolved_incident(
    session_factory, embedder, *,  # type: ignore[no-untyped-def]
    service: str, title: str, root_cause: str, remediation: str,
    status: str = "resolved", diagnosis_was_correct: bool | None = True,
) -> str:
    vec = await embedder.embed(f"{title}\n{root_cause}\n{remediation}")
    async with session_factory() as s:
        incident_id = (await s.execute(
            text(
                "INSERT INTO incidents (external_id, source, service, severity, title, "
                "fingerprint, raw_payload, status, embedding) "
                "VALUES (:eid, 'generic', :service, 'SEV3', :title, :fp, "
                "        CAST('{}' AS jsonb), :status, CAST(:vec AS vector)) "
                "RETURNING id"
            ),
            {
                "eid": f"e-{uuid4()}", "service": service, "title": title,
                "fp": f"fp-{uuid4()}", "status": status, "vec": vec,
            },
        )).scalar_one()
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category, "
                "diagnosis_was_correct) "
                "VALUES (:id, :rc, :rm, 'data', :dc)"
            ),
            {"id": incident_id, "rc": root_cause, "rm": remediation,
             "dc": diagnosis_was_correct},
        )
        await s.commit()
    return str(incident_id)


async def test_top_k_filters_unresolved_incidents(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    # Seed: one resolved (matches), one open (excluded by status filter).
    await _seed_resolved_incident(
        session_factory, embedder,
        service="api", title="db connection timeout",
        root_cause="pool exhaustion", remediation="raised pool size",
    )
    # Open incident: insert without resolutions row to avoid JOIN match
    # AND with status='open' to verify the status filter.
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO incidents (external_id, source, service, severity, title, "
                "fingerprint, raw_payload, status, embedding) "
                "VALUES (:eid, 'generic', 'api', 'SEV3', 'unrelated', :fp, "
                "        CAST('{}' AS jsonb), 'open', CAST(:vec AS vector))"
            ),
            {"eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}",
             "vec": await embedder.embed("unrelated text")},
        )
        await s.commit()

    result = await store.top_k(query_text="connection pool exhaustion",
                               k=5, exclude_incident_id=None)
    assert result.status == "ok"
    titles = {item.title for item in result.data}
    assert "db connection timeout" in titles
    assert "unrelated" not in titles


async def test_top_k_excludes_diagnosis_was_correct_false(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    await _seed_resolved_incident(
        session_factory, embedder,
        service="api", title="should be excluded",
        root_cause="x", remediation="y", diagnosis_was_correct=False,
    )
    await _seed_resolved_incident(
        session_factory, embedder,
        service="api", title="should be included (null)",
        root_cause="x", remediation="y", diagnosis_was_correct=None,
    )
    await _seed_resolved_incident(
        session_factory, embedder,
        service="api", title="should be included (true)",
        root_cause="x", remediation="y", diagnosis_was_correct=True,
    )
    result = await store.top_k(query_text="anything", k=10, exclude_incident_id=None)
    titles = {item.title for item in result.data}
    assert "should be excluded" not in titles
    assert "should be included (null)" in titles
    assert "should be included (true)" in titles


async def test_top_k_excludes_query_incident(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    excluded = await _seed_resolved_incident(
        session_factory, embedder,
        service="api", title="exclude me", root_cause="x", remediation="y",
    )
    await _seed_resolved_incident(
        session_factory, embedder,
        service="api", title="keep me", root_cause="x", remediation="y",
    )
    from uuid import UUID
    result = await store.top_k(
        query_text="anything", k=10, exclude_incident_id=UUID(excluded),
    )
    titles = {item.title for item in result.data}
    assert "exclude me" not in titles
    assert "keep me" in titles


async def test_top_k_returns_id_in_similar_prefix(store, session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    await _seed_resolved_incident(
        session_factory, embedder,
        service="api", title="t", root_cause="x", remediation="y",
    )
    result = await store.top_k(query_text="anything", k=5, exclude_incident_id=None)
    assert result.status == "ok"
    assert result.data, "expected at least one result"
    for item in result.data:
        assert item.id.startswith("similar:"), item.id
        assert -1.0 <= item.cosine_similarity <= 1.0
```

- [ ] **Step 8: Run E2E tests**

```bash
.venv/bin/pytest tests/integration/memory/test_store_e2e.py -v -m integration
```
Expected: 4 passed. First test downloads the model (one-time).

- [ ] **Step 9: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 10: Stage and pause for review**

```bash
git add sentinel/memory/store.py sentinel/memory/__init__.py sentinel/observability/metrics.py \
    tests/unit/memory/test_store.py tests/integration/memory/test_store_e2e.py
git status
```

---

### Task 6: Persistence errors + `ResolveIncidentResponse`

**Files:**
- Create: `sentinel/persistence/errors.py`
- Modify: `sentinel/schemas/api.py`

- [ ] **Step 1: Create the errors module**

Create `sentinel/persistence/errors.py`:

```python
# sentinel/persistence/errors.py
"""Domain exceptions raised by persistence repositories.

Mirrors the sentinel/diagnosis/errors.py precedent: small, explicit exception
types that map cleanly to HTTP statuses at the route layer.
"""

from __future__ import annotations

from uuid import UUID


class IncidentNotFound(Exception):
    def __init__(self, incident_id: UUID) -> None:
        super().__init__(f"incident not found: {incident_id}")
        self.incident_id = incident_id


class IncidentAlreadyResolved(Exception):
    def __init__(self, incident_id: UUID) -> None:
        super().__init__(f"incident already resolved: {incident_id}")
        self.incident_id = incident_id
```

- [ ] **Step 2: Add `ResolveIncidentResponse` to `sentinel/schemas/api.py`**

Open `sentinel/schemas/api.py`. After the `ResolveIncidentRequest` class (line 51), add:

```python
class ResolveIncidentResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    incident_id: UUID
    status: Literal["resolved"]
    resolved_at: datetime
    event_id: UUID  # staged `incident.resolved` outbox event_id
```

`UUID`, `datetime`, `Literal` are already imported at the top of the file. No new imports needed.

- [ ] **Step 3: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 4: Stage and pause for review**

```bash
git add sentinel/persistence/errors.py sentinel/schemas/api.py
git status
```

---

### Task 7: PostgresResolutionRepository

**Files:**
- Modify: `sentinel/persistence/repositories.py`
- Create: `tests/unit/persistence/test_resolution_repo.py`
- Create: `tests/integration/memory/test_resolution_repo_e2e.py`

- [ ] **Step 1: Update `ResolutionRepository` Protocol and add concrete impl**

Open `sentinel/persistence/repositories.py`. Locate the `ResolutionRepository` Protocol stub:

```python
class ResolutionRepository(Protocol):
    async def record(self, incident_id: UUID, resolution: ResolveIncidentRequest) -> None: ...
```

Replace with:

```python
class ResolutionRepository(Protocol):
    async def record(
        self,
        incident_id: UUID,
        resolution: ResolveIncidentRequest,
        *,
        outbox_topic: str,
    ) -> ResolveRecordResult: ...
```

After the `PostgresDiagnosisRepository` class (near the end), add:

```python
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
            # 1. Lock the incident row.
            incident = (
                await session.execute(
                    select(IncidentModel)
                    .where(IncidentModel.id == incident_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if incident is None:
                raise IncidentNotFound(incident_id)
            if incident.status in ("resolved", "closed"):
                raise IncidentAlreadyResolved(incident_id)

            now = (await session.execute(select(func.now()))).scalar_one()
            now_iso = now.isoformat()

            # 2. INSERT resolutions row.
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

            # 3. UPDATE incident.
            incident.status = "resolved"
            incident.resolved_at = now

            # 4. Stage incident.resolved outbox row.
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

        return ResolveRecordResult(
            incident_id=incident_id, resolved_at=now, event_id=event_id
        )
```

Also add `PostgresResolutionRepository` to the bottom `__all__` list.

- [ ] **Step 2: Write unit tests (exception paths via fake session)**

Create `tests/unit/persistence/test_resolution_repo.py`:

```python
# tests/unit/persistence/test_resolution_repo.py
"""PostgresResolutionRepository unit tests — exception paths via fake session.

Atomic-commit + outbox-payload behavior is covered in
tests/integration/memory/test_resolution_repo_e2e.py.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import PostgresResolutionRepository
from sentinel.schemas.api import ResolveIncidentRequest

pytestmark = pytest.mark.asyncio


def _body() -> ResolveIncidentRequest:
    return ResolveIncidentRequest(
        root_cause="rc",
        remediation="rm",
        category="data",
        diagnosis_was_correct=True,
    )


def _make_session_factory(*, lock_returns: Any) -> Any:
    """Build a fake async_sessionmaker that yields a session whose first
    `execute().scalar_one_or_none()` returns `lock_returns`.
    """
    session = MagicMock()
    # execute() is called many times. The first call's result is what we control;
    # subsequent calls just need to not blow up.
    first_result = MagicMock()
    first_result.scalar_one_or_none.return_value = lock_returns
    session.execute = AsyncMock(return_value=first_result)
    session.begin = AsyncMock()
    session.add = MagicMock()

    @asynccontextmanager
    async def factory():  # type: ignore[no-untyped-def]
        yield session

    return factory, session


async def test_raises_incident_not_found_when_missing() -> None:
    factory, _ = _make_session_factory(lock_returns=None)
    repo = PostgresResolutionRepository(session_factory=factory)
    with pytest.raises(IncidentNotFound):
        await repo.record(uuid4(), _body(), outbox_topic="t")


async def test_raises_incident_already_resolved_when_status_resolved() -> None:
    incident_row = SimpleNamespace(status="resolved")
    factory, _ = _make_session_factory(lock_returns=incident_row)
    repo = PostgresResolutionRepository(session_factory=factory)
    with pytest.raises(IncidentAlreadyResolved):
        await repo.record(uuid4(), _body(), outbox_topic="t")


async def test_raises_incident_already_resolved_when_status_closed() -> None:
    incident_row = SimpleNamespace(status="closed")
    factory, _ = _make_session_factory(lock_returns=incident_row)
    repo = PostgresResolutionRepository(session_factory=factory)
    with pytest.raises(IncidentAlreadyResolved):
        await repo.record(uuid4(), _body(), outbox_topic="t")
```

If `session.begin()` needs to be a context manager rather than `AsyncMock`, replace with:

```python
    @asynccontextmanager
    async def _begin():  # type: ignore[no-untyped-def]
        yield
    session.begin = _begin
```

Try the simpler form first; tweak if pytest complains.

- [ ] **Step 3: Run unit tests**

```bash
.venv/bin/pytest tests/unit/persistence/test_resolution_repo.py -v
```
Expected: 3 passed.

- [ ] **Step 4: Write integration tests**

Create `tests/integration/memory/test_resolution_repo_e2e.py`:

```python
# tests/integration/memory/test_resolution_repo_e2e.py
"""PostgresResolutionRepository against a real Postgres."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text

from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import PostgresResolutionRepository
from sentinel.schemas.api import ResolveIncidentRequest

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture
def resolution_repo(session_factory) -> PostgresResolutionRepository:  # type: ignore[no-untyped-def]
    return PostgresResolutionRepository(session_factory)


def _body() -> ResolveIncidentRequest:
    return ResolveIncidentRequest(
        root_cause="db deadlock during migration",
        remediation="killed migration; ran online schema change instead",
        category="data",
        diagnosis_was_correct=True,
        notes="see #1234",
        resolved_by="oncall@example.com",
    )


async def _insert_open_incident(session_factory) -> str:  # type: ignore[no-untyped-def]
    async with session_factory() as s:
        rid = (await s.execute(
            text(
                "INSERT INTO incidents (external_id, source, service, severity, title, "
                "fingerprint, raw_payload, status) "
                "VALUES (:eid, 'generic', 'api', 'SEV3', 'test', :fp, "
                "CAST('{}' AS jsonb), 'open') RETURNING id"
            ),
            {"eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}"},
        )).scalar_one()
        await s.commit()
    return str(rid)


async def test_record_success_returns_event_id_and_writes_all_three(resolution_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_open_incident(session_factory)
    result = await resolution_repo.record(
        incident_id, _body(), outbox_topic="sentinel.incidents"
    )
    assert result.incident_id == incident_id
    assert result.event_id is not None
    assert result.resolved_at is not None

    # resolutions row exists
    async with session_factory() as s:
        res_row = (await s.execute(
            text("SELECT root_cause, remediation, category FROM resolutions "
                 "WHERE incident_id = :id"),
            {"id": incident_id},
        )).first()
        assert res_row is not None
        assert res_row[0] == "db deadlock during migration"

        # incident is resolved
        inc_row = (await s.execute(
            text("SELECT status, resolved_at FROM incidents WHERE id = :id"),
            {"id": incident_id},
        )).first()
        assert inc_row is not None
        assert inc_row[0] == "resolved"
        assert inc_row[1] is not None

        # outbox row exists with correct event type
        out_row = (await s.execute(
            text("SELECT topic, payload FROM outbox_events WHERE id = :id"),
            {"id": result.event_id},
        )).first()
        assert out_row is not None
        assert out_row[0] == "sentinel.incidents"
        assert out_row[1]["event"] == "incident.resolved"
        assert out_row[1]["event_id"] == str(result.event_id)


async def test_record_raises_for_missing_incident(resolution_repo) -> None:  # type: ignore[no-untyped-def]
    with pytest.raises(IncidentNotFound):
        await resolution_repo.record(
            uuid4(), _body(), outbox_topic="sentinel.incidents"
        )


async def test_record_raises_for_already_resolved(resolution_repo, session_factory) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_open_incident(session_factory)
    # First resolve: success.
    await resolution_repo.record(incident_id, _body(), outbox_topic="sentinel.incidents")
    # Second resolve: raises.
    with pytest.raises(IncidentAlreadyResolved):
        await resolution_repo.record(
            incident_id, _body(), outbox_topic="sentinel.incidents"
        )
```

- [ ] **Step 5: Run integration tests**

```bash
.venv/bin/pytest tests/integration/memory/test_resolution_repo_e2e.py -v -m integration
```
Expected: 3 passed.

- [ ] **Step 6: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 7: Stage and pause for review**

```bash
git add sentinel/persistence/repositories.py \
    tests/unit/persistence/test_resolution_repo.py \
    tests/integration/memory/test_resolution_repo_e2e.py
git status
```

---

### Task 8: Resolve route

**Files:**
- Create: `sentinel/api/routes/resolve.py`
- Create: `tests/unit/api/test_resolve_route.py`

- [ ] **Step 1: Write failing route test**

Create `tests/unit/api/test_resolve_route.py`:

```python
# tests/unit/api/test_resolve_route.py
"""Resolve route — exception mapping and happy path."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from sentinel.api.routes.resolve import router as resolve_router
from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import ResolveRecordResult


def _app(repo) -> FastAPI:  # type: ignore[no-untyped-def]
    app = FastAPI()
    app.state.resolution_repo = repo
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(resolve_router)
    return app


def _body() -> dict:
    return {
        "root_cause": "rc",
        "remediation": "rm",
        "category": "data",
    }


def test_success_returns_200_with_response_schema() -> None:
    incident_id = uuid4()
    event_id = uuid4()
    now = datetime.now(UTC)
    repo = type("R", (), {})()
    repo.record = AsyncMock(return_value=ResolveRecordResult(
        incident_id=incident_id, resolved_at=now, event_id=event_id,
    ))
    client = TestClient(_app(repo))
    resp = client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert resp.status_code == 200
    data = resp.json()
    assert data["incident_id"] == str(incident_id)
    assert data["status"] == "resolved"
    assert data["event_id"] == str(event_id)


def test_returns_404_when_incident_missing() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.record = AsyncMock(side_effect=IncidentNotFound(incident_id))
    client = TestClient(_app(repo))
    resp = client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert resp.status_code == 404
    assert resp.json()["detail"] == "incident_not_found"


def test_returns_409_when_already_resolved() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.record = AsyncMock(side_effect=IncidentAlreadyResolved(incident_id))
    client = TestClient(_app(repo))
    resp = client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert resp.status_code == 409
    assert resp.json()["detail"] == "incident_already_resolved"


def test_invalid_payload_returns_422() -> None:
    incident_id = uuid4()
    repo = type("R", (), {})()
    repo.record = AsyncMock()
    client = TestClient(_app(repo))
    # Missing required field `category`.
    resp = client.post(
        f"/incidents/{incident_id}/resolve",
        json={"root_cause": "x", "remediation": "y"},
    )
    assert resp.status_code == 422
```

- [ ] **Step 2: Run failing tests**

```bash
.venv/bin/pytest tests/unit/api/test_resolve_route.py -v
```
Expected: fails — `sentinel.api.routes.resolve` does not exist.

- [ ] **Step 3: Implement the route**

Create `sentinel/api/routes/resolve.py`:

```python
# sentinel/api/routes/resolve.py
"""POST /incidents/{incident_id}/resolve — persist resolution + status + outbox event."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import ResolutionRepository
from sentinel.schemas.api import ResolveIncidentRequest, ResolveIncidentResponse

router = APIRouter(tags=["incidents"])


@router.post(
    "/incidents/{incident_id}/resolve",
    response_model=ResolveIncidentResponse,
    status_code=200,
    responses={
        404: {"description": "Incident not found"},
        409: {"description": "Incident already resolved"},
        422: {"description": "Invalid payload"},
    },
)
async def resolve_incident(
    incident_id: UUID,
    body: ResolveIncidentRequest,
    request: Request,
) -> ResolveIncidentResponse:
    repo: ResolutionRepository = request.app.state.resolution_repo
    outbox_topic: str = request.app.state.outbox_topic
    try:
        result = await repo.record(incident_id, body, outbox_topic=outbox_topic)
    except IncidentNotFound as e:
        raise HTTPException(status_code=404, detail="incident_not_found") from e
    except IncidentAlreadyResolved as e:
        raise HTTPException(status_code=409, detail="incident_already_resolved") from e
    return ResolveIncidentResponse(
        incident_id=result.incident_id,
        status="resolved",
        resolved_at=result.resolved_at,
        event_id=result.event_id,
    )
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/unit/api/test_resolve_route.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 6: Stage and pause for review**

```bash
git add sentinel/api/routes/resolve.py tests/unit/api/test_resolve_route.py
git status
```

---

### Task 9: MemoryConsumerDeps + MemoryConsumer

**Files:**
- Create: `sentinel/memory/deps.py`
- Create: `sentinel/memory/consumer.py`
- Create: `tests/unit/memory/test_consumer.py`

- [ ] **Step 1: Create deps**

Create `sentinel/memory/deps.py`:

```python
# sentinel/memory/deps.py
"""Dependency bundle for MemoryConsumer.

Constructed once in app.py lifespan and reused across events.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentinel.enrichment.protocols import EmbeddingProvider
    from sentinel.memory.pipeline import MemoryPipeline
    from sentinel.persistence.repositories import IncidentRepository


@dataclass(frozen=True, slots=True)
class MemoryConsumerDeps:
    incident_repo: IncidentRepository
    embedding_provider: EmbeddingProvider
    pipeline: MemoryPipeline
```

- [ ] **Step 2: Register memory metrics**

Open `sentinel/observability/metrics.py` and add (next to the metrics added in Task 5):

```python
memory_events_consumed_total = Counter(
    "sentinel_memory_events_consumed_total",
    "Memory consumer events consumed, by event type.",
    labelnames=("type",),
)
memory_events_failed_total = Counter(
    "sentinel_memory_events_failed_total",
    "Memory consumer events that failed processing, by reason.",
    labelnames=("reason",),
)
memory_duplicates_total = Counter(
    "sentinel_memory_duplicates_total",
    "Memory consumer events that were duplicates of an already-processed event_id.",
)
memory_invalid_events_total = Counter(
    "sentinel_memory_invalid_events_total",
    "Memory consumer events whose envelope failed schema validation (poison pills).",
)
memory_embedding_duration_seconds = Histogram(
    "sentinel_memory_embedding_duration_seconds",
    "Wall time of text→vector embedding in the memory consumer, by event type.",
    labelnames=("event_type",),
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
```

Confirm `Counter` is already imported; if not, `from prometheus_client import Counter` (or extend).

- [ ] **Step 3: Write failing consumer tests**

Create `tests/unit/memory/test_consumer.py`:

```python
# tests/unit/memory/test_consumer.py
"""MemoryConsumer.handle_message — envelope validation, branching, idempotency, errors."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from sentinel.memory.consumer import MemoryConsumer
from sentinel.memory.deps import MemoryConsumerDeps
from sentinel.memory.pipeline import MemoryPipeline
from sentinel.persistence.repositories import MemoryIncidentRow, ResolutionData

pytestmark = pytest.mark.asyncio


def _msg(payload: dict[str, Any] | None, *, raw: bytes | None = None) -> Any:
    if raw is not None:
        value = raw
    elif payload is not None:
        value = json.dumps(payload).encode()
    else:
        value = b""
    return SimpleNamespace(value=value, offset=0, partition=0)


def _make_consumer(
    *,
    incident_repo: Any = None,
    embedder: Any = None,
    pipeline: MemoryPipeline | None = None,
) -> tuple[MemoryConsumer, Any]:
    kafka = MagicMock()
    kafka.commit = AsyncMock()
    deps = MemoryConsumerDeps(
        incident_repo=incident_repo or MagicMock(),
        embedding_provider=embedder or MagicMock(),
        pipeline=pipeline or MemoryPipeline(),
    )
    return MemoryConsumer(consumer=kafka, deps=deps), kafka


async def test_invalid_envelope_commits_offset_as_poison_pill() -> None:
    consumer, kafka = _make_consumer()
    await consumer.handle_message(_msg(None, raw=b"not json"))
    kafka.commit.assert_awaited_once()


async def test_unknown_event_type_commits_offset() -> None:
    consumer, kafka = _make_consumer()
    await consumer.handle_message(_msg({
        "event_id": str(uuid4()),
        "event": "some.other.event",
        "incident_id": str(uuid4()),
        "ts": "2026-05-19T16:00:00+00:00",
    }))
    kafka.commit.assert_awaited_once()


async def test_opened_event_writes_initial_embedding() -> None:
    incident_id = uuid4()
    event_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=MemoryIncidentRow(
        id=incident_id, service="api", title="db timeout", status="open", resolution=None,
    ))
    incident_repo.set_embedding = AsyncMock(return_value="written")
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.1] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(_msg({
        "event_id": str(event_id),
        "event": "incident.opened",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    embedder.embed.assert_awaited_once_with("api db timeout")
    incident_repo.set_embedding.assert_awaited_once()
    kafka.commit.assert_awaited_once()


async def test_resolved_event_writes_resolved_embedding() -> None:
    incident_id = uuid4()
    event_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=MemoryIncidentRow(
        id=incident_id, service="api", title="db timeout", status="resolved",
        resolution=ResolutionData(
            root_cause="pool exhausted", remediation="raised pool", diagnosis_was_correct=True,
        ),
    ))
    incident_repo.set_embedding = AsyncMock(return_value="written")
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.2] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(_msg({
        "event_id": str(event_id),
        "event": "incident.resolved",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    embedder.embed.assert_awaited_once_with("db timeout\npool exhausted\nraised pool")
    incident_repo.set_embedding.assert_awaited_once()
    kafka.commit.assert_awaited_once()


async def test_resolved_event_without_resolution_row_commits() -> None:
    incident_id = uuid4()
    event_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=MemoryIncidentRow(
        id=incident_id, service="api", title="t", status="resolved", resolution=None,
    ))
    incident_repo.set_embedding = AsyncMock()
    embedder = MagicMock()
    embedder.embed = AsyncMock()

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(_msg({
        "event_id": str(event_id),
        "event": "incident.resolved",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    embedder.embed.assert_not_awaited()
    incident_repo.set_embedding.assert_not_awaited()
    kafka.commit.assert_awaited_once()


async def test_unknown_incident_commits_offset() -> None:
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=None)
    embedder = MagicMock()
    embedder.embed = AsyncMock()

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(_msg({
        "event_id": str(uuid4()),
        "event": "incident.opened",
        "incident_id": str(uuid4()),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    embedder.embed.assert_not_awaited()
    kafka.commit.assert_awaited_once()


async def test_duplicate_event_id_commits_offset() -> None:
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=MemoryIncidentRow(
        id=incident_id, service="a", title="b", status="open", resolution=None,
    ))
    incident_repo.set_embedding = AsyncMock(return_value="duplicate")
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.0] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(_msg({
        "event_id": str(uuid4()),
        "event": "incident.opened",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    kafka.commit.assert_awaited_once()


async def test_embedding_timeout_commits_as_poison_pill() -> None:
    import asyncio
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=MemoryIncidentRow(
        id=incident_id, service="a", title="b", status="open", resolution=None,
    ))
    incident_repo.set_embedding = AsyncMock()
    embedder = MagicMock()
    embedder.embed = AsyncMock(side_effect=asyncio.TimeoutError())

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(_msg({
        "event_id": str(uuid4()),
        "event": "incident.opened",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    incident_repo.set_embedding.assert_not_awaited()
    kafka.commit.assert_awaited_once()


async def test_db_write_failure_does_not_commit() -> None:
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=MemoryIncidentRow(
        id=incident_id, service="a", title="b", status="open", resolution=None,
    ))
    incident_repo.set_embedding = AsyncMock(side_effect=RuntimeError("db down"))
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.0] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    with pytest.raises(RuntimeError):
        await consumer.handle_message(_msg({
            "event_id": str(uuid4()),
            "event": "incident.opened",
            "incident_id": str(incident_id),
            "ts": "2026-05-19T16:00:00+00:00",
        }))

    kafka.commit.assert_not_awaited()
```

- [ ] **Step 4: Run failing tests**

```bash
.venv/bin/pytest tests/unit/memory/test_consumer.py -v
```
Expected: fails — `sentinel.memory.consumer` does not exist.

- [ ] **Step 5: Implement MemoryConsumer**

Create `sentinel/memory/consumer.py`:

```python
# sentinel/memory/consumer.py
"""Kafka consumer for incident.opened / incident.resolved → embeddings.

Mirrors EnrichmentConsumer/DiagnosisConsumer:
- subscribes to the single sentinel.incidents topic with a distinct group_id
- enable_auto_commit=False; manual commit only after successful handle_message
- at-least-once delivery; per-event idempotency via event_id
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import BaseModel, ConfigDict, ValidationError

from sentinel.memory.deps import MemoryConsumerDeps
from sentinel.observability.metrics import (
    memory_duplicates_total,
    memory_embedding_duration_seconds,
    memory_events_consumed_total,
    memory_events_failed_total,
    memory_invalid_events_total,
)

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, ConsumerRecord


_LOG = logging.getLogger("sentinel.memory.consumer")

_EVENTS_OF_INTEREST = frozenset({"incident.opened", "incident.resolved"})


class _EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="ignore")
    event_id: UUID
    event: str
    incident_id: UUID


class MemoryConsumer:
    def __init__(
        self,
        *,
        consumer: AIOKafkaConsumer,
        deps: MemoryConsumerDeps,
    ) -> None:
        self._consumer = consumer
        self._deps = deps
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        async for msg in self._consumer:
            if self._stop.is_set():
                break
            try:
                await self.handle_message(msg)
            except Exception:
                _LOG.exception(
                    "memory_event_failed",
                    extra={"offset": msg.offset, "partition": msg.partition},
                )
                # Do NOT commit; redelivery via at-least-once.
                continue
            await self._consumer.commit()

    async def handle_message(self, msg: ConsumerRecord) -> None:
        # 1. Parse + validate envelope.
        try:
            payload = json.loads(msg.value)
            envelope = _EventEnvelope.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as e:
            _LOG.error(
                "memory_invalid_envelope",
                extra={"offset": msg.offset, "err": repr(e)},
            )
            memory_invalid_events_total.inc()
            await self._consumer.commit()
            return

        # 2. Branch on event type.
        if envelope.event not in _EVENTS_OF_INTEREST:
            _LOG.debug(
                "memory_skip_event_type",
                extra={"event": envelope.event, "incident_id": str(envelope.incident_id)},
            )
            await self._consumer.commit()
            return

        memory_events_consumed_total.labels(type=envelope.event).inc()

        # 3. Load incident (+ optional resolution).
        row = await self._deps.incident_repo.load_for_memory(envelope.incident_id)
        if row is None:
            _LOG.warning(
                "memory_unknown_incident",
                extra={"incident_id": str(envelope.incident_id)},
            )
            memory_events_failed_total.labels(reason="unknown_incident").inc()
            await self._consumer.commit()
            return

        # 4. Compose text per event type.
        if envelope.event == "incident.opened":
            text = self._deps.pipeline.compose_initial(row)
        else:  # incident.resolved
            if row.resolution is None:
                _LOG.warning(
                    "memory_resolution_missing",
                    extra={"incident_id": str(envelope.incident_id)},
                )
                memory_events_failed_total.labels(reason="resolution_missing").inc()
                await self._consumer.commit()
                return
            text = self._deps.pipeline.compose_resolved(row, row.resolution)

        # 5. Embed (timeout → poison pill).
        start = time.monotonic()
        try:
            vector = await self._deps.embedding_provider.embed(text)
        except asyncio.TimeoutError:
            _LOG.warning(
                "embedding_compute_timeout",
                extra={"len": len(text), "incident_id": str(envelope.incident_id)},
            )
            memory_events_failed_total.labels(reason="embedding_timeout").inc()
            await self._consumer.commit()
            return
        finally:
            memory_embedding_duration_seconds.labels(event_type=envelope.event).observe(
                time.monotonic() - start
            )

        # 6. Idempotent write. Repository raises on infra failure → caller's
        #    run() loop catches, does NOT commit, redelivery handles it.
        result = await self._deps.incident_repo.set_embedding(
            envelope.incident_id, vector, event_id=envelope.event_id
        )
        if result == "written":
            _LOG.info(
                "memory_written",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "event_id": str(envelope.event_id),
                    "event_type": envelope.event,
                },
            )
        elif result == "duplicate":
            _LOG.info(
                "memory_duplicate",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "event_id": str(envelope.event_id),
                },
            )
            memory_duplicates_total.inc()
        else:  # unknown_incident
            _LOG.warning(
                "memory_set_embedding_unknown_incident",
                extra={"incident_id": str(envelope.incident_id)},
            )
            memory_events_failed_total.labels(reason="unknown_incident").inc()

        await self._consumer.commit()
```

- [ ] **Step 6: Update `sentinel/memory/__init__.py`**

Append imports and update `__all__`:

```python
from sentinel.memory.consumer import MemoryConsumer
from sentinel.memory.deps import MemoryConsumerDeps

__all__ = [
    "FastEmbedProvider",
    "MemoryConsumer",
    "MemoryConsumerDeps",
    "MemoryPipeline",
    "PgVectorIncidentStore",
]
```

- [ ] **Step 7: Run consumer tests**

```bash
.venv/bin/pytest tests/unit/memory/test_consumer.py -v
```
Expected: 9 passed. If `db_write_failure_does_not_commit` fails because `handle_message` swallows the exception, adjust: `handle_message` must re-raise so the test's `with pytest.raises(RuntimeError)` catches it. (The `run()` loop catches above `handle_message`; `handle_message` itself propagates.)

- [ ] **Step 8: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 9: Stage and pause for review**

```bash
git add sentinel/memory/deps.py sentinel/memory/consumer.py \
    sentinel/memory/__init__.py sentinel/observability/metrics.py \
    tests/unit/memory/test_consumer.py
git status
```

---

### Task 10: Settings + `app.py` lifespan wiring

**Files:**
- Modify: `sentinel/config/settings.py`
- Modify: `.env.example`
- Modify: `sentinel/api/app.py`

- [ ] **Step 1: Add settings**

Open `sentinel/config/settings.py`. After the existing `kafka_consumer_group_diagnoser` line in the Settings class, add:

```python
    # Memory & embeddings (Work Area H)
    embedding_model_name: str = "BAAI/bge-large-en-v1.5"
    embedding_model_cache_dir: str = "/var/cache/fastembed"
    embedding_compute_timeout_seconds: float = 5.0
    kafka_consumer_group_memory: str = "sentinel-memory"
    memory_consumer_enabled: bool = True
```

- [ ] **Step 2: Update `.env.example`**

Open `.env.example` (or the project's actual `.env.example` location — confirm via `ls .env.example` first). Add:

```bash
# Memory & embeddings (Work Area H)
SENTINEL_EMBEDDING_MODEL_NAME=BAAI/bge-large-en-v1.5
SENTINEL_EMBEDDING_MODEL_CACHE_DIR=/var/cache/fastembed
SENTINEL_EMBEDDING_COMPUTE_TIMEOUT_SECONDS=5.0
SENTINEL_KAFKA_CONSUMER_GROUP_MEMORY=sentinel-memory
SENTINEL_MEMORY_CONSUMER_ENABLED=true
```

If `.env.example` does not exist, skip this step and note the gap for follow-up. (The settings have sensible defaults — the app boots without the env vars.)

- [ ] **Step 3: Update `sentinel/api/app.py` — imports**

Open `sentinel/api/app.py`. In the imports near the top, add:

```python
from pathlib import Path

from sentinel.memory import (
    FastEmbedProvider,
    MemoryConsumer,
    MemoryConsumerDeps,
    MemoryPipeline,
    PgVectorIncidentStore,
)
from sentinel.persistence.repositories import PostgresResolutionRepository
from sentinel.api.routes.resolve import router as resolve_router
```

(Confirm `Path` isn't already imported.)

- [ ] **Step 4: Wire `resolution_repo` + `outbox_topic` on `app.state` (unconditional)**

In the `lifespan` async generator, after the existing `outbox_repo` construction (~line 63), add:

```python
    # Resolve route deps (independent of memory_consumer_enabled).
    resolution_repo = PostgresResolutionRepository(session_factory)
    app.state.resolution_repo = resolution_repo
    app.state.outbox_topic = settings.kafka_topic_incidents
```

- [ ] **Step 5: Construct memory pieces conditionally + replace `NotConfiguredSimilarIncidents`**

Locate the existing `enrich_deps = EnrichmentDeps(...)` block (~lines 83-92). *Before* it, add:

```python
    # ---- Memory (Phase 4) --------------------------------------------------
    memory_consumer: MemoryConsumer | None = None
    memory_task: asyncio.Task[None] | None = None
    mem_kafka_consumer: AIOKafkaConsumer | None = None
    similar_incidents_store = NotConfiguredSimilarIncidents()

    if settings.memory_consumer_enabled:
        embedding_provider = FastEmbedProvider(
            model_cache_dir=Path(settings.embedding_model_cache_dir),
            compute_timeout_s=settings.embedding_compute_timeout_seconds,
        )

        similar_incidents_store = PgVectorIncidentStore(
            session_factory=session_factory,
            embedding_provider=embedding_provider,
        )

        mem_kafka_consumer = AIOKafkaConsumer(
            settings.kafka_topic_incidents,
            bootstrap_servers=settings.kafka_brokers,
            group_id=settings.kafka_consumer_group_memory,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await mem_kafka_consumer.start()

        memory_deps = MemoryConsumerDeps(
            incident_repo=incident_repo,
            embedding_provider=embedding_provider,
            pipeline=MemoryPipeline(),
        )
        memory_consumer = MemoryConsumer(consumer=mem_kafka_consumer, deps=memory_deps)
        memory_task = asyncio.create_task(memory_consumer.run(), name="memory-consumer")
        app.state.memory_consumer = memory_consumer
```

Now modify the existing `enrich_deps` construction. Find the line:

```python
        similar_incidents=NotConfiguredSimilarIncidents(),
```

Replace with:

```python
        similar_incidents=similar_incidents_store,
```

- [ ] **Step 6: Add shutdown sequence for the memory consumer**

In the `finally:` block of `lifespan`, locate the existing diagnoser shutdown block (`if diagnoser is not None and diagnosis_task is not None and diag_kafka_consumer is not None:`). After that block, before the enricher shutdown, add:

```python
        if memory_consumer is not None and memory_task is not None and mem_kafka_consumer is not None:
            memory_consumer.stop()
            try:
                await asyncio.wait_for(memory_task, timeout=5.0)
            except TimeoutError:
                memory_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await memory_task
            with contextlib.suppress(Exception):
                await mem_kafka_consumer.stop()
```

(Memory consumer stops *after* diagnoser but *before* enricher — they are all independent Kafka consumers and the order is mostly cosmetic.)

- [ ] **Step 7: Register the resolve router in `build_app`**

In `build_app()`, after `app.include_router(webhooks_router)`, add:

```python
    app.include_router(resolve_router)
```

- [ ] **Step 8: Run the existing test suite**

```bash
make test
```
Expected: all unit tests pass. The new lifespan code isn't unit-tested directly — it's covered by integration in Task 11.

- [ ] **Step 9: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 10: Stage and pause for review**

```bash
git add sentinel/config/settings.py .env.example sentinel/api/app.py
git status
```

If `.env.example` doesn't exist, omit it from the `git add`.

---

### Task 11: End-to-end integration tests

**Files:**
- Create: `tests/integration/memory/test_memory_e2e.py`
- Create: `tests/integration/memory/test_resolve_route_e2e.py`

- [ ] **Step 1: Write E2E memory tests**

Create `tests/integration/memory/test_memory_e2e.py`:

```python
# tests/integration/memory/test_memory_e2e.py
"""End-to-end memory loop: events → consumer → embeddings → retrieval.

Exercises the full MemoryConsumer + PgVectorIncidentStore + repo path against
a real Postgres. Does NOT spin up Kafka — invokes consumer.handle_message
directly with fabricated ConsumerRecord-shaped messages (same pattern as
existing diagnosis/enrichment consumer integration tests).
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text

from sentinel.memory.consumer import MemoryConsumer
from sentinel.memory.deps import MemoryConsumerDeps
from sentinel.memory.embeddings import FastEmbedProvider
from sentinel.memory.pipeline import MemoryPipeline
from sentinel.memory.store import PgVectorIncidentStore
from sentinel.persistence.repositories import PostgresIncidentRepository

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture
def embedder(tmp_path: Path) -> FastEmbedProvider:
    return FastEmbedProvider(model_cache_dir=tmp_path / "fastembed-cache")


def _msg(payload: dict[str, Any]) -> Any:
    return SimpleNamespace(value=json.dumps(payload).encode(), offset=0, partition=0)


async def _insert_open_incident(session_factory, *, title: str = "db timeout") -> str:  # type: ignore[no-untyped-def]
    async with session_factory() as s:
        rid = (await s.execute(
            text(
                "INSERT INTO incidents (external_id, source, service, severity, title, "
                "fingerprint, raw_payload, status) "
                "VALUES (:eid, 'generic', 'api', 'SEV3', :title, :fp, "
                "        CAST('{}' AS jsonb), 'open') RETURNING id"
            ),
            {"eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}", "title": title},
        )).scalar_one()
        await s.commit()
    return str(rid)


async def test_opened_event_writes_initial_embedding(session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_open_incident(session_factory)
    repo = PostgresIncidentRepository(session_factory)
    deps = MemoryConsumerDeps(
        incident_repo=repo, embedding_provider=embedder, pipeline=MemoryPipeline(),
    )
    kafka = AsyncMock()
    consumer = MemoryConsumer(consumer=kafka, deps=deps)

    event_id = uuid4()
    await consumer.handle_message(_msg({
        "event_id": str(event_id),
        "event": "incident.opened",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    async with session_factory() as s:
        row = (await s.execute(
            text("SELECT embedding IS NOT NULL, last_embedding_event_id "
                 "FROM incidents WHERE id = :id"),
            {"id": incident_id},
        )).first()
    assert row[0] is True
    assert str(row[1]) == str(event_id)
    kafka.commit.assert_awaited_once()


async def test_resolved_event_updates_embedding_distinct_from_initial(
    session_factory, embedder,  # type: ignore[no-untyped-def]
) -> None:
    """Spec acceptance: resolving updates the embedding; the new embedding
    has different cosine distance to a query than the old embedding."""
    incident_id = await _insert_open_incident(session_factory, title="db timeout")
    repo = PostgresIncidentRepository(session_factory)
    deps = MemoryConsumerDeps(
        incident_repo=repo, embedding_provider=embedder, pipeline=MemoryPipeline(),
    )
    kafka = AsyncMock()
    consumer = MemoryConsumer(consumer=kafka, deps=deps)

    # 1. Initial opened-path embedding.
    await consumer.handle_message(_msg({
        "event_id": str(uuid4()),
        "event": "incident.opened",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    # Capture the initial vector.
    async with session_factory() as s:
        initial_vec_str = (await s.execute(
            text("SELECT embedding::text FROM incidents WHERE id = :id"),
            {"id": incident_id},
        )).scalar_one()

    # 2. Add a resolution row + emit resolved event.
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category) "
                "VALUES (:id, 'pool exhausted under burst load', "
                "        'raised pool size; added circuit breaker on upstream', 'capacity')"
            ),
            {"id": incident_id},
        )
        await s.execute(
            text("UPDATE incidents SET status='resolved' WHERE id = :id"),
            {"id": incident_id},
        )
        await s.commit()

    await consumer.handle_message(_msg({
        "event_id": str(uuid4()),
        "event": "incident.resolved",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }))

    # Capture the resolved-path vector.
    async with session_factory() as s:
        resolved_vec_str = (await s.execute(
            text("SELECT embedding::text FROM incidents WHERE id = :id"),
            {"id": incident_id},
        )).scalar_one()

    assert initial_vec_str != resolved_vec_str


async def test_replay_resolved_event_is_idempotent(session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    incident_id = await _insert_open_incident(session_factory)
    async with session_factory() as s:
        await s.execute(
            text(
                "INSERT INTO resolutions (incident_id, root_cause, remediation, category) "
                "VALUES (:id, 'rc', 'rm', 'data')"
            ),
            {"id": incident_id},
        )
        await s.execute(
            text("UPDATE incidents SET status='resolved' WHERE id = :id"),
            {"id": incident_id},
        )
        await s.commit()

    repo = PostgresIncidentRepository(session_factory)
    deps = MemoryConsumerDeps(
        incident_repo=repo, embedding_provider=embedder, pipeline=MemoryPipeline(),
    )
    kafka = AsyncMock()
    consumer = MemoryConsumer(consumer=kafka, deps=deps)

    event_id = uuid4()
    payload = {
        "event_id": str(event_id),
        "event": "incident.resolved",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }
    await consumer.handle_message(_msg(payload))
    await consumer.handle_message(_msg(payload))  # replay

    assert kafka.commit.await_count == 2


async def test_top_k_uses_hnsw_index_when_data_present(session_factory, embedder) -> None:  # type: ignore[no-untyped-def]
    """Sanity: with a meaningful row count, the planner uses idx_incidents_embedding."""
    # Seed 200 resolved incidents with embeddings.
    for i in range(200):
        async with session_factory() as s:
            rid = (await s.execute(
                text(
                    "INSERT INTO incidents (external_id, source, service, severity, title, "
                    "fingerprint, raw_payload, status, embedding) "
                    "VALUES (:eid, 'generic', 'api', 'SEV3', :title, :fp, "
                    "        CAST('{}' AS jsonb), 'resolved', CAST(:vec AS vector)) "
                    "RETURNING id"
                ),
                {
                    "eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}",
                    "title": f"incident {i}",
                    "vec": await embedder.embed(f"description {i}"),
                },
            )).scalar_one()
            await s.execute(
                text(
                    "INSERT INTO resolutions (incident_id, root_cause, remediation, category) "
                    "VALUES (:id, :rc, 'fixed', 'data')"
                ),
                {"id": rid, "rc": f"root cause {i}"},
            )
            await s.commit()

    qvec = await embedder.embed("anything")
    async with session_factory() as s:
        plan_rows = (await s.execute(
            text(
                "EXPLAIN SELECT id FROM incidents "
                "WHERE embedding IS NOT NULL "
                "ORDER BY embedding <=> CAST(:vec AS vector) LIMIT 5"
            ),
            {"vec": qvec},
        )).all()
    plan_text = "\n".join(str(r[0]) for r in plan_rows)
    # HNSW shows up as "Index Scan using idx_incidents_embedding".
    # On extremely cold tables planner may still seqscan; on 200 rows it should
    # use the index. If this is flaky on first runs, increase the seed count.
    assert "idx_incidents_embedding" in plan_text, plan_text
```

- [ ] **Step 2: Write E2E resolve route tests**

Create `tests/integration/memory/test_resolve_route_e2e.py`:

```python
# tests/integration/memory/test_resolve_route_e2e.py
"""POST /incidents/{id}/resolve against a real app + Postgres."""

from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import text

from sentinel.api.routes.resolve import router as resolve_router
from sentinel.persistence.repositories import PostgresResolutionRepository

pytestmark = pytest.mark.integration


@pytest.fixture()
def app_client(session_factory):  # type: ignore[no-untyped-def]
    """Minimal FastAPI app with the resolve route wired to real Postgres.

    We do NOT use sentinel.api.app.build_app() — its lifespan boots Kafka
    consumers, Anthropic client, Redis, etc. For this route-level E2E we
    only need: resolution_repo on app.state, outbox_topic on app.state,
    and the resolve router registered.
    """
    app = FastAPI()
    app.state.resolution_repo = PostgresResolutionRepository(session_factory)
    app.state.outbox_topic = "sentinel.incidents"
    app.include_router(resolve_router)
    with TestClient(app) as c:
        yield c


def _insert_open_incident_sync(session_factory) -> str:  # type: ignore[no-untyped-def]
    """Synchronous helper — wraps the async insert. TestClient is sync, so
    test bodies stay sync too; we use asyncio.run for DB setup.
    """
    import asyncio

    async def _go() -> str:
        async with session_factory() as s:
            rid = (await s.execute(
                text(
                    "INSERT INTO incidents (external_id, source, service, severity, title, "
                    "fingerprint, raw_payload, status) "
                    "VALUES (:eid, 'generic', 'api', 'SEV3', 't', :fp, "
                    "        CAST('{}' AS jsonb), 'open') RETURNING id"
                ),
                {"eid": f"e-{uuid4()}", "fp": f"fp-{uuid4()}"},
            )).scalar_one()
            await s.commit()
        return str(rid)

    return asyncio.run(_go())


def _body() -> dict:
    return {
        "root_cause": "rc",
        "remediation": "rm",
        "category": "data",
        "diagnosis_was_correct": True,
    }


def test_resolve_success_then_409_on_replay(app_client: TestClient, session_factory) -> None:  # type: ignore[no-untyped-def]
    import asyncio
    incident_id = _insert_open_incident_sync(session_factory)
    r1 = app_client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert r1.status_code == 200, r1.text
    data = r1.json()
    assert data["status"] == "resolved"
    assert data["incident_id"] == incident_id

    async def _check_outbox() -> str:
        async with session_factory() as s:
            return (await s.execute(
                text("SELECT payload->>'event' FROM outbox_events "
                     "WHERE payload->>'incident_id' = :id"),
                {"id": incident_id},
            )).scalar_one()

    assert asyncio.run(_check_outbox()) == "incident.resolved"

    # Second call returns 409.
    r2 = app_client.post(f"/incidents/{incident_id}/resolve", json=_body())
    assert r2.status_code == 409


def test_resolve_404_for_missing_incident(app_client: TestClient) -> None:
    r = app_client.post(f"/incidents/{uuid4()}/resolve", json=_body())
    assert r.status_code == 404
```

Sync test bodies (no `@pytest.mark.asyncio`) because `TestClient` is sync. Async DB setup uses `asyncio.run` inside helpers — matches `_reset_state`'s pattern in the conftest.

- [ ] **Step 3: Run E2E tests**

```bash
.venv/bin/pytest tests/integration/memory/ -v -m integration
```
Expected: all tests pass. The HNSW EXPLAIN test (`test_top_k_uses_hnsw_index_when_data_present`) is the slowest because it seeds 200 incidents — expect ~30s. If flaky, increase seed count to 500.

- [ ] **Step 4: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 5: Stage and pause for review**

```bash
git add tests/integration/memory/test_memory_e2e.py \
    tests/integration/memory/test_resolve_route_e2e.py
git status
```

---

### Task 12: Dockerfile pre-download

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1: Inspect existing Dockerfile**

Run:
```bash
cat Dockerfile
```
Identify where Python deps are installed (usually a `RUN pip install ...` step). The model pre-download should come after deps install.

- [ ] **Step 2: Add model pre-download step**

After the `RUN pip install ...` (or `RUN make bootstrap`) step, add:

```dockerfile
# Pre-download fastembed model so first container start is offline-fast.
ENV SENTINEL_EMBEDDING_MODEL_CACHE_DIR=/var/cache/fastembed
RUN python -c "from fastembed import TextEmbedding; \
               TextEmbedding(model_name='BAAI/bge-large-en-v1.5', cache_dir='/var/cache/fastembed')"
```

- [ ] **Step 3: Build the image to validate**

Run:
```bash
docker build -t sentinel:test .
```
Expected: succeeds. Image build time increases by ~30s (model download). Final image gains ~400MB.

- [ ] **Step 4: Verify model is cached**

Run:
```bash
docker run --rm sentinel:test ls /var/cache/fastembed
```
Expected: lists model files (varies — usually a directory tree with ONNX file).

- [ ] **Step 5: Smoke-test the image runs**

Run:
```bash
docker run --rm sentinel:test python -c \
  "from fastembed import TextEmbedding; \
   m = TextEmbedding('BAAI/bge-large-en-v1.5', cache_dir='/var/cache/fastembed'); \
   print(len(list(m.embed(['ok']))[0]))"
```
Expected: prints `1024` (no network access needed).

- [ ] **Step 6: Stage and pause for review**

```bash
git add Dockerfile
git status
```

---

### Task 13: ADRs

**Files:**
- Create: `docs/adr/0005-embedding-on-resolve.md`
- Create: `docs/adr/0006-local-embeddings.md`

- [ ] **Step 1: Write ADR 0005**

Create `docs/adr/0005-embedding-on-resolve.md`:

```markdown
# 0005 — Embedding on resolve, not on open

**Status:** Accepted
**Date:** 2026-05-19

## Context

Sentinel's similar-incident retrieval depends on a `vector(1024)` embedding stored on `incidents.embedding`. Two natural moments to compute or update that vector:

1. **At open** — embed the alert title (`service + title`) so the row is searchable from the moment it lands.
2. **At resolve** — recompute from `title + root_cause + remediation` once the operator has captured what actually happened.

We do both. This ADR records *why*, and which alternatives we rejected.

## Decision

- **At open**, MemoryConsumer computes `embed(f"{service} {title}")` and writes it. This makes the row immediately retrievable — useful while it's still being diagnosed.
- **At resolve**, MemoryConsumer re-computes `embed(f"{title}\n{root_cause}\n{remediation}")` and overwrites the column. This is the "gets smarter from use" loop: the embedding now reflects the *actual cause*, not the surface symptom.

Both writes go through `IncidentRepository.set_embedding`, which is idempotent on `last_embedding_event_id`.

## Why

The opened-path embedding is the fallback while we wait for human truth. The resolved-path embedding is the actual learning signal. A query like *"db pool exhaustion"* against a corpus of resolved incidents matches *because* their embeddings encode the words *pool exhaustion* from root cause, not just whatever surface symptom the alert title said (often misleading: "high latency", "5xx surge").

The retrieval filter (`status IN ('resolved','closed') AND diagnosis_was_correct IS NOT FALSE`) means only post-resolve embeddings actually get retrieved. The opened-path embedding is essentially insurance against the case where we want to query the open corpus later.

## Alternatives rejected

- **Embed only on resolve.** Loses retrievability of in-flight incidents. We do not currently need that, but the cost of also embedding on open is one extra ~100ms compute per incident, paid asynchronously off the request path.
- **Re-embed on every status change.** Wasteful — only `resolved` carries new information.
- **Embed at diagnosis time** (e.g., embed the hypothesis). Diagnoses are model-generated; embedding them feeds the model's interpretation back into retrieval, amplifying its biases. We embed only operator-confirmed text.
- **Persist multiple embedding versions per incident.** Adds complexity for unclear benefit at V1. The single-row pattern is sufficient and aligned with the spec.

## Consequences

- A resolved incident takes one extra MemoryConsumer event to become discoverable in its post-resolve form.
- If the consumer is stalled, retrieval still works but with stale embeddings. The outbox + at-least-once guarantee means no permanent loss.
- ADR 0006 (local embeddings) makes the compute essentially free, so embedding on both events has negligible cost.
```

- [ ] **Step 2: Write ADR 0006**

Create `docs/adr/0006-local-embeddings.md`:

```markdown
# 0006 — Local embeddings via fastembed (bge-large-en-v1.5)

**Status:** Accepted
**Date:** 2026-05-19

## Context

Sentinel needs a 1024-dim text embedding provider for the memory & retrieval loop (Work Area H). The spec (§H deliverables) suggests Voyage AI, Anthropic, or OpenAI. Anthropic does not currently expose an embeddings endpoint. Voyage AI and OpenAI both require paid API keys.

This repo is a public portfolio. Reviewers must be able to clone it, run `docker compose up`, and see the full system work — including similar-incident retrieval — without provisioning any third-party API credentials.

## Decision

Use the `fastembed` Python library with the `BAAI/bge-large-en-v1.5` model. Properties:

- **Local** — runs ONNX-format model in-process on CPU. No network calls at inference time.
- **High quality** — MTEB average ~64 on retrieval tasks, comparable to or better than OpenAI's `text-embedding-3-small` for English technical text.
- **1024-dim** — matches the migration-0005 column shape.
- **No torch dependency** — fastembed pulls only `onnxruntime` (~50MB), keeping install footprint reasonable.

The provider lives behind the existing `EmbeddingProvider` Protocol in `sentinel/enrichment/protocols.py`; a future remote-provider impl is a one-file change.

## Consequences

### Positive

- Zero API keys required for the public demo.
- Zero per-call cost; evals can run nightly in CI without budget concerns.
- Deterministic output (fixed model, no temperature) — reproducible retrieval rankings.
- No network dependency on a third-party LLM provider for embedding compute.

### Negative

- ~500MB install footprint (fastembed + onnxruntime + ~400MB model weights). Mitigated by pre-downloading the model into the Docker image (`Dockerfile` step in Task 12) so cold-start is offline-fast.
- ~80-150ms CPU inference per call. Acceptable because the embedding runs off the request path (`MemoryConsumer` after Kafka, not in the resolve handler).
- bge-large-en-v1.5 produces 1024-dim vectors, not the spec's originally-aspirational 1536. Migration 0005 alters the existing `Vector(1536)` columns (which were empty) to `Vector(1024)`.

### Migration story to a remote provider

If we later want to swap to a paid embedding provider (e.g., OpenAI `text-embedding-3-large` for higher quality):

1. Add a new `OpenAIEmbeddings` class implementing the same `EmbeddingProvider` Protocol.
2. Settings switch `embedding_provider` between `"local"` and `"openai"` (a future pluggable layer; not present in V1).
3. If the new provider produces a different dimension, another migration changes the column shape — same drop+recreate pattern as 0005.

The Protocol seam means no other code changes.

## Alternatives rejected

- **OpenAI `text-embedding-3-small`** (1536-dim, natively the right shape). Rejected because it requires a paid API key, which contradicts the public-portfolio goal.
- **Voyage AI `voyage-3`** (1024-dim, slightly higher MTEB than bge-large). Rejected for the same reason.
- **`sentence-transformers` library** with the same bge-large model. Rejected because the install footprint is ~2GB (pulls torch). fastembed delivers the same model via ONNX without torch.
- **`bge-small-en-v1.5`** (384-dim, ~130MB model, MTEB ~62). Rejected because bge-large's quality gain is worth the extra ~270MB on a portfolio project. If install footprint becomes a real problem, swapping to bge-small is a one-line change + a column-dim migration.
```

- [ ] **Step 3: Stage and pause for review**

```bash
git add docs/adr/0005-embedding-on-resolve.md docs/adr/0006-local-embeddings.md
git status
```

---

### Task 14: Final code review + handoff

**Files:** (no edits; review only)

- [ ] **Step 1: Run the full test suite**

```bash
make lint
make typecheck
make test
make test-integration
```
Expected: all green. Note any test that needs adjustment.

- [ ] **Step 2: Verify migration round-trips one more time**

```bash
make migrate-down && make migrate
```
Expected: succeeds.

- [ ] **Step 3: Run docker-compose to verify the full stack boots**

```bash
make compose-down
make compose-up
sleep 10
curl -s http://localhost:8000/healthz
```
Expected: `{"status": "ok", ...}` or similar.

If `make compose-up` fails because of the bigger Docker image (fastembed model bundled), confirm the Docker daemon has enough disk space.

- [ ] **Step 4: Smoke-test the resolve loop end-to-end**

Send a webhook:
```bash
curl -X POST http://localhost:8000/webhooks/generic \
  -H 'Content-Type: application/json' \
  -d '{"id":"smoke-1","service":"api","severity":"SEV3","title":"db timeout","raw_payload":{}}'
```

Note the returned `incident_id`. Wait ~3s for the MemoryConsumer to write the initial embedding.

Resolve:
```bash
curl -X POST http://localhost:8000/incidents/<incident_id>/resolve \
  -H 'Content-Type: application/json' \
  -d '{"root_cause":"pool exhausted","remediation":"raised pool","category":"capacity","diagnosis_was_correct":true}'
```
Expected: `200` with `{"status": "resolved", "event_id": "..."}`.

Wait ~3s. Verify in psql that `incidents.embedding` is non-null and `last_embedding_event_id` matches the resolve's `event_id`:

```sql
SELECT id, status, embedding IS NOT NULL, last_embedding_event_id
FROM incidents WHERE id = '<incident_id>';
```

- [ ] **Step 5: Run the code-review subagent (per project policy)**

Invoke `superpowers:requesting-code-review` against the staged changes. Address any findings inline.

- [ ] **Step 6: Hand off to user for commit**

State: "All tasks staged. Ready for the user to commit & push."

---

## Acceptance checklist (against design §8 and spec §H)

- [ ] `make lint`, `make typecheck`, `make test`, `make test-integration` all green.
- [ ] Migration 0005 reversible — round-tripped via `make migrate-down && make migrate`.
- [ ] `incidents.embedding` is `Vector(1024)` after migration; `last_embedding_event_id` column exists with partial index.
- [ ] `runbooks.embedding` is `Vector(1024)` after migration.
- [ ] `FastEmbedProvider.embed` returns a 1024-element list, deterministic, bounded by 5s timeout.
- [ ] `PgVectorIncidentStore.top_k` filters by `status IN ('resolved','closed')` AND `(diagnosis_was_correct IS NULL OR = TRUE)` AND `exclude_incident_id`.
- [ ] `PgVectorIncidentStore` replaces `NotConfiguredSimilarIncidents()` in `app.py` lifespan.
- [ ] `MemoryConsumer` subscribes to `sentinel.incidents` with group `sentinel-memory`, handles both `incident.opened` and `incident.resolved`, ignores other events.
- [ ] `MemoryConsumer.handle_message` is idempotent on `event_id` via `set_embedding`'s conditional UPDATE.
- [ ] `POST /incidents/{id}/resolve` returns 200 on success, 404 on missing, 409 on already-resolved, 422 on invalid payload.
- [ ] Resolve handler persists resolution + status + outbox event atomically (one transaction).
- [ ] Resolve handler returns in <50ms (no embedding compute on request path).
- [ ] Spec acceptance: resolving an incident updates its embedding; the new embedding differs from the initial.
- [ ] Spec acceptance: retrieval ≤1s for 10k incidents (verified by HNSW index plan, not load-tested in this phase).
- [ ] Five new Prometheus metrics registered.
- [ ] ADR 0005 (embedding-on-resolve) and ADR 0006 (local embeddings) committed under `docs/adr/`.
- [ ] Dockerfile pre-downloads the embedding model.
- [ ] Five new settings keys with sensible defaults; `.env.example` updated.
- [ ] `memory_consumer_enabled=False` keeps the existing degraded path intact (integration-test escape hatch).
