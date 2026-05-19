# Phase 4 — Enrichment (Work Area F) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land Work Area F (parallel context assembly) end-to-end: Kafka consumer on `sentinel.incidents` → parallel fetchers with per-fetcher timeouts and circuit breakers → `IncidentContext` assembled with stable IDs → persisted as JSONB on `incidents` → `incident.enriched` event emitted via the existing outbox. Includes three Phase 3 patches the design surfaced as prerequisites (outbox payload carries `event_id`, Kafka producer/consumer carry W3C tracecontext headers, `IncidentContext` schema gains `incident_id`+`assembled_at`).

**Architecture:** New module `sentinel/enrichment/` with `circuit_breaker.py` (asyncio, rolling 5-failure / 60s window, 30s cooldown), `orchestrator.py` (`asyncio.gather` over six fetchers, each wrapped in breaker + `asyncio.wait_for`), `consumer.py` (aiokafka consumer for `sentinel.incidents`), `protocols.py` (interfaces for not-yet-built deps), `defaults.py` (no-op adapters that return `FetcherResult(status="degraded")`), and `fetchers/` (six modules: two real DB-backed, four stubs using defaults). Persistence gets four new columns on `incidents` and two new repository methods. Lifespan starts the consumer alongside the existing OutboxDrainer.

**Tech Stack:** Python 3.12, asyncio, Pydantic v2, SQLAlchemy 2.x async, asyncpg, alembic, aiokafka, OpenTelemetry SDK, structlog (config) + stdlib logging (call sites), testcontainers-postgres/-kafka, pytest, hypothesis (optional).

**Conventions for this repo (do not violate):**
- The user performs all `git commit`/`git push` — Claude must not commit. Tasks end with "stage and pause for review", never with `git commit`. One subagent code review (via `superpowers:requesting-code-review`) runs at the end of the whole plan before the user commits.
- `mypy --strict` and `ruff` are gating; both must be clean before review.
- New env vars go into `Settings`, `.env.example`, and `config/dev.yaml` in the same task that introduces them. F itself adds no new env vars (adapter env vars land later with each integration).
- No `dict[str, Any]` on Pydantic API boundaries. JSONB columns may be `dict[str, Any]` internally; that does not leak into wire types.
- Every external call has a documented timeout. Per-fetcher timeout default 5.0s, configurable per fetcher.
- ADRs go in `docs/adr/`; not required for F.
- Designs and plans go in `plans/`.

---

## File Structure

### Phase 3 patches (land in this PR)

| File | Responsibility |
|---|---|
| `sentinel/schemas/context.py` (modify) | Add `incident_id` (UUID) and `assembled_at` (datetime) to `IncidentContext`. |
| `sentinel/persistence/repositories.py` (modify) | Both `IncidentRepository.ingest()` outbox-payload constructions (opened + recurred paths) include an `event_id` field; `OutboxEventModel.id` is explicitly set in Python (not relying on server default) so the payload can include it. |
| `sentinel/ingestion/kafka_producer.py` (modify) | `KafkaProducer.emit()` gains an optional `headers` parameter; passes through to `send_and_wait`. |
| `sentinel/ingestion/outbox_drainer.py` (modify) | Inject W3C `traceparent` (and `tracestate` if present) into Kafka message headers from the current span context. |
| `sentinel/api/app.py` (modify) | Call `configure_tracing(settings)` in lifespan so spans aren't no-ops. |

### Persistence amendment (Work Area B extension)

| File | Responsibility |
|---|---|
| `migrations/versions/0003_incident_enrichment_context.py` (new) | Add `context_json`, `context_assembled_at`, `context_version`, `last_enrichment_event_id` columns + partial index on `last_enrichment_event_id`. Reversible. |
| `sentinel/persistence/models.py` (modify) | Add the four new columns on `IncidentModel`. |
| `sentinel/persistence/repositories.py` (modify) | Add `StoredEnrichmentContext`, `EnrichmentWriteResult` dataclasses; add `write_enrichment_context()` and `get_enrichment_context()` to `IncidentRepository` Protocol + Postgres impl. |

### Enrichment module (Work Area F)

| File | Responsibility |
|---|---|
| `sentinel/enrichment/__init__.py` | Public exports: `assemble`, `CircuitBreaker`, `CircuitOpenError`, `EnrichmentDeps`, three Protocol classes. |
| `sentinel/enrichment/circuit_breaker.py` | `CircuitBreaker` + `CircuitOpenError`. Deque-backed rolling window, asyncio lock, injectable clock, state-change callback. |
| `sentinel/enrichment/protocols.py` | `EmbeddingProvider`, `SimilarIncidentRetrieval`, `RunbookRetrieval`, `LogSearchAdapter`, `ActiveAlertsAdapter` Protocols. |
| `sentinel/enrichment/defaults.py` | `NotConfigured*` no-op implementations returning `FetcherResult(status="degraded", error="not_configured")`. |
| `sentinel/enrichment/deps.py` | `EnrichmentDeps` dataclass: fetchers tuple, per-fetcher breakers dict, repositories, protocols, settings. |
| `sentinel/enrichment/orchestrator.py` | `Fetcher` Protocol, `_run()` helper, `assemble()` top-level. |
| `sentinel/enrichment/fetchers/__init__.py` | `default_fetchers()` factory returning a tuple of six fetchers. |
| `sentinel/enrichment/fetchers/deploys.py` | Real DB-backed fetcher reading `DeployRepository`. |
| `sentinel/enrichment/fetchers/related_alerts.py` | Real DB-backed fetcher reading recent incidents on same service. |
| `sentinel/enrichment/fetchers/similar_incidents.py` | Uses `SimilarIncidentRetrieval` Protocol (default: degraded). |
| `sentinel/enrichment/fetchers/runbooks.py` | Uses `RunbookRetrieval` Protocol (default: degraded). |
| `sentinel/enrichment/fetchers/recent_logs.py` | Uses `LogSearchAdapter` Protocol (default: degraded). |
| `sentinel/enrichment/fetchers/active_alerts.py` | Uses `ActiveAlertsAdapter` Protocol (default: degraded). |
| `sentinel/enrichment/consumer.py` | `EnrichmentConsumer` aiokafka consumer for `sentinel.incidents`. |

### Observability extension

| File | Responsibility |
|---|---|
| `sentinel/observability/metrics.py` (modify) | Register enrichment metrics (durations, failures, section status, events consumed/failed/duplicates/invalid, circuit-breaker-state gauge). |
| `sentinel/observability/tracing.py` (modify) | Add `span_for_assemble(incident_id)` helper. |

### Persistence repository extensions (for deploys/related_alerts fetchers)

| File | Responsibility |
|---|---|
| `sentinel/persistence/repositories.py` (modify) | `DeployRepository.recent_for_service(service, *, window)` query method; `IncidentRepository.recent_for_service(service, *, window, exclude)` query method. |

### App wiring

| File | Responsibility |
|---|---|
| `sentinel/api/app.py` (modify) | Construct `EnrichmentDeps`, start `EnrichmentConsumer` task in lifespan (alongside OutboxDrainer); orderly shutdown. |
| `sentinel/config/settings.py` (modify) | One new constant: `kafka_consumer_group_enricher` (default `"sentinel-enricher"`). No new env var — derived. |

### Tests

| File | Responsibility |
|---|---|
| `tests/unit/enrichment/__init__.py` | (empty) |
| `tests/unit/enrichment/test_circuit_breaker.py` | State transitions, rolling window pruning (injected clock), half-open single-trial, cancellation does not count. |
| `tests/unit/enrichment/test_orchestrator.py` | Gather coerces exceptions to FetcherResult; slow fetcher does not block others; per-fetcher timeout enforced; breaker integration; IDs round-trip. |
| `tests/unit/enrichment/test_defaults.py` | All `NotConfigured*` return `degraded` with stable error. |
| `tests/unit/enrichment/fetchers/test_deploys.py` | Real fetcher against a fake `DeployRepository`. |
| `tests/unit/enrichment/fetchers/test_related_alerts.py` | Real fetcher against a fake `IncidentRepository`. |
| `tests/unit/enrichment/test_consumer.py` | Envelope validation, duplicate event_id no-op, unknown incident no-op-with-commit, db-failure leaves offset uncommitted (fake aiokafka). |
| `tests/integration/enrichment/__init__.py` | (empty) |
| `tests/integration/enrichment/conftest.py` | Postgres + Kafka testcontainers, migration applied. |
| `tests/integration/enrichment/test_end_to_end.py` | Produce `incident.opened` → consumer runs → `context_json` populated → `incident.enriched` in outbox. Replay → version unchanged. Hanging fetcher → context still persisted within ~6s. |
| `tests/unit/ingestion/test_outbox_event_id.py` | New Phase 3 patch unit test: outbox payload carries `event_id` matching `OutboxEventModel.id`. |
| `tests/unit/ingestion/test_kafka_producer_headers.py` | New Phase 3 patch unit test: `emit(headers=…)` forwards to producer. |

---

## Task 1: Phase 3 patch — `IncidentContext` schema gains `incident_id` + `assembled_at`

**Files:**
- Modify: `sentinel/schemas/context.py:85-96`
- Test: existing tests for `IncidentContext` (if any) + a new `tests/unit/schemas/test_incident_context.py`

- [ ] **Step 1: Add a failing unit test for the new fields**

Create `tests/unit/schemas/test_incident_context.py`:

```python
# tests/unit/schemas/test_incident_context.py
"""IncidentContext requires incident_id and assembled_at."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from sentinel.schemas.context import FetcherResult, IncidentContext


def _empty_results() -> dict[str, FetcherResult]:
    now = datetime.now(UTC)
    empty = FetcherResult(status="ok", data=[], fetched_at=now)
    return {
        "recent_deploys": empty,
        "related_alerts": empty,
        "similar_incidents": empty,
        "runbooks": empty,
        "recent_logs": empty,
        "active_alerts": empty,
    }


def test_incident_context_has_incident_id_and_assembled_at() -> None:
    incident_id = uuid4()
    assembled_at = datetime.now(UTC)
    ctx = IncidentContext(
        incident_id=incident_id,
        assembled_at=assembled_at,
        **_empty_results(),
    )
    assert ctx.incident_id == incident_id
    assert ctx.assembled_at == assembled_at


def test_incident_context_rejects_missing_incident_id() -> None:
    with pytest.raises(ValidationError):
        IncidentContext(assembled_at=datetime.now(UTC), **_empty_results())  # type: ignore[call-arg]
```

- [ ] **Step 2: Run tests to verify failure**

Run: `pytest tests/unit/schemas/test_incident_context.py -v`
Expected: FAIL with `ValidationError` for unexpected/missing fields or `TypeError` for unexpected keyword `incident_id`.

- [ ] **Step 3: Add the two fields to `IncidentContext`**

In `sentinel/schemas/context.py`, modify the `IncidentContext` class to:

```python
class IncidentContext(BaseModel):
    """All six fetcher sections, each as its own FetcherResult."""

    model_config = ConfigDict(frozen=True)

    incident_id: UUID
    assembled_at: datetime

    recent_deploys: FetcherResult[DeployItem]
    related_alerts: FetcherResult[RelatedAlertItem]
    similar_incidents: FetcherResult[SimilarIncidentItem]
    runbooks: FetcherResult[RunbookItem]
    recent_logs: FetcherResult[LogLine]
    active_alerts: FetcherResult[RelatedAlertItem]
```

Add the import at the top of the file (alongside the existing `datetime` import):

```python
from uuid import UUID
```

- [ ] **Step 4: Run tests to verify pass**

Run: `pytest tests/unit/schemas/test_incident_context.py -v`
Expected: PASS.

- [ ] **Step 5: Run full unit suite + typecheck for regressions**

Run:
```bash
pytest tests/unit -q
mypy --strict sentinel
ruff check sentinel tests
```
Expected: all green. If any other test constructs `IncidentContext` with positional args or as a `dict[str, Any]`, fix the call site in the same task.

- [ ] **Step 6: Stage and pause**

Run: `git add sentinel/schemas/context.py tests/unit/schemas/test_incident_context.py`. Do not commit.

---

## Task 2: Phase 3 patch — outbox payload carries `event_id`

**Files:**
- Modify: `sentinel/persistence/repositories.py` (two payload constructions inside `IncidentRepository.ingest()`, around lines 388–400 and 425–435)
- Test: `tests/unit/ingestion/test_outbox_event_id.py` (new)

**Why explicit Python-side UUID:** The Kafka message must carry the `event_id`. `OutboxEventModel.id` is set by `server_default=gen_random_uuid()`, so at row-construction time the Python object doesn't yet know its UUID. We assign it explicitly in Python (`uuid.uuid4()`) and include the same value in the JSON payload — one round-trip, no second write.

- [ ] **Step 1: Add a failing unit test for the opened path**

Create `tests/unit/ingestion/test_outbox_event_id.py`:

```python
# tests/unit/ingestion/test_outbox_event_id.py
"""Outbox payload includes event_id matching the OutboxEventModel.id."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from sentinel.persistence.repositories import PostgresIncidentRepository
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.enums import SeverityType


@pytest.fixture()
def normalized_alert() -> NormalizedAlert:
    return NormalizedAlert(
        source="generic",
        external_id="ext-1",
        service="payments",
        severity=SeverityType.SEV2,
        title="db timeout",
        raw_payload={"hello": "world"},
        received_at=datetime.now(UTC),
    )


class _StubSessionFactory:
    """Captures the OutboxEventModel that would be inserted, no DB."""
    captured: list[object]

    def __init__(self) -> None:
        self.captured = []

    def __call__(self) -> "_StubSession":
        return _StubSession(self.captured)


class _StubSession:
    def __init__(self, captured: list[object]) -> None:
        self._captured = captured

    async def __aenter__(self) -> "_StubSession":
        return self

    async def __aexit__(self, *exc: object) -> None:  # noqa: D401
        return None

    def add(self, obj: object) -> None:
        self._captured.append(obj)

    async def flush(self, *_args: object) -> None: ...
    async def commit(self) -> None: ...
    async def execute(self, *args: object, **kwargs: object) -> object:
        # Stub out the existing dedup-window query: always "no existing".
        from types import SimpleNamespace
        return SimpleNamespace(scalar_one_or_none=lambda: None)


@pytest.mark.asyncio
async def test_outbox_payload_includes_event_id_matching_row_id(
    normalized_alert: NormalizedAlert,
) -> None:
    factory = _StubSessionFactory()
    repo = PostgresIncidentRepository(factory)  # type: ignore[arg-type]
    await repo.ingest(
        normalized_alert,
        fingerprint="fp-1",
        outbox_topic="sentinel.incidents",
        payload_hash="ph-1",
    )
    outbox_row = next(
        c for c in factory.captured
        if type(c).__name__ == "OutboxEventModel"
    )
    payload = outbox_row.payload  # type: ignore[attr-defined]
    row_id = outbox_row.id  # type: ignore[attr-defined]
    assert isinstance(row_id, UUID)
    assert payload["event_id"] == str(row_id)
    assert payload["event"] == "incident.opened"
```

(If `NormalizedAlert`'s actual constructor differs from the stub above, adjust kwargs to match — read `sentinel/schemas/alert.py` first.)

- [ ] **Step 2: Run the test to verify failure**

Run: `pytest tests/unit/ingestion/test_outbox_event_id.py -v`
Expected: FAIL — `payload` has no `event_id` key OR `outbox_row.id` is `None` (server-default).

- [ ] **Step 3: Update the `OutboxEventModel` construction in the recurred path**

In `sentinel/persistence/repositories.py`, locate the recurred-path payload construction (around line 387–397). The file currently has `from uuid import UUID` but does **not** have `import uuid`. Add `import uuid` alongside the existing uuid import at the top of the file. Then modify the OutboxEventModel construction:

```python
            outbox_id = uuid.uuid4()
            now_iso_recurred = now.isoformat()
            outbox_row = OutboxEventModel(
                id=outbox_id,
                topic=outbox_topic,
                key=str(existing.id),
                payload={
                    "event_id": str(outbox_id),
                    "event": "incident.recurred",
                    "incident_id": str(existing.id),
                    "fingerprint": fingerprint,
                    "source": alert.source,
                    "ts": now_iso_recurred,
                },
            )
```

(Use the names that already exist in that scope for `now_iso`/`now`/`existing`. Do not introduce duplicate locals — read the surrounding 10 lines first and reuse what's there. The block above is illustrative.)

- [ ] **Step 4: Update the `OutboxEventModel` construction in the opened path**

In the same file, the opened-path construction (around line 425–435). Apply the same change:

```python
            outbox_id = uuid.uuid4()
            outbox_row = OutboxEventModel(
                id=outbox_id,
                topic=outbox_topic,
                key=str(new_incident.id),
                payload={
                    "event_id": str(outbox_id),
                    "event": "incident.opened",
                    "incident_id": str(new_incident.id),
                    "fingerprint": fingerprint,
                    "source": alert.source,
                    "ts": now_iso,
                },
            )
```

- [ ] **Step 5: Run the test to verify pass**

Run: `pytest tests/unit/ingestion/test_outbox_event_id.py -v`
Expected: PASS.

- [ ] **Step 6: Run existing ingestion tests for regressions**

Run:
```bash
pytest tests/unit/ingestion -q
pytest tests/unit/persistence -q
mypy --strict sentinel
ruff check sentinel tests
```
Expected: all green. Any existing test that asserts on payload keys may need updating to expect `event_id` (additive — should not break existing assertions unless they use `assert payload == {…}` exact-match).

- [ ] **Step 7: Stage and pause**

Run: `git add sentinel/persistence/repositories.py tests/unit/ingestion/test_outbox_event_id.py`. Do not commit.

---

## Task 3: Phase 3 patch — Kafka producer accepts headers; OutboxDrainer injects W3C traceparent

**Files:**
- Modify: `sentinel/ingestion/kafka_producer.py`
- Modify: `sentinel/ingestion/outbox_drainer.py`
- Modify: `sentinel/api/app.py` (call `configure_tracing` in lifespan)
- Test: `tests/unit/ingestion/test_kafka_producer_headers.py` (new)

- [ ] **Step 1: Add a failing unit test for `KafkaProducer.emit(headers=…)`**

Create `tests/unit/ingestion/test_kafka_producer_headers.py`:

```python
# tests/unit/ingestion/test_kafka_producer_headers.py
"""KafkaProducer.emit forwards headers kwarg to aiokafka."""
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from sentinel.ingestion.kafka_producer import KafkaProducer


@pytest.mark.asyncio
async def test_emit_forwards_headers() -> None:
    kp = KafkaProducer(brokers="ignored")
    fake = AsyncMock()
    kp._producer = fake  # type: ignore[attr-defined]
    headers: list[tuple[str, bytes]] = [("traceparent", b"00-abc-def-01")]
    await kp.emit(
        topic="t", key="k", payload={"a": 1}, headers=headers,
    )
    args: tuple[Any, ...] = fake.send_and_wait.call_args.args
    kwargs: dict[str, Any] = fake.send_and_wait.call_args.kwargs
    assert kwargs.get("headers") == headers


@pytest.mark.asyncio
async def test_emit_without_headers_omits_kwarg() -> None:
    kp = KafkaProducer(brokers="ignored")
    fake = AsyncMock()
    kp._producer = fake  # type: ignore[attr-defined]
    await kp.emit(topic="t", key="k", payload={"a": 1})
    kwargs: dict[str, Any] = fake.send_and_wait.call_args.kwargs
    assert "headers" not in kwargs or kwargs["headers"] is None
```

- [ ] **Step 2: Run the test to verify failure**

Run: `pytest tests/unit/ingestion/test_kafka_producer_headers.py -v`
Expected: FAIL — `emit()` does not accept `headers`.

- [ ] **Step 3: Add `headers` parameter to `KafkaProducer.emit`**

In `sentinel/ingestion/kafka_producer.py`, replace the `emit` method:

```python
    async def emit(
        self,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaProducer.start() must be awaited before emit()")
        value = json.dumps(payload).encode()
        kwargs: dict[str, Any] = {"value": value, "key": key.encode()}
        if headers is not None:
            kwargs["headers"] = headers
        await self._producer.send_and_wait(topic, **kwargs)
```

- [ ] **Step 4: Run the producer test to verify pass**

Run: `pytest tests/unit/ingestion/test_kafka_producer_headers.py -v`
Expected: PASS.

- [ ] **Step 5: Make the OutboxDrainer inject `traceparent`**

In `sentinel/ingestion/outbox_drainer.py`, add imports at the top:

```python
from opentelemetry import propagate
```

Inside `_tick`, replace the `await asyncio.wait_for(self._producer.emit(...))` call with one that builds headers from the current OTel context:

```python
                carrier: dict[str, str] = {}
                propagate.inject(carrier)
                headers: list[tuple[str, bytes]] | None = (
                    [(k, v.encode()) for k, v in carrier.items()] if carrier else None
                )
                try:
                    await asyncio.wait_for(
                        self._producer.emit(
                            topic=event.topic,
                            key=event.key,
                            payload=event.payload,
                            headers=headers,
                        ),
                        timeout=self._emit_timeout,
                    )
```

(Leave the surrounding `mark_published`/metric calls and exception handling exactly as they are; only the emit call signature changes.)

- [ ] **Step 6: Wire `configure_tracing` into the app lifespan**

In `sentinel/api/app.py`, add to the imports near the top:

```python
from sentinel.observability.tracing import configure_tracing
```

And as the very first statement inside the `lifespan` function (before any other setup):

```python
    settings = load_settings()
    configure_tracing(settings)
```

(`settings = load_settings()` already exists — keep one assignment; add only the `configure_tracing(settings)` line immediately after it.)

- [ ] **Step 7: Run all ingestion tests for regressions**

Run:
```bash
pytest tests/unit/ingestion -q
mypy --strict sentinel
ruff check sentinel tests
```
Expected: green. If any existing outbox-drainer test stubs `producer.emit` with a strict signature, update the stub to accept `headers`.

- [ ] **Step 8: Stage and pause**

Run:
```bash
git add \
  sentinel/ingestion/kafka_producer.py \
  sentinel/ingestion/outbox_drainer.py \
  sentinel/api/app.py \
  tests/unit/ingestion/test_kafka_producer_headers.py
```
Do not commit.

---

## Task 4: Migration 0003 — incident enrichment context columns

**Files:**
- Create: `migrations/versions/0003_incident_enrichment_context.py`
- Modify: `sentinel/persistence/models.py` (`IncidentModel`)
- Test: round-trip via existing alembic plumbing; new `tests/integration/persistence/test_migration_0003.py`

- [ ] **Step 1: Write the failing integration test**

Create `tests/integration/persistence/test_migration_0003.py`:

```python
# tests/integration/persistence/test_migration_0003.py
"""Migration 0003 adds the four enrichment columns and is reversible."""
from __future__ import annotations

import subprocess
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


pytestmark = pytest.mark.integration


@pytest.fixture()
def alembic_env(pg_dsn: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    yield


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


@pytest.mark.asyncio
async def test_upgrade_adds_columns_then_downgrade_drops_them(
    pg_dsn: str, alembic_env: None,
) -> None:
    _run(["alembic", "upgrade", "0003_incident_enrichment_context"])

    engine = create_async_engine(pg_dsn)
    async with engine.begin() as conn:
        cols = await conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'incidents' AND column_name IN "
            "('context_json','context_assembled_at','context_version','last_enrichment_event_id')"
        ))
        names = {row[0] for row in cols.fetchall()}
    assert names == {
        "context_json", "context_assembled_at",
        "context_version", "last_enrichment_event_id",
    }

    _run(["alembic", "downgrade", "0002_outbox_events"])
    async with engine.begin() as conn:
        cols = await conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'incidents' AND column_name IN "
            "('context_json','context_assembled_at','context_version','last_enrichment_event_id')"
        ))
        names = {row[0] for row in cols.fetchall()}
    assert names == set()
    await engine.dispose()
```

- [ ] **Step 2: Run the test to verify failure**

Run: `pytest tests/integration/persistence/test_migration_0003.py -v -m integration`
Expected: FAIL — migration file does not exist.

- [ ] **Step 3: Create migration 0003**

Create `migrations/versions/0003_incident_enrichment_context.py`:

```python
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
        postgresql_where=text("last_enrichment_event_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_incidents_last_enrichment_event_id", table_name="incidents")
    op.drop_column("incidents", "last_enrichment_event_id")
    op.drop_column("incidents", "context_version")
    op.drop_column("incidents", "context_assembled_at")
    op.drop_column("incidents", "context_json")
```

- [ ] **Step 4: Add the four columns to `IncidentModel`**

In `sentinel/persistence/models.py`, inside `class IncidentModel(Base):`, after the existing `embedding` line, add:

```python
    context_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    context_assembled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    context_version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0"
    )
    last_enrichment_event_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), nullable=True
    )
```

(`models.py` already imports `Integer` and `text` directly from `sqlalchemy` — use them as-is, no aliasing required.)

Add an index entry inside `__table_args__`:

```python
        Index(
            "ix_incidents_last_enrichment_event_id",
            "last_enrichment_event_id",
            postgresql_where=text("last_enrichment_event_id IS NOT NULL"),
        ),
```

- [ ] **Step 5: Run the migration test to verify pass**

Run: `pytest tests/integration/persistence/test_migration_0003.py -v -m integration`
Expected: PASS.

- [ ] **Step 6: Run model regression checks**

Run:
```bash
mypy --strict sentinel
ruff check sentinel tests
pytest tests/unit/persistence -q
```
Expected: green.

- [ ] **Step 7: Stage and pause**

Run:
```bash
git add \
  migrations/versions/0003_incident_enrichment_context.py \
  sentinel/persistence/models.py \
  tests/integration/persistence/test_migration_0003.py
```
Do not commit.

---

## Task 5: Repository — `write_enrichment_context` + `get_enrichment_context`

**Files:**
- Modify: `sentinel/persistence/repositories.py`
- Test: `tests/integration/persistence/test_enrichment_context_repo.py` (new)

- [ ] **Step 1: Write the failing integration tests**

Create `tests/integration/persistence/test_enrichment_context_repo.py`:

```python
# tests/integration/persistence/test_enrichment_context_repo.py
"""IncidentRepository enrichment-context methods."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from sentinel.persistence.repositories import (
    OutboxEvent,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.context import FetcherResult, IncidentContext
from sentinel.schemas.enums import SeverityType


pytestmark = pytest.mark.integration


def _empty_ctx(incident_id, assembled_at):
    empty = FetcherResult(status="ok", data=[], fetched_at=assembled_at)
    return IncidentContext(
        incident_id=incident_id,
        assembled_at=assembled_at,
        recent_deploys=empty,
        related_alerts=empty,
        similar_incidents=empty,
        runbooks=empty,
        recent_logs=empty,
        active_alerts=empty,
    )


@pytest.fixture()
def migrated(pg_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Apply all migrations to the test DB. No project-wide migrate fixture exists yet."""
    import subprocess
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    subprocess.run(["alembic", "upgrade", "head"], check=True)


@pytest.fixture()
async def repo(pg_dsn, migrated):
    engine = create_async_engine(pg_dsn)
    sf = make_session_factory(engine)
    yield PostgresIncidentRepository(sf)
    await engine.dispose()


@pytest.mark.asyncio
async def test_write_then_get_round_trips(repo):
    alert = NormalizedAlert(
        source="generic", external_id="ext-1", service="api",
        severity=SeverityType.SEV2, title="boom",
        raw_payload={}, received_at=datetime.now(UTC),
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-1")

    event_id = uuid4()
    assembled_at = datetime.now(UTC)
    result = await repo.write_enrichment_context(
        incident_id=incident_id,
        event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at,
        outbox_event=None,
    )
    assert result.status == "written"
    assert result.version == 1

    stored = await repo.get_enrichment_context(incident_id)
    assert stored is not None
    assert stored.version == 1
    assert stored.assembled_at == assembled_at


@pytest.mark.asyncio
async def test_duplicate_event_id_is_noop(repo):
    alert = NormalizedAlert(
        source="generic", external_id="ext-dup", service="api",
        severity=SeverityType.SEV2, title="boom",
        raw_payload={}, received_at=datetime.now(UTC),
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-dup")
    event_id = uuid4()
    assembled_at = datetime.now(UTC)
    first = await repo.write_enrichment_context(
        incident_id=incident_id, event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at, outbox_event=None,
    )
    assert first.status == "written"
    second = await repo.write_enrichment_context(
        incident_id=incident_id, event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at, outbox_event=None,
    )
    assert second.status == "duplicate"
    stored = await repo.get_enrichment_context(incident_id)
    assert stored.version == 1


@pytest.mark.asyncio
async def test_outbox_event_staged_in_same_tx(repo):
    alert = NormalizedAlert(
        source="generic", external_id="ext-ob", service="api",
        severity=SeverityType.SEV2, title="boom",
        raw_payload={}, received_at=datetime.now(UTC),
    )
    incident_id = await repo.create_from_alert(alert, fingerprint="fp-ob")
    event_id = uuid4()
    assembled_at = datetime.now(UTC)
    outbox_event = OutboxEvent(
        id=uuid4(),
        topic="sentinel.incidents",
        key=str(incident_id),
        payload={"event": "incident.enriched", "event_id": str(uuid4()),
                 "incident_id": str(incident_id)},
        attempts=0,
        created_at=assembled_at,
    )
    result = await repo.write_enrichment_context(
        incident_id=incident_id, event_id=event_id,
        context=_empty_ctx(incident_id, assembled_at),
        assembled_at=assembled_at, outbox_event=outbox_event,
    )
    assert result.status == "written"
    # outbox row must be queryable
    from sqlalchemy import select
    from sentinel.persistence.models import OutboxEventModel
    async with repo._session_factory() as s:
        rows = (await s.execute(
            select(OutboxEventModel).where(OutboxEventModel.id == outbox_event.id)
        )).scalars().all()
        assert len(rows) == 1
        assert rows[0].payload["event"] == "incident.enriched"
```

(If a `migrate` fixture doesn't exist, copy or create one mirroring the existing pattern from `tests/integration/persistence/conftest.py` — it should `alembic upgrade head` against the test DB.)

- [ ] **Step 2: Run the tests to verify failure**

Run: `pytest tests/integration/persistence/test_enrichment_context_repo.py -v -m integration`
Expected: FAIL — `write_enrichment_context` and `get_enrichment_context` don't exist; `OutboxEvent` may already exist but `outbox_event` kwarg is unknown.

- [ ] **Step 3: Add the dataclasses and Protocol additions**

In `sentinel/persistence/repositories.py`, near the existing `IngestResult` dataclass, add:

```python
@dataclass(frozen=True, slots=True)
class EnrichmentWriteResult:
    status: Literal["written", "duplicate"]
    version: int  # post-write version when written; existing version when duplicate


@dataclass(frozen=True, slots=True)
class StoredEnrichmentContext:
    context: IncidentContext
    assembled_at: datetime
    version: int
    last_event_id: UUID
```

Add to `IncidentRepository` Protocol (around line 242):

```python
    async def write_enrichment_context(
        self,
        *,
        incident_id: UUID,
        event_id: UUID,
        context: IncidentContext,
        assembled_at: datetime,
        outbox_event: OutboxEvent | None = None,
    ) -> EnrichmentWriteResult: ...

    async def get_enrichment_context(
        self,
        incident_id: UUID,
    ) -> StoredEnrichmentContext | None: ...
```

Add the import at top of file if not already present:

```python
from sentinel.schemas.context import IncidentContext
```

- [ ] **Step 4: Implement `write_enrichment_context` on `PostgresIncidentRepository`**

In `PostgresIncidentRepository`, add:

```python
    async def write_enrichment_context(
        self,
        *,
        incident_id: UUID,
        event_id: UUID,
        context: IncidentContext,
        assembled_at: datetime,
        outbox_event: OutboxEvent | None = None,
    ) -> EnrichmentWriteResult:
        ctx_json = context.model_dump(mode="json")
        async with self._session_factory() as s:
            result = await s.execute(
                text(
                    "UPDATE incidents "
                    "SET context_json = :ctx, "
                    "    context_assembled_at = :assembled_at, "
                    "    context_version = context_version + 1, "
                    "    last_enrichment_event_id = :event_id "
                    "WHERE id = :incident_id "
                    "  AND (last_enrichment_event_id IS DISTINCT FROM :event_id) "
                    "RETURNING context_version"
                ),
                {
                    "ctx": ctx_json,
                    "assembled_at": assembled_at,
                    "event_id": event_id,
                    "incident_id": incident_id,
                },
            )
            row = result.first()
            if row is None:
                await s.rollback()
                # Read the current version to report it back.
                existing = await s.execute(
                    text(
                        "SELECT context_version FROM incidents WHERE id = :id"
                    ),
                    {"id": incident_id},
                )
                existing_row = existing.first()
                version = int(existing_row[0]) if existing_row else 0
                return EnrichmentWriteResult(status="duplicate", version=version)

            new_version = int(row[0])
            if outbox_event is not None:
                s.add(OutboxEventModel(
                    id=outbox_event.id,
                    topic=outbox_event.topic,
                    key=outbox_event.key,
                    payload=outbox_event.payload,
                ))
            await s.commit()
            return EnrichmentWriteResult(status="written", version=new_version)
```

(`repositories.py` already imports `text` directly from `sqlalchemy` — no aliasing required. If you prefer SQLAlchemy core `update().returning(...)`, the functional contract is the conditional UPDATE; either form is acceptable as long as the `IS DISTINCT FROM` semantics survive.)

- [ ] **Step 5: Implement `get_enrichment_context`**

Add to `PostgresIncidentRepository`:

```python
    async def get_enrichment_context(
        self,
        incident_id: UUID,
    ) -> StoredEnrichmentContext | None:
        async with self._session_factory() as s:
            row = (await s.execute(
                text(
                    "SELECT context_json, context_assembled_at, "
                    "       context_version, last_enrichment_event_id "
                    "FROM incidents WHERE id = :id"
                ),
                {"id": incident_id},
            )).first()
            if row is None or row[0] is None:
                return None
            ctx_json, assembled_at, version, last_event_id = row
            return StoredEnrichmentContext(
                context=IncidentContext.model_validate(ctx_json),
                assembled_at=assembled_at,
                version=int(version),
                last_event_id=last_event_id,
            )
```

- [ ] **Step 6: Run the tests to verify pass**

Run:
```bash
pytest tests/integration/persistence/test_enrichment_context_repo.py -v -m integration
mypy --strict sentinel
ruff check sentinel tests
```
Expected: green.

- [ ] **Step 7: Stage and pause**

Run:
```bash
git add \
  sentinel/persistence/repositories.py \
  tests/integration/persistence/test_enrichment_context_repo.py
```
Do not commit.

---

## Task 6: CircuitBreaker

**Files:**
- Create: `sentinel/enrichment/__init__.py` (empty initially — exports added later)
- Create: `sentinel/enrichment/circuit_breaker.py`
- Test: `tests/unit/enrichment/__init__.py` (empty), `tests/unit/enrichment/test_circuit_breaker.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/enrichment/__init__.py` (empty file).

Create `tests/unit/enrichment/test_circuit_breaker.py`:

```python
# tests/unit/enrichment/test_circuit_breaker.py
"""CircuitBreaker state machine and rolling-window semantics."""
from __future__ import annotations

import asyncio

import pytest

from sentinel.enrichment.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
)


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0
    def __call__(self) -> float:
        return self.t
    def advance(self, dt: float) -> None:
        self.t += dt


@pytest.mark.asyncio
async def test_starts_closed() -> None:
    cb = CircuitBreaker("x", time_fn=_Clock())
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_opens_after_five_failures_in_window() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=5, window_s=60.0, time_fn=clock)
    async def boom() -> None:
        raise RuntimeError("boom")
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
        clock.advance(1.0)
    assert cb.state == "open"


@pytest.mark.asyncio
async def test_does_not_open_when_failures_age_out() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=5, window_s=60.0, time_fn=clock)
    async def boom() -> None:
        raise RuntimeError("boom")
    for _ in range(4):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
        clock.advance(20.0)  # spread 4 failures across 60s
    # 5th failure at t=80; first failure at t=0 is now > window_s old → pruned.
    with pytest.raises(RuntimeError):
        await cb.call(boom)
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_open_state_short_circuits() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, cooldown_s=30.0, time_fn=clock)
    async def boom() -> None:
        raise RuntimeError("boom")
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    assert cb.state == "open"
    with pytest.raises(CircuitOpenError):
        await cb.call(boom)


@pytest.mark.asyncio
async def test_half_open_after_cooldown_then_close_on_success() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, cooldown_s=30.0, time_fn=clock)
    async def boom() -> None:
        raise RuntimeError("boom")
    async def ok() -> int:
        return 1
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    assert cb.state == "open"
    clock.advance(31.0)
    # First call enters half-open and runs the trial.
    assert await cb.call(ok) == 1
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_half_open_failure_reopens() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, cooldown_s=30.0, time_fn=clock)
    async def boom() -> None:
        raise RuntimeError("boom")
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    clock.advance(31.0)
    with pytest.raises(RuntimeError):
        await cb.call(boom)
    assert cb.state == "open"


@pytest.mark.asyncio
async def test_cancellation_does_not_count_as_failure() -> None:
    clock = _Clock()
    cb = CircuitBreaker("x", threshold=2, window_s=60.0, time_fn=clock)
    async def cancelled() -> None:
        raise asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await cb.call(cancelled)
    with pytest.raises(asyncio.CancelledError):
        await cb.call(cancelled)
    assert cb.state == "closed"


@pytest.mark.asyncio
async def test_state_change_callback_fires_on_transitions() -> None:
    clock = _Clock()
    events: list[tuple[str, str]] = []
    cb = CircuitBreaker(
        "x", threshold=2, window_s=60.0, cooldown_s=30.0,
        time_fn=clock, on_state_change=lambda old, new: events.append((old, new)),
    )
    async def boom() -> None:
        raise RuntimeError("boom")
    async def ok() -> int:
        return 1
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await cb.call(boom)
    assert events[-1] == ("closed", "open")
    clock.advance(31.0)
    await cb.call(ok)
    # Two transitions: open → half_open (on trial start) and half_open → closed (on success).
    assert events[-2] == ("open", "half_open")
    assert events[-1] == ("half_open", "closed")
```

- [ ] **Step 2: Run the tests to verify failure**

Run: `pytest tests/unit/enrichment/test_circuit_breaker.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Implement the breaker**

Create `sentinel/enrichment/__init__.py` (empty for now; exports added in the last enrichment task).

Create `sentinel/enrichment/circuit_breaker.py`:

```python
# sentinel/enrichment/circuit_breaker.py
"""Per-fetcher circuit breaker.

State machine: closed → open (after `threshold` failures within `window_s`)
→ half_open (after `cooldown_s` from opening) → closed (on a successful
trial) or back to open (on a failed trial).

Failures use a rolling time-deque, not a counter — counter-only fails the
"rolling" requirement; see program-of-work F-risks.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Literal, TypeVar

T = TypeVar("T")

State = Literal["closed", "open", "half_open"]


class CircuitOpenError(Exception):
    """Raised when the breaker short-circuits a call."""


class CircuitBreaker:
    def __init__(
        self,
        name: str,
        *,
        threshold: int = 5,
        window_s: float = 60.0,
        cooldown_s: float = 30.0,
        time_fn: Callable[[], float] = time.monotonic,
        on_state_change: Callable[[str, str], None] | None = None,
    ) -> None:
        self._name = name
        self._threshold = threshold
        self._window_s = window_s
        self._cooldown_s = cooldown_s
        self._time = time_fn
        self._on_state_change = on_state_change
        self._failures: deque[float] = deque()
        self._state: State = "closed"
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> State:
        return self._state

    def _transition(self, new: State) -> None:
        old = self._state
        if old == new:
            return
        self._state = new
        if new == "open":
            self._opened_at = self._time()
        elif new == "closed":
            self._failures.clear()
            self._opened_at = None
        if self._on_state_change is not None:
            self._on_state_change(old, new)

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._failures and self._failures[0] <= cutoff:
            self._failures.popleft()

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        async with self._lock:
            now = self._time()
            if self._state == "open":
                assert self._opened_at is not None
                if now - self._opened_at >= self._cooldown_s:
                    self._transition("half_open")
                else:
                    raise CircuitOpenError(f"breaker {self._name} open")
            # In closed or half_open we permit the call. half_open allows only one
            # in-flight call; the outer lock is released during fn(), so we use a
            # state flag rather than holding the lock across the await.
        try:
            result = await fn()
        except asyncio.CancelledError:
            # Cancellation is not an integration fault; do not record.
            raise
        except BaseException:
            async with self._lock:
                now2 = self._time()
                if self._state == "half_open":
                    # Trial failed → re-open and re-arm cooldown.
                    self._failures.append(now2)
                    self._transition("open")
                else:
                    self._failures.append(now2)
                    self._prune(now2)
                    if len(self._failures) >= self._threshold:
                        self._transition("open")
            raise
        else:
            async with self._lock:
                if self._state == "half_open":
                    self._transition("closed")
            return result
```

- [ ] **Step 4: Run the tests to verify pass**

Run:
```bash
pytest tests/unit/enrichment/test_circuit_breaker.py -v
mypy --strict sentinel/enrichment
ruff check sentinel/enrichment tests/unit/enrichment
```
Expected: all green.

- [ ] **Step 5: Stage and pause**

Run:
```bash
git add \
  sentinel/enrichment/__init__.py \
  sentinel/enrichment/circuit_breaker.py \
  tests/unit/enrichment/__init__.py \
  tests/unit/enrichment/test_circuit_breaker.py
```
Do not commit.

---

## Task 7: Enrichment Protocols + `EnrichmentDeps`

**Files:**
- Create: `sentinel/enrichment/protocols.py`
- Create: `sentinel/enrichment/deps.py`

- [ ] **Step 1: Create Protocols**

Create `sentinel/enrichment/protocols.py`:

```python
# sentinel/enrichment/protocols.py
"""Protocols for enrichment dependencies that may not be built yet.

Real implementations land in:
- memory/ (Work Area H): EmbeddingProvider, SimilarIncidentRetrieval, RunbookRetrieval
- integrations/ (per source): LogSearchAdapter, ActiveAlertsAdapter

Until they're wired in, the default no-op implementations in
sentinel/enrichment/defaults.py supply degraded results.
"""
from __future__ import annotations

from datetime import datetime
from typing import Protocol
from uuid import UUID

from sentinel.schemas.context import (
    FetcherResult,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)


class EmbeddingProvider(Protocol):
    async def embed(self, text: str) -> list[float]: ...


class SimilarIncidentRetrieval(Protocol):
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        exclude_incident_id: UUID | None,
    ) -> FetcherResult[SimilarIncidentItem]: ...


class RunbookRetrieval(Protocol):
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        min_cosine: float,
    ) -> FetcherResult[RunbookItem]: ...


class LogSearchAdapter(Protocol):
    async def recent_errors(
        self,
        *,
        service: str,
        since: datetime,
        limit: int,
    ) -> FetcherResult[LogLine]: ...


class ActiveAlertsAdapter(Protocol):
    async def active_for_service(
        self,
        *,
        service: str,
        exclude_external_id: str,
    ) -> FetcherResult[RelatedAlertItem]: ...
```

- [ ] **Step 2: Create `EnrichmentDeps`**

Create `sentinel/enrichment/deps.py`:

```python
# sentinel/enrichment/deps.py
"""Dependency bundle passed to each fetcher.

Constructed once at app startup in app.py lifespan. Fetchers never
construct DB sessions or HTTP clients; they consume what's in here.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentinel.enrichment.circuit_breaker import CircuitBreaker
    from sentinel.enrichment.orchestrator import Fetcher
    from sentinel.enrichment.protocols import (
        ActiveAlertsAdapter,
        LogSearchAdapter,
        RunbookRetrieval,
        SimilarIncidentRetrieval,
    )
    from sentinel.persistence.repositories import (
        DeployRepository,
        IncidentRepository,
    )


@dataclass(frozen=True, slots=True)
class EnrichmentDeps:
    fetchers: tuple["Fetcher", ...]
    breakers: dict[str, "CircuitBreaker"]
    incident_repo: "IncidentRepository"
    deploy_repo: "DeployRepository"
    similar_incidents: "SimilarIncidentRetrieval"
    runbooks: "RunbookRetrieval"
    log_search: "LogSearchAdapter"
    active_alerts: "ActiveAlertsAdapter"
```

- [ ] **Step 3: Verify it typechecks**

Run:
```bash
mypy --strict sentinel/enrichment
ruff check sentinel/enrichment
```
Expected: green.

- [ ] **Step 4: Stage and pause**

Run:
```bash
git add sentinel/enrichment/protocols.py sentinel/enrichment/deps.py
```
Do not commit.

---

## Task 8: Default no-op adapters

**Files:**
- Create: `sentinel/enrichment/defaults.py`
- Test: `tests/unit/enrichment/test_defaults.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/enrichment/test_defaults.py`:

```python
# tests/unit/enrichment/test_defaults.py
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from sentinel.enrichment.defaults import (
    NotConfiguredActiveAlerts,
    NotConfiguredLogSearch,
    NotConfiguredRunbookRetrieval,
    NotConfiguredSimilarIncidents,
)


@pytest.mark.asyncio
async def test_similar_incidents_default_returns_degraded() -> None:
    r = await NotConfiguredSimilarIncidents().top_k(
        query_text="x", k=5, exclude_incident_id=uuid4(),
    )
    assert r.status == "degraded"
    assert r.data == []
    assert r.error == "not_configured"


@pytest.mark.asyncio
async def test_runbooks_default_returns_degraded() -> None:
    r = await NotConfiguredRunbookRetrieval().top_k(
        query_text="x", k=3, min_cosine=0.6,
    )
    assert r.status == "degraded"
    assert r.error == "not_configured"


@pytest.mark.asyncio
async def test_log_search_default_returns_degraded() -> None:
    r = await NotConfiguredLogSearch().recent_errors(
        service="api", since=datetime.now(UTC), limit=50,
    )
    assert r.status == "degraded"
    assert r.error == "not_configured"


@pytest.mark.asyncio
async def test_active_alerts_default_returns_degraded() -> None:
    r = await NotConfiguredActiveAlerts().active_for_service(
        service="api", exclude_external_id="ext-1",
    )
    assert r.status == "degraded"
    assert r.error == "not_configured"
```

- [ ] **Step 2: Run the tests to verify failure**

Run: `pytest tests/unit/enrichment/test_defaults.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement defaults**

Create `sentinel/enrichment/defaults.py`:

```python
# sentinel/enrichment/defaults.py
"""No-op adapters that return degraded FetcherResults.

Returned when a real implementation (e.g. pgvector from Work Area H, or a
log adapter) is not yet wired. The pipeline always assembles a context;
sections without an implementation are explicitly degraded rather than
silently missing.
"""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sentinel.schemas.context import (
    FetcherResult,
    LogLine,
    RelatedAlertItem,
    RunbookItem,
    SimilarIncidentItem,
)

_NOT_CONFIGURED = "not_configured"


class NotConfiguredSimilarIncidents:
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        exclude_incident_id: UUID | None,
    ) -> FetcherResult[SimilarIncidentItem]:
        return FetcherResult(
            status="degraded", data=[],
            error=_NOT_CONFIGURED, fetched_at=datetime.now(UTC),
        )


class NotConfiguredRunbookRetrieval:
    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        min_cosine: float,
    ) -> FetcherResult[RunbookItem]:
        return FetcherResult(
            status="degraded", data=[],
            error=_NOT_CONFIGURED, fetched_at=datetime.now(UTC),
        )


class NotConfiguredLogSearch:
    async def recent_errors(
        self,
        *,
        service: str,
        since: datetime,
        limit: int,
    ) -> FetcherResult[LogLine]:
        return FetcherResult(
            status="degraded", data=[],
            error=_NOT_CONFIGURED, fetched_at=datetime.now(UTC),
        )


class NotConfiguredActiveAlerts:
    async def active_for_service(
        self,
        *,
        service: str,
        exclude_external_id: str,
    ) -> FetcherResult[RelatedAlertItem]:
        return FetcherResult(
            status="degraded", data=[],
            error=_NOT_CONFIGURED, fetched_at=datetime.now(UTC),
        )
```

- [ ] **Step 4: Run tests to verify pass**

Run:
```bash
pytest tests/unit/enrichment/test_defaults.py -v
mypy --strict sentinel/enrichment
ruff check sentinel/enrichment tests/unit/enrichment
```
Expected: green.

- [ ] **Step 5: Stage and pause**

Run:
```bash
git add sentinel/enrichment/defaults.py tests/unit/enrichment/test_defaults.py
```
Do not commit.

---

## Task 9: Fetcher Protocol + orchestrator

**Files:**
- Create: `sentinel/enrichment/orchestrator.py`
- Modify: `sentinel/observability/metrics.py` (register new metrics)
- Test: `tests/unit/enrichment/test_orchestrator.py`

- [ ] **Step 1: Register enrichment metrics**

`sentinel/observability/metrics.py` already defines `enrichment_duration_seconds`, `enrichment_failures_total`, and `circuit_breaker_state` (created via `_safe_histogram` / `_safe_counter` / `_safe_gauge` helpers — these tolerate re-registration on reload). F adds six new metrics using the same helpers. Append:

```python
enrichment_assemble_duration_seconds = _safe_histogram(
    "sentinel_enrichment_assemble_duration_seconds",
    "End-to-end assemble() duration in seconds.",
    [],
)
enrichment_section_status_total = _safe_counter(
    "sentinel_enrichment_section_status_total",
    "Per-fetcher section status outcomes.",
    ["fetcher", "status"],
)
enrichment_events_consumed_total = _safe_counter(
    "sentinel_enrichment_events_consumed_total",
    "Kafka events the enricher consumed.",
    ["type"],
)
enrichment_events_failed_total = _safe_counter(
    "sentinel_enrichment_events_failed_total",
    "Kafka events the enricher could not process.",
    ["reason"],
)
enrichment_duplicates_total = _safe_counter(
    "sentinel_enrichment_duplicates_total",
    "Re-delivered events that the enricher idempotently ignored.",
    [],
)
enrichment_invalid_events_total = _safe_counter(
    "sentinel_enrichment_invalid_events_total",
    "Events the enricher rejected as malformed (poison-pill committed).",
    [],
)
```

(Counter with `[]` labelnames is fine for the `_total` ones that don't need labels — call sites use `.inc()` directly without `.labels(...)`. If `_safe_counter` rejects an empty list, drop the `labelnames=` kwarg and adjust the helper call style to match existing zero-label counters in the file.)

- [ ] **Step 2: Write failing tests for the orchestrator**

Create `tests/unit/enrichment/test_orchestrator.py`:

```python
# tests/unit/enrichment/test_orchestrator.py
"""assemble() coerces exceptions, enforces timeouts, runs fetchers in parallel."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from sentinel.enrichment.circuit_breaker import CircuitBreaker
from sentinel.enrichment.orchestrator import Fetcher, assemble
from sentinel.schemas.context import FetcherResult, IncidentContext


@dataclass(frozen=True)
class _Incident:
    id: UUID
    service: str
    external_id: str
    title: str
    severity: str


def _ok_result(name: str) -> FetcherResult:
    return FetcherResult(status="ok", data=[], fetched_at=datetime.now(UTC))


class _OkFetcher:
    timeout_s = 5.0
    def __init__(self, name: str) -> None:
        self.name = name
    async def fetch(self, incident, deps):
        return _ok_result(self.name)


class _RaisingFetcher:
    timeout_s = 5.0
    name = "raises"
    async def fetch(self, incident, deps):
        raise RuntimeError("kaboom")


class _SlowFetcher:
    timeout_s = 0.05
    name = "slow"
    async def fetch(self, incident, deps):
        await asyncio.sleep(0.5)
        return _ok_result(self.name)


class _NonFetcherResultFetcher:
    timeout_s = 5.0
    name = "wrong_return"
    async def fetch(self, incident, deps):
        return "not a FetcherResult"  # type: ignore[return-value]


def _make_deps(fetchers):
    from sentinel.enrichment.deps import EnrichmentDeps
    return EnrichmentDeps(
        fetchers=tuple(fetchers),
        breakers={f.name: CircuitBreaker(f.name) for f in fetchers},
        incident_repo=None,           # type: ignore[arg-type]
        deploy_repo=None,             # type: ignore[arg-type]
        similar_incidents=None,       # type: ignore[arg-type]
        runbooks=None,                # type: ignore[arg-type]
        log_search=None,              # type: ignore[arg-type]
        active_alerts=None,           # type: ignore[arg-type]
    )


def _make_incident():
    return _Incident(
        id=uuid4(), service="api", external_id="ext-1",
        title="boom", severity="SEV2",
    )


@pytest.mark.asyncio
async def test_assemble_returns_incident_context() -> None:
    incident = _make_incident()
    fetchers = [
        _OkFetcher("deploys"),
        _OkFetcher("related_alerts"),
        _OkFetcher("similar_incidents"),
        _OkFetcher("runbooks"),
        _OkFetcher("recent_logs"),
        _OkFetcher("active_alerts"),
    ]
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert isinstance(ctx, IncidentContext)
    assert ctx.incident_id == incident.id
    assert ctx.recent_deploys.status == "ok"


@pytest.mark.asyncio
async def test_raising_fetcher_becomes_failed_result() -> None:
    incident = _make_incident()
    fetchers = [
        _RaisingFetcher(),                  # raises → mapped onto "deploys" by position? no — name match.
        _OkFetcher("related_alerts"),
        _OkFetcher("similar_incidents"),
        _OkFetcher("runbooks"),
        _OkFetcher("recent_logs"),
        _OkFetcher("active_alerts"),
    ]
    # Rename first fetcher to the canonical slot so the orchestrator maps it.
    fetchers[0] = type("F", (), {
        "name": "deploys", "timeout_s": 5.0,
        "fetch": _RaisingFetcher().fetch,
    })()
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert ctx.recent_deploys.status == "failed"
    assert "RuntimeError" in (ctx.recent_deploys.error or "")


@pytest.mark.asyncio
async def test_slow_fetcher_times_out_others_complete() -> None:
    incident = _make_incident()
    slow = _SlowFetcher()
    slow.name = "deploys"  # type: ignore[misc]
    fetchers = [
        slow,
        _OkFetcher("related_alerts"),
        _OkFetcher("similar_incidents"),
        _OkFetcher("runbooks"),
        _OkFetcher("recent_logs"),
        _OkFetcher("active_alerts"),
    ]
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert ctx.recent_deploys.status == "failed"
    assert "timeout" in (ctx.recent_deploys.error or "").lower()
    assert ctx.related_alerts.status == "ok"


@pytest.mark.asyncio
async def test_non_fetcherresult_return_is_coerced_to_failed() -> None:
    incident = _make_incident()
    bad = _NonFetcherResultFetcher()
    bad.name = "deploys"  # type: ignore[misc]
    fetchers = [
        bad,
        _OkFetcher("related_alerts"),
        _OkFetcher("similar_incidents"),
        _OkFetcher("runbooks"),
        _OkFetcher("recent_logs"),
        _OkFetcher("active_alerts"),
    ]
    deps = _make_deps(fetchers)
    ctx = await assemble(incident, deps)
    assert ctx.recent_deploys.status == "failed"
```

- [ ] **Step 3: Run tests to verify failure**

Run: `pytest tests/unit/enrichment/test_orchestrator.py -v`
Expected: FAIL — `orchestrator.assemble` does not exist.

- [ ] **Step 4: Implement the orchestrator**

Create `sentinel/enrichment/orchestrator.py`:

```python
# sentinel/enrichment/orchestrator.py
"""Parallel fetcher orchestrator.

Fetchers run concurrently via asyncio.gather(return_exceptions=True). Each
is wrapped in a per-fetcher CircuitBreaker and an asyncio.wait_for timeout.
Fetcher misbehavior (raising, returning a non-FetcherResult) is coerced to
a FetcherResult(status="failed", ...) — assemble() never raises.

The IncidentContext schema expects six named section fields; fetchers are
mapped onto sections by their .name attribute, not by list position.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any, Protocol

from sentinel.enrichment.circuit_breaker import CircuitBreaker, CircuitOpenError
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.observability.metrics import (
    enrichment_assemble_duration_seconds,
    enrichment_duration_seconds,
    enrichment_failures_total,
    enrichment_section_status_total,
)
from sentinel.schemas.context import FetcherResult, IncidentContext

log = logging.getLogger(__name__)

_SECTION_NAMES = (
    "deploys",
    "related_alerts",
    "similar_incidents",
    "runbooks",
    "recent_logs",
    "active_alerts",
)

_SECTION_TO_FIELD = {
    "deploys": "recent_deploys",
    "related_alerts": "related_alerts",
    "similar_incidents": "similar_incidents",
    "runbooks": "runbooks",
    "recent_logs": "recent_logs",
    "active_alerts": "active_alerts",
}


class Fetcher(Protocol):
    name: str
    timeout_s: float
    async def fetch(self, incident: Any, deps: EnrichmentDeps) -> FetcherResult: ...


def _failed_result(reason: str, error: str) -> FetcherResult:
    return FetcherResult(
        status="failed", data=[], error=error, fetched_at=datetime.now(UTC),
    )


async def _run(
    fetcher: Fetcher, incident: Any, deps: EnrichmentDeps,
) -> FetcherResult:
    started = time.monotonic()
    breaker = deps.breakers.get(fetcher.name)
    try:
        if breaker is None:
            result = await asyncio.wait_for(
                fetcher.fetch(incident, deps), timeout=fetcher.timeout_s,
            )
        else:
            result = await breaker.call(
                lambda: asyncio.wait_for(
                    fetcher.fetch(incident, deps), timeout=fetcher.timeout_s,
                )
            )
    except asyncio.TimeoutError:
        log.warning(
            "fetcher_timed_out",
            extra={"fetcher": fetcher.name, "incident_id": str(incident.id)},
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name, reason="timeout",
        ).inc()
        return _failed_result("timeout", "timeout")
    except CircuitOpenError:
        log.debug(
            "breaker_short_circuit", extra={"fetcher": fetcher.name},
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name, reason="circuit_open",
        ).inc()
        return _failed_result("circuit_open", "circuit_open")
    except Exception as e:  # noqa: BLE001
        log.warning(
            "fetcher_errored",
            extra={
                "fetcher": fetcher.name,
                "incident_id": str(incident.id),
                "exc_type": type(e).__name__,
            },
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name, reason="error",
        ).inc()
        return _failed_result("error", f"{type(e).__name__}: {e}")
    finally:
        enrichment_duration_seconds.labels(fetcher=fetcher.name).observe(
            time.monotonic() - started,
        )

    if not isinstance(result, FetcherResult):
        log.warning(
            "fetcher_returned_non_result",
            extra={"fetcher": fetcher.name, "type": type(result).__name__},
        )
        enrichment_failures_total.labels(
            fetcher=fetcher.name, reason="bad_return",
        ).inc()
        return _failed_result("bad_return", "fetcher returned non-FetcherResult")

    enrichment_section_status_total.labels(
        fetcher=fetcher.name, status=result.status,
    ).inc()
    return result


async def assemble(incident: Any, deps: EnrichmentDeps) -> IncidentContext:
    started = time.monotonic()
    coros = [_run(f, incident, deps) for f in deps.fetchers]
    raw = await asyncio.gather(*coros, return_exceptions=True)

    by_name: dict[str, FetcherResult] = {}
    for fetcher, value in zip(deps.fetchers, raw, strict=True):
        if isinstance(value, FetcherResult):
            by_name[fetcher.name] = value
        elif isinstance(value, BaseException):
            log.warning(
                "fetcher_orchestrator_exception",
                extra={
                    "fetcher": fetcher.name,
                    "exc_type": type(value).__name__,
                },
            )
            by_name[fetcher.name] = _failed_result(
                "orchestrator", f"{type(value).__name__}: {value}",
            )
        else:
            by_name[fetcher.name] = _failed_result(
                "bad_return", "fetcher returned non-FetcherResult",
            )

    # Any required section the deps did not include? Use a placeholder degraded.
    placeholder = FetcherResult(
        status="degraded", data=[], error="not_configured",
        fetched_at=datetime.now(UTC),
    )
    section_results = {
        _SECTION_TO_FIELD[n]: by_name.get(n, placeholder) for n in _SECTION_NAMES
    }
    ctx = IncidentContext(
        incident_id=incident.id,
        assembled_at=datetime.now(UTC),
        **section_results,
    )
    enrichment_assemble_duration_seconds.observe(time.monotonic() - started)
    return ctx
```

- [ ] **Step 5: Run the orchestrator tests**

Run:
```bash
pytest tests/unit/enrichment/test_orchestrator.py -v
mypy --strict sentinel/enrichment
ruff check sentinel/enrichment tests/unit/enrichment
```
Expected: green.

- [ ] **Step 6: Stage and pause**

Run:
```bash
git add \
  sentinel/observability/metrics.py \
  sentinel/enrichment/orchestrator.py \
  tests/unit/enrichment/test_orchestrator.py
```
Do not commit.

---

## Task 10: Fetcher — `deploys` (real, DB-backed)

**Files:**
- Modify: `sentinel/persistence/repositories.py` (add `DeployRepository.recent_for_service`)
- Create: `sentinel/enrichment/fetchers/__init__.py`
- Create: `sentinel/enrichment/fetchers/deploys.py`
- Test: `tests/unit/enrichment/fetchers/__init__.py` (empty), `tests/unit/enrichment/fetchers/test_deploys.py`

- [ ] **Step 1: Write the failing fetcher test**

Create `tests/unit/enrichment/fetchers/__init__.py` (empty).

Create `tests/unit/enrichment/fetchers/test_deploys.py`:

```python
# tests/unit/enrichment/fetchers/test_deploys.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.fetchers.deploys import DeploysFetcher
from sentinel.schemas.context import DeployItem


@dataclass(frozen=True)
class _Incident:
    id: UUID
    service: str


class _FakeDeployRepo:
    def __init__(self, rows: list[DeployItem]) -> None:
        self._rows = rows
        self.called_with: dict | None = None
    async def recent_for_service_window(
        self, *, service: str, since: datetime,
    ) -> list[DeployItem]:
        self.called_with = {"service": service, "since": since}
        return self._rows


def _deps(deploy_repo):
    return EnrichmentDeps(
        fetchers=(),
        breakers={},
        incident_repo=None,            # type: ignore[arg-type]
        deploy_repo=deploy_repo,
        similar_incidents=None,        # type: ignore[arg-type]
        runbooks=None,                 # type: ignore[arg-type]
        log_search=None,               # type: ignore[arg-type]
        active_alerts=None,            # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_deploys_fetcher_returns_ok_with_rows() -> None:
    fixed = datetime(2026, 5, 19, tzinfo=UTC)
    repo = _FakeDeployRepo([
        DeployItem(id="deploy:abc", service="api", sha="abc",
                   pr_number=None, pr_title=None, pr_diff_summary=None,
                   deployed_at=fixed, deployed_by=None),
    ])
    incident = _Incident(id=uuid4(), service="api")
    r = await DeploysFetcher().fetch(incident, _deps(repo))
    assert r.status == "ok"
    assert len(r.data) == 1
    assert r.data[0].sha == "abc"
    assert repo.called_with is not None
    assert repo.called_with["service"] == "api"
    assert datetime.now(UTC) - repo.called_with["since"] > timedelta(hours=23)


@pytest.mark.asyncio
async def test_deploys_fetcher_returns_ok_empty_when_no_rows() -> None:
    repo = _FakeDeployRepo([])
    incident = _Incident(id=uuid4(), service="api")
    r = await DeploysFetcher().fetch(incident, _deps(repo))
    assert r.status == "ok"
    assert r.data == []
```

- [ ] **Step 2: Run tests to verify failure**

Run: `pytest tests/unit/enrichment/fetchers/test_deploys.py -v`
Expected: FAIL — `DeploysFetcher` not defined; `DeployRepository.recent_for_service` not defined.

- [ ] **Step 3: Add `recent_for_service_window` to `DeployRepository`**

`DeployRepository` and `PostgresDeployRepository` both already exist in `sentinel/persistence/repositories.py` (Protocol at ~line 445, concrete at ~line 461). The existing `recent_for_service(service, *, limit=20)` returns `list[DeployRow]` and is consumed by `tests/integration/persistence/test_repositories.py:111` — **do not change its signature**. Add a new method `recent_for_service_window` next to it that takes a `since` datetime and returns `list[DeployItem]` (the schema-level type with the stable `deploy:<sha>` id).

In the `DeployRepository` Protocol, add:

```python
    async def recent_for_service_window(
        self,
        *,
        service: str,
        since: datetime,
    ) -> list[DeployItem]: ...
```

Add the import at the top of the file if not present:

```python
from sentinel.schemas.context import DeployItem
```

In `PostgresDeployRepository`, add the concrete method:

```python
    async def recent_for_service_window(
        self,
        *,
        service: str,
        since: datetime,
    ) -> list[DeployItem]:
        from sentinel.schemas.ids import deploy_id
        async with self._session_factory() as s:
            rows = (await s.execute(
                select(DeployModel)
                .where(DeployModel.service == service, DeployModel.deployed_at >= since)
                .order_by(DeployModel.deployed_at.desc())
            )).scalars().all()
            return [
                DeployItem(
                    id=deploy_id(row.sha),
                    service=row.service,
                    sha=row.sha,
                    pr_number=row.pr_number,
                    pr_title=row.pr_title,
                    pr_diff_summary=row.pr_diff_summary,
                    deployed_at=row.deployed_at,
                    deployed_by=row.deployed_by,
                )
                for row in rows
            ]
```

- [ ] **Step 4: Implement the fetcher**

Create `sentinel/enrichment/fetchers/__init__.py`:

```python
# sentinel/enrichment/fetchers/__init__.py
"""Built-in fetchers + a factory wiring them together with default Protocols."""
from __future__ import annotations

from typing import TYPE_CHECKING

from sentinel.enrichment.fetchers.active_alerts import ActiveAlertsFetcher
from sentinel.enrichment.fetchers.deploys import DeploysFetcher
from sentinel.enrichment.fetchers.recent_logs import RecentLogsFetcher
from sentinel.enrichment.fetchers.related_alerts import RelatedAlertsFetcher
from sentinel.enrichment.fetchers.runbooks import RunbooksFetcher
from sentinel.enrichment.fetchers.similar_incidents import SimilarIncidentsFetcher

if TYPE_CHECKING:
    from sentinel.enrichment.orchestrator import Fetcher


def default_fetchers() -> tuple["Fetcher", ...]:
    return (
        DeploysFetcher(),
        RelatedAlertsFetcher(),
        SimilarIncidentsFetcher(),
        RunbooksFetcher(),
        RecentLogsFetcher(),
        ActiveAlertsFetcher(),
    )
```

Create `sentinel/enrichment/fetchers/deploys.py`:

```python
# sentinel/enrichment/fetchers/deploys.py
"""Recent deploys on the incident's service in the last 24h."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import DeployItem, FetcherResult


class DeploysFetcher:
    name = "deploys"
    timeout_s = 5.0

    async def fetch(
        self, incident: Any, deps: EnrichmentDeps,
    ) -> FetcherResult[DeployItem]:
        since = datetime.now(UTC) - timedelta(hours=24)
        rows = await deps.deploy_repo.recent_for_service_window(
            service=incident.service, since=since,
        )
        return FetcherResult(
            status="ok", data=list(rows), fetched_at=datetime.now(UTC),
        )
```

- [ ] **Step 5: Run tests to verify pass**

Run:
```bash
pytest tests/unit/enrichment/fetchers/test_deploys.py -v
mypy --strict sentinel
ruff check sentinel tests
```
Expected: green. (The mypy run may complain about `PostgresDeployRepository` if it didn't previously exist — make sure the concrete repo from Step 3 is well-formed.)

- [ ] **Step 6: Stage and pause**

Run:
```bash
git add \
  sentinel/persistence/repositories.py \
  sentinel/enrichment/fetchers/__init__.py \
  sentinel/enrichment/fetchers/deploys.py \
  tests/unit/enrichment/fetchers/__init__.py \
  tests/unit/enrichment/fetchers/test_deploys.py
```
Do not commit.

---

## Task 11: Fetcher — `related_alerts` (real, DB-backed)

**Files:**
- Modify: `sentinel/persistence/repositories.py` (`IncidentRepository.recent_for_service`)
- Create: `sentinel/enrichment/fetchers/related_alerts.py`
- Test: `tests/unit/enrichment/fetchers/test_related_alerts.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/enrichment/fetchers/test_related_alerts.py`:

```python
# tests/unit/enrichment/fetchers/test_related_alerts.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.fetchers.related_alerts import RelatedAlertsFetcher
from sentinel.schemas.context import RelatedAlertItem


@dataclass(frozen=True)
class _Incident:
    id: UUID
    service: str


class _FakeIncidentRepo:
    def __init__(self, rows: list[RelatedAlertItem]) -> None:
        self._rows = rows
        self.called_with: dict | None = None
    async def recent_for_service(
        self,
        *,
        service: str,
        since: datetime,
        exclude_incident_id: UUID,
    ) -> list[RelatedAlertItem]:
        self.called_with = {
            "service": service, "since": since,
            "exclude_incident_id": exclude_incident_id,
        }
        return self._rows


def _deps(repo):
    return EnrichmentDeps(
        fetchers=(),
        breakers={},
        incident_repo=repo,
        deploy_repo=None,             # type: ignore[arg-type]
        similar_incidents=None,       # type: ignore[arg-type]
        runbooks=None,                # type: ignore[arg-type]
        log_search=None,              # type: ignore[arg-type]
        active_alerts=None,           # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_related_alerts_returns_ok_excluding_self() -> None:
    me = uuid4()
    items = [RelatedAlertItem(
        id=f"related:{uuid4()}", service="api", severity="SEV2",
        title="boom", opened_at=datetime.now(UTC),
    )]
    repo = _FakeIncidentRepo(items)
    r = await RelatedAlertsFetcher().fetch(
        _Incident(id=me, service="api"), _deps(repo),
    )
    assert r.status == "ok"
    assert repo.called_with is not None
    assert repo.called_with["exclude_incident_id"] == me
    assert datetime.now(UTC) - repo.called_with["since"] > timedelta(hours=23)
```

- [ ] **Step 2: Run test to verify failure**

Run: `pytest tests/unit/enrichment/fetchers/test_related_alerts.py -v`
Expected: FAIL — `RelatedAlertsFetcher` and `IncidentRepository.recent_for_service` don't exist.

- [ ] **Step 3: Add `IncidentRepository.recent_for_service`**

`IncidentRepository` Protocol does not currently have any `recent_for_service` method (existing methods are `create_from_alert`, `get`, `list_recent`, `mark_diagnosing`, `ingest`). Add to `IncidentRepository` Protocol in `sentinel/persistence/repositories.py`:

```python
    async def recent_for_service(
        self,
        *,
        service: str,
        since: datetime,
        exclude_incident_id: UUID,
    ) -> list[RelatedAlertItem]: ...
```

And on `PostgresIncidentRepository`:

```python
    async def recent_for_service(
        self,
        *,
        service: str,
        since: datetime,
        exclude_incident_id: UUID,
    ) -> list[RelatedAlertItem]:
        from sentinel.schemas.ids import related_id
        async with self._session_factory() as s:
            rows = (await s.execute(
                select(IncidentModel).where(
                    IncidentModel.service == service,
                    IncidentModel.opened_at >= since,
                    IncidentModel.id != exclude_incident_id,
                ).order_by(IncidentModel.opened_at.desc())
            )).scalars().all()
            return [
                RelatedAlertItem(
                    id=related_id(row.id),
                    service=row.service,
                    severity=row.severity,  # SeverityType — cast if mypy complains
                    title=row.title,
                    opened_at=row.opened_at,
                )
                for row in rows
            ]
```

Add import: `from sentinel.schemas.context import RelatedAlertItem` if not present.

- [ ] **Step 4: Implement the fetcher**

Create `sentinel/enrichment/fetchers/related_alerts.py`:

```python
# sentinel/enrichment/fetchers/related_alerts.py
"""Other incidents on the same service in the last 24h, excluding this one."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, RelatedAlertItem


class RelatedAlertsFetcher:
    name = "related_alerts"
    timeout_s = 5.0

    async def fetch(
        self, incident: Any, deps: EnrichmentDeps,
    ) -> FetcherResult[RelatedAlertItem]:
        since = datetime.now(UTC) - timedelta(hours=24)
        rows = await deps.incident_repo.recent_for_service(
            service=incident.service,
            since=since,
            exclude_incident_id=incident.id,
        )
        return FetcherResult(
            status="ok", data=list(rows), fetched_at=datetime.now(UTC),
        )
```

- [ ] **Step 5: Run tests to verify pass**

Run:
```bash
pytest tests/unit/enrichment/fetchers/test_related_alerts.py -v
mypy --strict sentinel
ruff check sentinel tests
```
Expected: green.

- [ ] **Step 6: Stage and pause**

Run:
```bash
git add \
  sentinel/persistence/repositories.py \
  sentinel/enrichment/fetchers/related_alerts.py \
  tests/unit/enrichment/fetchers/test_related_alerts.py
```
Do not commit.

---

## Task 12: Stub fetchers — `similar_incidents`, `runbooks`, `recent_logs`, `active_alerts`

Each delegates to its respective Protocol on `EnrichmentDeps`. When the deps wire in `NotConfigured*`, the fetcher returns `degraded`. When (in future phases) real implementations replace the defaults, the same fetcher transparently returns real data.

**Files:**
- Create: `sentinel/enrichment/fetchers/similar_incidents.py`
- Create: `sentinel/enrichment/fetchers/runbooks.py`
- Create: `sentinel/enrichment/fetchers/recent_logs.py`
- Create: `sentinel/enrichment/fetchers/active_alerts.py`

- [ ] **Step 1: Implement `similar_incidents`**

Create `sentinel/enrichment/fetchers/similar_incidents.py`:

```python
# sentinel/enrichment/fetchers/similar_incidents.py
from __future__ import annotations

from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, SimilarIncidentItem


class SimilarIncidentsFetcher:
    name = "similar_incidents"
    timeout_s = 5.0

    async def fetch(
        self, incident: Any, deps: EnrichmentDeps,
    ) -> FetcherResult[SimilarIncidentItem]:
        return await deps.similar_incidents.top_k(
            query_text=f"{incident.service} {incident.title}",
            k=5,
            exclude_incident_id=incident.id,
        )
```

- [ ] **Step 2: Implement `runbooks`**

Create `sentinel/enrichment/fetchers/runbooks.py`:

```python
# sentinel/enrichment/fetchers/runbooks.py
from __future__ import annotations

from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, RunbookItem


class RunbooksFetcher:
    name = "runbooks"
    timeout_s = 5.0

    async def fetch(
        self, incident: Any, deps: EnrichmentDeps,
    ) -> FetcherResult[RunbookItem]:
        return await deps.runbooks.top_k(
            query_text=incident.title, k=3, min_cosine=0.6,
        )
```

- [ ] **Step 3: Implement `recent_logs`**

Create `sentinel/enrichment/fetchers/recent_logs.py`:

```python
# sentinel/enrichment/fetchers/recent_logs.py
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, LogLine


class RecentLogsFetcher:
    name = "recent_logs"
    timeout_s = 5.0

    async def fetch(
        self, incident: Any, deps: EnrichmentDeps,
    ) -> FetcherResult[LogLine]:
        since = datetime.now(UTC) - timedelta(minutes=5)
        return await deps.log_search.recent_errors(
            service=incident.service, since=since, limit=50,
        )
```

- [ ] **Step 4: Implement `active_alerts`**

Create `sentinel/enrichment/fetchers/active_alerts.py`:

```python
# sentinel/enrichment/fetchers/active_alerts.py
from __future__ import annotations

from typing import Any

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.schemas.context import FetcherResult, RelatedAlertItem


class ActiveAlertsFetcher:
    name = "active_alerts"
    timeout_s = 5.0

    async def fetch(
        self, incident: Any, deps: EnrichmentDeps,
    ) -> FetcherResult[RelatedAlertItem]:
        return await deps.active_alerts.active_for_service(
            service=incident.service,
            exclude_external_id=incident.external_id,
        )
```

- [ ] **Step 5: Verify typecheck and lint**

Run:
```bash
mypy --strict sentinel/enrichment
ruff check sentinel/enrichment
pytest tests/unit/enrichment -q
```
Expected: green.

- [ ] **Step 6: Stage and pause**

Run:
```bash
git add sentinel/enrichment/fetchers
```
Do not commit.

---

## Task 13: Wire breaker state changes to the metric gauge

The breaker accepts an `on_state_change` callback. We use it to update `sentinel_circuit_breaker_state{integration}`. This is application-level wiring; the breaker itself stays decoupled from prometheus.

**Files:**
- Modify: `sentinel/enrichment/__init__.py` (export public surface)
- Modify: `sentinel/enrichment/orchestrator.py` (no change here — wiring happens in app.py in Task 15)
- Create: `sentinel/enrichment/metrics_wiring.py`

- [ ] **Step 1: Add a helper that builds a breaker with the gauge wired**

Create `sentinel/enrichment/metrics_wiring.py`:

```python
# sentinel/enrichment/metrics_wiring.py
"""Build a CircuitBreaker that updates the Prometheus state gauge.

Keeping this wiring out of CircuitBreaker itself preserves testability:
unit tests can construct breakers without touching prometheus globals.
"""
from __future__ import annotations

from sentinel.enrichment.circuit_breaker import CircuitBreaker
from sentinel.observability.metrics import circuit_breaker_state

_STATE_TO_INT = {"closed": 0, "open": 1, "half_open": 2}


def make_breaker(name: str) -> CircuitBreaker:
    def _on_change(old: str, new: str) -> None:
        circuit_breaker_state.labels(integration=name).set(_STATE_TO_INT[new])
    cb = CircuitBreaker(name, on_state_change=_on_change)
    # Initialize the gauge to "closed" so dashboards don't show a missing series.
    circuit_breaker_state.labels(integration=name).set(_STATE_TO_INT["closed"])
    return cb
```

- [ ] **Step 2: Expose the public surface**

Replace `sentinel/enrichment/__init__.py` contents:

```python
# sentinel/enrichment/__init__.py
"""Enrichment public API."""
from sentinel.enrichment.circuit_breaker import CircuitBreaker, CircuitOpenError
from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.enrichment.fetchers import default_fetchers
from sentinel.enrichment.metrics_wiring import make_breaker
from sentinel.enrichment.orchestrator import assemble
from sentinel.enrichment.protocols import (
    ActiveAlertsAdapter,
    EmbeddingProvider,
    LogSearchAdapter,
    RunbookRetrieval,
    SimilarIncidentRetrieval,
)

__all__ = [
    "ActiveAlertsAdapter",
    "CircuitBreaker",
    "CircuitOpenError",
    "EmbeddingProvider",
    "EnrichmentDeps",
    "LogSearchAdapter",
    "RunbookRetrieval",
    "SimilarIncidentRetrieval",
    "assemble",
    "default_fetchers",
    "make_breaker",
]
```

- [ ] **Step 3: Verify mypy + ruff**

Run:
```bash
mypy --strict sentinel/enrichment
ruff check sentinel/enrichment
```
Expected: green.

- [ ] **Step 4: Stage and pause**

Run:
```bash
git add sentinel/enrichment/metrics_wiring.py sentinel/enrichment/__init__.py
```
Do not commit.

---

## Task 14: Enrichment Kafka consumer

**Files:**
- Create: `sentinel/enrichment/consumer.py`
- Create: `sentinel/schemas/enrichment_event.py` (envelope schema)
- Modify: `sentinel/config/settings.py` (constant for consumer group name)
- Test: `tests/unit/enrichment/test_consumer.py`

- [ ] **Step 1: Define the envelope schema**

Create `sentinel/schemas/enrichment_event.py`:

```python
# sentinel/schemas/enrichment_event.py
"""Kafka envelope the enrichment consumer reads from sentinel.incidents."""
from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class IncidentEvent(BaseModel):
    """One Kafka message on `sentinel.incidents` (post Phase 3 patch)."""

    model_config = ConfigDict(frozen=True, extra="allow")

    event_id: UUID
    event: Literal["incident.opened", "incident.recurred"]
    incident_id: UUID
    fingerprint: str
    source: str
    ts: datetime
```

(The producer also writes `incident.enriched` (Task 14 below) and possibly other events — `Literal` constrains the consumer's accepted set; other event types are filtered at the application layer, not by Pydantic.)

Wait — `Literal` will reject unknown events at parse time. We want graceful skipping, so the consumer must filter on a raw key. Update: change `event` field to `str` and filter inside the consumer.

```python
    event: str
```

- [ ] **Step 2: Add the consumer group constant**

In `sentinel/config/settings.py`, add to the `Settings` class:

```python
    kafka_consumer_group_enricher: str = "sentinel-enricher"
```

(No env var override is required — derived constant. If the file's convention is to add every setting to `.env.example` regardless, follow that.)

- [ ] **Step 3: Write failing tests for the consumer**

Create `tests/unit/enrichment/test_consumer.py`:

```python
# tests/unit/enrichment/test_consumer.py
"""EnrichmentConsumer message handling."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from sentinel.enrichment.consumer import EnrichmentConsumer


@dataclass
class _FakeMsg:
    value: bytes
    offset: int = 0
    partition: int = 0
    key: bytes = b""


def _envelope(event: str = "incident.opened") -> bytes:
    return json.dumps({
        "event_id": str(uuid4()),
        "event": event,
        "incident_id": str(uuid4()),
        "fingerprint": "fp",
        "source": "generic",
        "ts": datetime.now(UTC).isoformat(),
    }).encode()


class _FakeIncidentRepo:
    def __init__(self) -> None:
        self.get_calls = 0
        self.write_calls = 0
        self.write_status = "written"
        self.incident_present = True
    async def get(self, incident_id: UUID) -> Any:
        self.get_calls += 1
        if not self.incident_present:
            return None
        return MagicMock(id=incident_id, service="api", external_id="ext-1",
                        title="boom", severity="SEV2")
    async def write_enrichment_context(self, *, incident_id, event_id, context,
                                       assembled_at, outbox_event=None):
        self.write_calls += 1
        from sentinel.persistence.repositories import EnrichmentWriteResult
        return EnrichmentWriteResult(status=self.write_status, version=1)  # type: ignore[arg-type]


async def _passthrough_assemble(incident, deps):
    from sentinel.schemas.context import FetcherResult, IncidentContext
    empty = FetcherResult(status="ok", data=[], fetched_at=datetime.now(UTC))
    return IncidentContext(
        incident_id=incident.id,
        assembled_at=datetime.now(UTC),
        recent_deploys=empty, related_alerts=empty,
        similar_incidents=empty, runbooks=empty,
        recent_logs=empty, active_alerts=empty,
    )


@pytest.mark.asyncio
async def test_handle_valid_event_writes_context_and_commits() -> None:
    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer, deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher._handle(_FakeMsg(value=_envelope()))
    assert repo.write_calls == 1


@pytest.mark.asyncio
async def test_handle_invalid_envelope_commits_and_counts() -> None:
    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer, deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher._handle(_FakeMsg(value=b"not json"))
    assert repo.write_calls == 0  # never even tried


@pytest.mark.asyncio
async def test_handle_unknown_event_type_skips() -> None:
    repo = _FakeIncidentRepo()
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer, deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher._handle(_FakeMsg(value=_envelope(event="incident.resolved")))
    assert repo.write_calls == 0


@pytest.mark.asyncio
async def test_handle_missing_incident_skips() -> None:
    repo = _FakeIncidentRepo()
    repo.incident_present = False
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer, deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher._handle(_FakeMsg(value=_envelope()))
    assert repo.write_calls == 0


@pytest.mark.asyncio
async def test_handle_duplicate_does_not_emit_outbox() -> None:
    repo = _FakeIncidentRepo()
    repo.write_status = "duplicate"
    consumer = AsyncMock()
    enricher = EnrichmentConsumer(
        consumer=consumer, deps=MagicMock(incident_repo=repo),
        assemble_fn=_passthrough_assemble,
        topic="sentinel.incidents",
    )
    await enricher._handle(_FakeMsg(value=_envelope()))
    assert repo.write_calls == 1
```

- [ ] **Step 4: Run tests to verify failure**

Run: `pytest tests/unit/enrichment/test_consumer.py -v`
Expected: FAIL — `EnrichmentConsumer` module missing.

- [ ] **Step 5: Implement `EnrichmentConsumer`**

Create `sentinel/enrichment/consumer.py`:

```python
# sentinel/enrichment/consumer.py
"""aiokafka consumer that triggers enrichment on incident.opened/recurred.

One process runs one consumer (the OutboxDrainer pattern). Idempotency is
enforced at the DB level via incidents.last_enrichment_event_id — re-delivery
of the same Kafka message is a no-op.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import ValidationError

from sentinel.enrichment.deps import EnrichmentDeps
from sentinel.observability.metrics import (
    enrichment_duplicates_total,
    enrichment_events_consumed_total,
    enrichment_events_failed_total,
    enrichment_invalid_events_total,
)
from sentinel.persistence.repositories import OutboxEvent
from sentinel.schemas.enrichment_event import IncidentEvent

log = logging.getLogger(__name__)

_ACCEPTED_EVENTS = frozenset({"incident.opened", "incident.recurred"})

AssembleFn = Callable[[Any, EnrichmentDeps], Awaitable[Any]]


class EnrichmentConsumer:
    def __init__(
        self,
        *,
        consumer: Any,                # aiokafka.AIOKafkaConsumer | test fake
        deps: EnrichmentDeps,
        assemble_fn: AssembleFn,
        topic: str,
        enriched_topic: str = "sentinel.incidents",
    ) -> None:
        self._consumer = consumer
        self._deps = deps
        self._assemble = assemble_fn
        self._topic = topic
        self._enriched_topic = enriched_topic
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        async for msg in self._consumer:
            if self._stop_event.is_set():
                break
            try:
                await self._handle(msg)
            except Exception:  # noqa: BLE001
                log.exception(
                    "enricher_event_failed",
                    extra={
                        "offset": getattr(msg, "offset", None),
                        "partition": getattr(msg, "partition", None),
                    },
                )
                enrichment_events_failed_total.labels(reason="exception").inc()
                # Do not commit — at-least-once re-delivery.
                continue
            await self._consumer.commit()

    async def _handle(self, msg: Any) -> None:
        try:
            payload = json.loads(msg.value)
            envelope = IncidentEvent.model_validate(payload)
        except (ValueError, ValidationError):
            log.error(
                "enricher_invalid_envelope",
                extra={"offset": getattr(msg, "offset", None)},
            )
            enrichment_invalid_events_total.inc()
            return

        if envelope.event not in _ACCEPTED_EVENTS:
            log.debug(
                "enricher_skip_event_type",
                extra={"event": envelope.event},
            )
            return

        enrichment_events_consumed_total.labels(type=envelope.event).inc()

        incident = await self._deps.incident_repo.get(envelope.incident_id)
        if incident is None:
            log.warning(
                "enricher_unknown_incident",
                extra={"incident_id": str(envelope.incident_id)},
            )
            enrichment_events_failed_total.labels(reason="missing_incident").inc()
            return

        ctx = await self._assemble(incident, self._deps)
        assembled_at = datetime.now(UTC)

        outbox_event_id = uuid4()
        outbox_event = OutboxEvent(
            id=outbox_event_id,
            topic=self._enriched_topic,
            key=str(envelope.incident_id),
            payload={
                "event_id": str(outbox_event_id),
                "event": "incident.enriched",
                "incident_id": str(envelope.incident_id),
                "ts": assembled_at.isoformat(),
            },
            attempts=0,
            created_at=assembled_at,
        )
        result = await self._deps.incident_repo.write_enrichment_context(
            incident_id=envelope.incident_id,
            event_id=envelope.event_id,
            context=ctx,
            assembled_at=assembled_at,
            outbox_event=outbox_event,
        )
        if result.status == "duplicate":
            log.info(
                "enricher_duplicate",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "event_id": str(envelope.event_id),
                },
            )
            enrichment_duplicates_total.inc()
        else:
            log.info(
                "enricher_wrote_context",
                extra={
                    "incident_id": str(envelope.incident_id),
                    "version": result.version,
                },
            )
```

- [ ] **Step 6: Run tests to verify pass**

Run:
```bash
pytest tests/unit/enrichment/test_consumer.py -v
mypy --strict sentinel
ruff check sentinel tests
```
Expected: green.

- [ ] **Step 7: Stage and pause**

Run:
```bash
git add \
  sentinel/schemas/enrichment_event.py \
  sentinel/config/settings.py \
  sentinel/enrichment/consumer.py \
  tests/unit/enrichment/test_consumer.py
```
Do not commit.

---

## Task 15: Wire enrichment into the app lifespan

**Files:**
- Modify: `sentinel/api/app.py`

- [ ] **Step 1: Add the consumer to the lifespan**

In `sentinel/api/app.py`, after the existing OutboxDrainer setup but before the `try: yield` block, add the enrichment wiring. Imports first (at the top of the file, alongside existing imports):

```python
from aiokafka import AIOKafkaConsumer

from sentinel.enrichment import (
    EnrichmentDeps,
    assemble,
    default_fetchers,
    make_breaker,
)
from sentinel.enrichment.consumer import EnrichmentConsumer
from sentinel.enrichment.defaults import (
    NotConfiguredActiveAlerts,
    NotConfiguredLogSearch,
    NotConfiguredRunbookRetrieval,
    NotConfiguredSimilarIncidents,
)
from sentinel.persistence.repositories import PostgresDeployRepository
```

Inside `lifespan`, after the existing `drainer_task = asyncio.create_task(...)` line:

```python
    # ---- Enrichment ----------------------------------------------------
    deploy_repo = PostgresDeployRepository(session_factory)

    fetchers = default_fetchers()
    breakers = {f.name: make_breaker(f.name) for f in fetchers}

    enrich_deps = EnrichmentDeps(
        fetchers=fetchers,
        breakers=breakers,
        incident_repo=incident_repo,
        deploy_repo=deploy_repo,
        similar_incidents=NotConfiguredSimilarIncidents(),
        runbooks=NotConfiguredRunbookRetrieval(),
        log_search=NotConfiguredLogSearch(),
        active_alerts=NotConfiguredActiveAlerts(),
    )

    kafka_consumer = AIOKafkaConsumer(
        settings.kafka_topic_incidents,
        bootstrap_servers=settings.kafka_brokers,
        group_id=settings.kafka_consumer_group_enricher,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await kafka_consumer.start()
    enricher = EnrichmentConsumer(
        consumer=kafka_consumer,
        deps=enrich_deps,
        assemble_fn=assemble,
        topic=settings.kafka_topic_incidents,
        enriched_topic=settings.kafka_topic_incidents,
    )
    enricher_task = asyncio.create_task(enricher.run(), name="enrichment-consumer")
    app.state.enrichment_consumer = enricher
```

Inside the `finally:` block, ensure orderly shutdown — between the drainer stop and the parallel cleanup:

```python
        enricher.stop()
        try:
            await asyncio.wait_for(enricher_task, timeout=5.0)
        except TimeoutError:
            enricher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await enricher_task
        with contextlib.suppress(Exception):
            await kafka_consumer.stop()
```

- [ ] **Step 2: Run unit tests + typecheck**

Run:
```bash
mypy --strict sentinel
ruff check sentinel
pytest tests/unit -q
```
Expected: green. (The lifespan change is exercised by integration tests in the next task.)

- [ ] **Step 3: Stage and pause**

Run: `git add sentinel/api/app.py`. Do not commit.

---

## Task 16: Integration test — end-to-end enrichment

**Files:**
- Create: `tests/integration/enrichment/__init__.py` (empty)
- Create: `tests/integration/enrichment/conftest.py` (testcontainers — copy pattern from `tests/integration/ingestion/conftest.py`)
- Create: `tests/integration/enrichment/test_end_to_end.py`

- [ ] **Step 1: Create the conftest with Postgres + Kafka fixtures**

Create `tests/integration/enrichment/__init__.py` (empty).

Create `tests/integration/enrichment/conftest.py`:

```python
# tests/integration/enrichment/conftest.py
"""Testcontainers fixtures for enrichment end-to-end tests."""
from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator

import pytest
from testcontainers.kafka import KafkaContainer
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


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[KafkaContainer]:
    with KafkaContainer() as kc:
        yield kc


@pytest.fixture()
def kafka_brokers(kafka_container: KafkaContainer) -> str:
    return kafka_container.get_bootstrap_server()


@pytest.fixture()
def migrated_db(pg_dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", pg_dsn)
    subprocess.run(["alembic", "upgrade", "head"], check=True)
```

- [ ] **Step 2: Write failing end-to-end tests**

Create `tests/integration/enrichment/test_end_to_end.py`:

```python
# tests/integration/enrichment/test_end_to_end.py
"""End-to-end: incident.opened on Kafka → context_json on row → incident.enriched in outbox."""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from sentinel.enrichment import (
    EnrichmentDeps,
    assemble,
    default_fetchers,
    make_breaker,
)
from sentinel.enrichment.consumer import EnrichmentConsumer
from sentinel.enrichment.defaults import (
    NotConfiguredActiveAlerts,
    NotConfiguredLogSearch,
    NotConfiguredRunbookRetrieval,
    NotConfiguredSimilarIncidents,
)
from sentinel.persistence.models import IncidentModel, OutboxEventModel
from sentinel.persistence.repositories import (
    PostgresDeployRepository,
    PostgresIncidentRepository,
)
from sentinel.persistence.session import make_session_factory
from sentinel.schemas.alert import NormalizedAlert
from sentinel.schemas.enums import SeverityType

pytestmark = pytest.mark.integration


async def _setup_enricher(pg_dsn: str, kafka_brokers: str, topic: str):
    engine = create_async_engine(pg_dsn)
    sf = make_session_factory(engine)
    incident_repo = PostgresIncidentRepository(sf)
    deploy_repo = PostgresDeployRepository(sf)
    fetchers = default_fetchers()
    deps = EnrichmentDeps(
        fetchers=fetchers,
        breakers={f.name: make_breaker(f.name) for f in fetchers},
        incident_repo=incident_repo,
        deploy_repo=deploy_repo,
        similar_incidents=NotConfiguredSimilarIncidents(),
        runbooks=NotConfiguredRunbookRetrieval(),
        log_search=NotConfiguredLogSearch(),
        active_alerts=NotConfiguredActiveAlerts(),
    )

    from aiokafka import AIOKafkaConsumer
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=kafka_brokers,
        group_id=f"test-{uuid4()}",
        enable_auto_commit=False, auto_offset_reset="earliest",
    )
    await consumer.start()
    enricher = EnrichmentConsumer(
        consumer=consumer, deps=deps,
        assemble_fn=assemble, topic=topic, enriched_topic=topic,
    )
    return engine, incident_repo, consumer, enricher


@pytest.mark.asyncio
async def test_end_to_end_writes_context_and_emits_enriched(
    pg_dsn: str, kafka_brokers: str, migrated_db: None,
) -> None:
    topic = "sentinel.incidents"
    engine, incident_repo, consumer, enricher = await _setup_enricher(
        pg_dsn, kafka_brokers, topic,
    )
    try:
        alert = NormalizedAlert(
            source="generic", external_id="ext-e2e", service="api",
            severity=SeverityType.SEV2, title="boom",
            raw_payload={}, received_at=datetime.now(UTC),
        )
        incident_id = await incident_repo.create_from_alert(alert, fingerprint="fp-e2e")

        producer = AIOKafkaProducer(bootstrap_servers=kafka_brokers)
        await producer.start()
        try:
            event_id = uuid4()
            await producer.send_and_wait(
                topic, value=json.dumps({
                    "event_id": str(event_id),
                    "event": "incident.opened",
                    "incident_id": str(incident_id),
                    "fingerprint": "fp-e2e",
                    "source": "generic",
                    "ts": datetime.now(UTC).isoformat(),
                }).encode(),
                key=str(incident_id).encode(),
            )
        finally:
            await producer.stop()

        runner = asyncio.create_task(enricher.run())
        # Allow a few seconds for the consumer to pick up + write.
        for _ in range(60):
            stored = await incident_repo.get_enrichment_context(incident_id)
            if stored is not None:
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("context_json was never written")

        assert stored.version == 1
        assert stored.context.incident_id == incident_id

        # Outbox row for incident.enriched
        from sentinel.persistence.session import make_session_factory
        sf = make_session_factory(engine)
        async with sf() as s:
            rows = (await s.execute(
                select(OutboxEventModel).where(
                    OutboxEventModel.topic == topic,
                )
            )).scalars().all()
            enriched = [r for r in rows if r.payload.get("event") == "incident.enriched"]
            assert len(enriched) == 1

        enricher.stop()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
    finally:
        await consumer.stop()
        await engine.dispose()


@pytest.mark.asyncio
async def test_replay_same_event_id_is_noop(
    pg_dsn: str, kafka_brokers: str, migrated_db: None,
) -> None:
    topic = "sentinel.incidents"
    engine, incident_repo, consumer, enricher = await _setup_enricher(
        pg_dsn, kafka_brokers, topic,
    )
    try:
        alert = NormalizedAlert(
            source="generic", external_id="ext-replay", service="api",
            severity=SeverityType.SEV2, title="boom",
            raw_payload={}, received_at=datetime.now(UTC),
        )
        incident_id = await incident_repo.create_from_alert(alert, fingerprint="fp-replay")

        event_id = uuid4()
        producer = AIOKafkaProducer(bootstrap_servers=kafka_brokers)
        await producer.start()
        body = json.dumps({
            "event_id": str(event_id), "event": "incident.opened",
            "incident_id": str(incident_id), "fingerprint": "fp-replay",
            "source": "generic", "ts": datetime.now(UTC).isoformat(),
        }).encode()
        try:
            await producer.send_and_wait(topic, value=body, key=str(incident_id).encode())
            await producer.send_and_wait(topic, value=body, key=str(incident_id).encode())
        finally:
            await producer.stop()

        runner = asyncio.create_task(enricher.run())
        for _ in range(60):
            stored = await incident_repo.get_enrichment_context(incident_id)
            if stored is not None and stored.version >= 1:
                break
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)  # let second delivery be processed
        stored = await incident_repo.get_enrichment_context(incident_id)
        assert stored is not None
        assert stored.version == 1   # not 2 — duplicate was a no-op

        enricher.stop()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
    finally:
        await consumer.stop()
        await engine.dispose()
```

Add `import contextlib` at the top of the test file.

- [ ] **Step 3: Run the integration tests**

Run: `pytest tests/integration/enrichment -v -m integration`
Expected: PASS. If `make compose-up` is required to provide containers, follow project convention (Docker Desktop is local). If the test infrastructure is shared with existing `tests/integration/ingestion/conftest.py`, factor the common fixtures into `tests/integration/conftest.py` instead — only do so if the duplication actually causes lint or test-discovery issues.

- [ ] **Step 4: Run full unit suite + typecheck for one last regression sweep**

Run:
```bash
make lint
make typecheck
make test
```
Expected: green.

- [ ] **Step 5: Stage and pause**

Run:
```bash
git add tests/integration/enrichment
```
Do not commit.

---

## Task 17: Code review + final quality gate

**Files:** all of the above. No new code; this is the review checkpoint.

- [ ] **Step 1: Run the full quality gate from scratch**

Run:
```bash
make lint
make typecheck
make test
make test-integration
```
All four must be green. If any are red, fix the cause (do not edit the test) and rerun the gate.

- [ ] **Step 2: Run a subagent code review**

Invoke `superpowers:requesting-code-review`. The review covers the entire Phase 4 changeset (all files staged across Tasks 1–16):
- Phase 3 patches (Tasks 1–3): correctness of the additions, no behavioral regressions on the opened/recurred paths.
- Migration 0003: reversibility, index correctness, downgrade order.
- Repository methods: SQL correctness, idempotency semantics, transaction scope including the outbox insert.
- CircuitBreaker: state-machine correctness, rolling-window semantics, lock discipline, cancellation handling.
- Orchestrator: error coercion, timeout enforcement, metric emission, fetcher-name → section mapping (no silent drops if a fetcher name is mistyped).
- Consumer: envelope validation, poison-pill handling, commit semantics on every branch, outbox event construction.
- Lifespan wiring: startup/shutdown order, no resource leaks on partial failure.

Address any blocking findings and rerun Step 1.

- [ ] **Step 3: Hand off to the user for commit**

Output the list of staged files (`git status --porcelain --staged`) and stop. The user performs the commit and PR per repo convention.

---

## Self-review

**Spec coverage** (against `plans/2026-05-19-enrichment-design.md`):

| Spec section | Covered by |
|---|---|
| §1 Module layout | File-structure table + Tasks 6–14 |
| §2 Circuit breaker | Task 6 (full state machine, deque window, lock, callback, cancellation rule, injectable clock) |
| §3 Fetcher contract & orchestrator | Task 9 (Fetcher Protocol, `_run`, `assemble`, exception coercion, IDs); Tasks 10–12 (six fetchers) |
| §4 Persistence & migration | Tasks 4 (migration 0003) + 5 (repository methods, conditional UPDATE, outbox-in-same-tx) |
| §5 Kafka consumer | Task 14 (envelope, processing loop, `_handle` branches, outbox emit) + Task 15 (lifespan wiring) |
| §6 Failure modes | Tested across Tasks 6 (breaker), 9 (orchestrator coercion), 14 (consumer branches), 16 (end-to-end) |
| §6 Observability metrics | Task 9 Step 1 (registration), used throughout orchestrator + consumer + breaker wiring |
| §6 OTel tracing | Task 3 Step 6 (`configure_tracing` in lifespan) + Task 3 Step 5 (traceparent in outbox messages). `span_for_assemble` helper deferred — orchestrator emits the existing `span_for_fetcher` per-fetcher span only; an explicit `enrichment.assemble` parent span is not added in F. **If a parent span is required for fan-out, add it as a follow-up task.** |
| §6 Logging | Stdlib `logging` with `extra={…}` used throughout — consistent with Phase 3. |
| §6 Tests | Unit tests in Tasks 6, 8, 9, 10, 11, 14; integration in Task 16 |
| §6 Quality gates | Task 17 |
| "Phase 3 patches landing with F" | Tasks 1, 2, 3 |

**Gap acknowledged:** the design's §6 Tracing called for `enrichment.assemble` as an explicit parent span. The plan wires `configure_tracing` and traceparent propagation but does not add the assemble-parent span. Rationale: it's purely additive observability; if dashboards need it, it's a single-task follow-up (`with tracer.start_as_current_span("enrichment.assemble"): ...` inside `assemble`). Calling this out so it's not a hidden gap.

**Placeholder scan:** Searched the plan for "TBD", "TODO", "implement later", "fill in". None present in step bodies. The "if it doesn't exist yet, create it" line in Task 10 Step 3 is conditional based on observed code state, not a placeholder — the verifier read the file and the concrete class is required regardless.

**Type consistency:**
- `EnrichmentWriteResult.status` is `Literal["written", "duplicate"]` — referenced consistently in Tasks 5, 14, 16.
- `Fetcher.name` is a `str` attribute used for both breaker lookup and section mapping in Task 9 — section mapping is via `_SECTION_TO_FIELD` dict, not list position.
- `OutboxEvent` dataclass fields used in Tasks 2, 5, 14 match the existing definition (`id, topic, key, payload, attempts, created_at`).
- `IncidentContext` fields (`incident_id`, `assembled_at`, six FetcherResult sections) added in Task 1 and consumed in Tasks 5, 9, 14, 16.

---

Plan complete and saved to `plans/2026-05-19-enrichment-plan.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — dispatch a fresh subagent per task with two-stage review between tasks. Best fit for L-size plan with 17 tasks: the main thread reviews each task's diff before moving on.

2. **Inline Execution** — execute tasks in this session via `superpowers:executing-plans` with batch checkpoints. Lower setup overhead, but the main thread holds all the context.

Which approach?
