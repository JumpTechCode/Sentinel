# Memory & feedback loop (Work Area H) — design

**Status:** approved (brainstorm), implementation plan to follow.
**Date:** 2026-05-19.
**Scope:** Work Area H per `.claude/program-of-work.md` §H. Plan size: M. Depends on B/C/F/G (all landed).

Phase 4 of Sentinel. Brings online the "gets smarter from use" loop: each resolved incident gets an embedding computed from `title + root_cause + remediation`, and the next opened incident's enrichment retrieves the top-k matches via pgvector cosine search. This is the project's headline learning loop.

Core scope only (per brainstorm answer): embeddings + store + pipeline + MemoryConsumer + resolve route + migration. Out of scope for this phase: runbooks real impl + seed data, `feedback_metrics.py` 30-day gauges, `embeddings_cache` table.

---

## 1. Module layout & responsibilities

```
sentinel/memory/
├── __init__.py            # public exports: EmbeddingProvider, FastEmbedProvider,
│                          #   PgVectorIncidentStore, MemoryPipeline, MemoryConsumer
├── embeddings.py          # FastEmbedProvider implementing EmbeddingProvider Protocol
│                          #   (Protocol already declared in enrichment/protocols.py)
├── store.py               # PgVectorIncidentStore — similar_incidents() with required
│                          #   filter (resolved/closed AND NOT diagnosis_was_correct=false)
├── pipeline.py            # MemoryPipeline.compose_initial(), compose_resolved()
│                          # Pure text-composition; provider injected at consumer level.
├── consumer.py            # MemoryConsumer (aiokafka) — subscribes to incident.opened
│                          #   + incident.resolved; calls pipeline + writes incidents.embedding
└── deps.py                # MemoryConsumerDeps dataclass:
                           #   incident_repo, embedding_provider, pipeline

sentinel/persistence/repositories.py
└── PostgresResolutionRepository      # concrete impl of existing ResolutionRepository
                                      # Protocol. INSERT into resolutions + UPDATE incident
                                      # status/resolved_at + outbox `incident.resolved`,
                                      # all atomically.

sentinel/persistence/errors.py        # NEW: IncidentNotFound, IncidentAlreadyResolved.
                                      # Mirrors sentinel/diagnosis/errors.py precedent.

sentinel/api/routes/resolve.py        # POST /incidents/{id}/resolve — thin route
                                      # delegating to ResolutionRepository.
```

**Boundary rules.**
- `memory/` owns the embedding provider, retrieval store, pipeline, and consumer. It depends on `persistence/` for DB access but only through narrow methods (cosine top-k, set_embedding, load_for_memory).
- `EmbeddingProvider` Protocol stays in `enrichment/protocols.py`; `memory/` provides the concrete impl.
- `PgVectorIncidentStore` *is* the replacement for `NotConfiguredSimilarIncidents` in `app.py` lifespan; it implements the existing `SimilarIncidentRetrieval` Protocol.
- `MemoryConsumer` writes `incidents.embedding` directly via a new repository method (`set_embedding(incident_id, vector, *, event_id)`). It does not touch other incident fields.
- Resolve route is the only new HTTP surface. Returns 200 in <50ms; no embedding compute on the request path.
- Runbooks retrieval stays `NotConfigured` for this phase (deferred).

---

## 2. Embedding provider — fastembed + bge-large-en-v1.5

**Choice:** `fastembed` library + `BAAI/bge-large-en-v1.5`. 1024-dim, MTEB avg ~64, no API keys, no network, no torch dep.

Rationale: this is a public portfolio repo where evaluators must be able to run the demo without paying for or signing up for any provider API. Quality matters because the similarity loop is the headline feature. Local CPU inference at ~100ms is acceptable given the consumer is off the request path.

### Interface

```python
# sentinel/memory/embeddings.py

class FastEmbedProvider:
    """Concrete EmbeddingProvider — local ONNX model, no API keys, no network."""

    DIM = 1024
    MODEL_NAME = "BAAI/bge-large-en-v1.5"

    def __init__(self, *, model_cache_dir: Path, compute_timeout_s: float = 5.0): ...

    async def embed(self, text: str) -> list[float]:
        """Returns a 1024-dim vector. Lazy-loads model on first call.

        Bounded by compute_timeout_s; on timeout raises asyncio.TimeoutError
        for the caller to handle (consumer treats this as a poison pill).
        """
```

### Key decisions

- **Lazy load, one instance per process.** Constructed in `app.py` lifespan and shared. Loading is ~1s; first `embed()` warms it. Subsequent calls ~80–150ms on CPU. An `asyncio.Lock` guards the lazy-load to keep the first concurrent calls from racing.
- **`run_in_executor` for the sync model call.** `fastembed.TextEmbedding.embed()` is sync NumPy; running it in the default thread pool keeps the asyncio loop free.
- **5s timeout.** Matches enrichment's "every external call has a timeout" invariant even though this is internal. Bounds pathological inputs from stalling the consumer.
- **No circuit breaker.** Not an external call. A local-model failure is a process-level fault (OOM, corrupted model file); a breaker would mask it.
- **Model cache dir.** Default `~/.cache/fastembed/` (fastembed default); overridable via `SENTINEL_EMBEDDING_MODEL_CACHE_DIR` for Docker (baked into `/var/cache/fastembed`).
- **Determinism.** bge-large is deterministic; no sampling. Same input → same vector.
- **Dependency footprint.** `fastembed` (~50MB) + `onnxruntime` (~50MB) + model (~400MB ONNX). ~500MB install bump total. Justified by free + high quality + offline.

### Failure modes

| Failure | Logged | Behavior |
|---|---|---|
| Model file missing/corrupt | ERROR `embedding_model_load_failed` | First `embed()` raises; consumer doesn't commit offset; manual intervention |
| Compute >5s timeout | WARN `embedding_compute_timeout` `{len, incident_id}` | Caller (consumer) treats as poison pill, commits offset, increments metric |
| OOM | ERROR + process crashes | Supervisor restart; offset uncommitted → redelivery |

---

## 3. Database changes — migration 0005

The existing `embedding Vector(1536)` columns on `incidents` and `runbooks` were aspirational — never populated. Switching to 1024-dim is a drop-and-recreate (pgvector does not support `ALTER COLUMN` dim).

Additionally, this migration adds `last_embedding_event_id UUID` to `incidents` for MemoryConsumer idempotency — same precedent as `last_enrichment_event_id` (migration 0003).

```python
# migrations/versions/0005_embedding_dim_1024_and_event_id.py
"""switch embedding columns to 1024 dim (bge-large) + add last_embedding_event_id"""

def upgrade() -> None:
    # No incidents.embedding values have been written yet; drop+recreate is safe.
    op.execute("DROP INDEX IF EXISTS idx_incidents_embedding")
    op.execute("DROP INDEX IF EXISTS idx_runbooks_embedding")

    op.drop_column("incidents", "embedding")
    op.drop_column("runbooks", "embedding")

    op.add_column("incidents", sa.Column("embedding", Vector(1024), nullable=True))
    op.add_column("runbooks",  sa.Column("embedding", Vector(1024), nullable=True))

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
    op.add_column("runbooks",  sa.Column("embedding", Vector(1536), nullable=True))
    op.execute(
        "CREATE INDEX idx_incidents_embedding ON incidents "
        "USING hnsw (embedding vector_cosine_ops)"
    )
    op.execute(
        "CREATE INDEX idx_runbooks_embedding ON runbooks "
        "USING hnsw (embedding vector_cosine_ops)"
    )
```

**Model updates** (`sentinel/persistence/models.py`):
- `IncidentModel.embedding`: `Vector(1536)` → `Vector(1024)`.
- `IncidentModel.last_embedding_event_id`: new `Mapped[UUID | None]` column.
- `RunbookModel.embedding`: `Vector(1536)` → `Vector(1024)`.

**Index choice.** HNSW with cosine ops. pgvector defaults (`m=16, ef_construction=64`) are fine for demo scale. Acceptance metric is ≤1s for 10k incidents — HNSW comfortably hits that.

**Append-only migration history.** We do *not* amend `0001_initial` even though no production data exists. Invariant #8 ("migrations are reversible") implies append-only history; a fresh `0005` preserves the audit trail and demonstrates the discipline.

**CI gate.** `make migrate-down && make migrate` round-trip against the integration Postgres — same pattern that gates every other migration.

---

## 4. MemoryConsumer

`sentinel/memory/consumer.py`. Mirrors the EnrichmentConsumer/DiagnosisConsumer pattern: same Kafka topic, distinct group_id, manual offset commit, at-least-once + idempotency on event_id.

### Subscription

- Topic: `sentinel.incidents` (the existing single topic).
- Consumer group: `sentinel-memory` (new).
- Events of interest: `incident.opened`, `incident.resolved`.
- Other event types: DEBUG log, commit.

### New event type

`incident.resolved` is introduced by this phase. Built by `PostgresResolutionRepository.record()` and staged as an outbox row in the resolve transaction. Envelope matches existing events:

```json
{
  "event_id": "<uuid>",
  "event": "incident.resolved",
  "incident_id": "<uuid>",
  "ts": "..."
}
```

### `handle_message(msg)` algorithm

1. Parse + Pydantic-validate envelope. Invalid → ERROR log, commit (poison pill), `sentinel_memory_invalid_events_total++`.
2. Branch on `event`:
   - `incident.opened` → `repo.load_for_memory(incident_id)` → `text = MemoryPipeline.compose_initial(row)` (= `f"{service} {title}"`).
   - `incident.resolved` → `repo.load_for_memory(incident_id)`. If `row.resolution is None` → WARN `memory_resolution_missing`, commit, metric increment. Else → `text = MemoryPipeline.compose_resolved(row, row.resolution)` (= `f"{title}\n{root_cause}\n{remediation}"`).
   - other → DEBUG, commit.
3. `vector = await embedding_provider.embed(text)` (5s timeout from §2). Timeout → WARN, commit (poison pill), metric. Other exceptions → ERROR, do **not** commit, redelivery.
4. `result = await repo.set_embedding(incident_id, vector, event_id=event_id)`.
   - `"written"` → INFO, commit.
   - `"duplicate"` → INFO `memory_duplicate`, commit, `sentinel_memory_duplicates_total++`.
   - `"unknown_incident"` → WARN `memory_unknown_incident`, commit, metric.
5. Commit offset.

### Idempotency

Conditional UPDATE mirrors the enrichment pattern:

```sql
UPDATE incidents
SET embedding = :vec,
    last_embedding_event_id = :event_id
WHERE id = :incident_id
  AND (last_embedding_event_id IS DISTINCT FROM :event_id)
RETURNING id;
```

`RETURNING` empty + incident exists → duplicate. Without `last_embedding_event_id`, a redelivered event would recompute the same vector and write it again — harmless but wasteful and untrackable.

### Why a dedicated consumer rather than piggybacking on enrichment

- Enrichment consumes `incident.opened` already, but its job is context assembly. Adding embedding compute there would mix concerns and block enrichment on the model load.
- Separate consumer = separate failure domain. Embedding errors do not stall enrichment.
- Matches the project's idiom: one consumer per phase deliverable (enrichment, diagnosis, memory).

### Lifecycle

Started unconditionally in `app.py` lifespan unless `memory_consumer_enabled=False`. Mirrors the `diagnoser` shutdown sequence (`stop()` → `wait_for(task, 5s)` → cancel on timeout → close consumer with `suppress(Exception)`).

### Metrics

- `sentinel_memory_events_consumed_total{type}` — counter (opened/resolved).
- `sentinel_memory_events_failed_total{reason}` — counter.
- `sentinel_memory_duplicates_total` — counter.
- `sentinel_memory_invalid_events_total` — counter.
- `sentinel_memory_embedding_duration_seconds{event_type}` — histogram (text→vector compute time).

---

## 5. Resolve route + ResolutionRepository

### Request/response schemas

`ResolveIncidentRequest` already exists in `sentinel/schemas/api.py:44-51`. Reused as-is. (No new constraints added in this phase — out of scope.)

```python
# Already in schemas/api.py — reused as-is
class ResolveIncidentRequest(BaseModel):
    root_cause: str = Field(min_length=1)
    remediation: str = Field(min_length=1)
    category: CategoryType
    diagnosis_was_correct: bool | None = None
    notes: str | None = None
    resolved_by: str | None = None
```

**NEW** — `ResolveIncidentResponse`:

```python
# Add to schemas/api.py
class ResolveIncidentResponse(BaseModel):
    model_config = ConfigDict(frozen=True)
    incident_id: UUID
    status: Literal["resolved"]
    resolved_at: datetime
    event_id: UUID  # the staged `incident.resolved` outbox event_id
```

### Exceptions

New file `sentinel/persistence/errors.py`. Mirrors `sentinel/diagnosis/errors.py` precedent.

```python
class IncidentNotFound(Exception):
    def __init__(self, incident_id: UUID) -> None:
        super().__init__(f"incident not found: {incident_id}")
        self.incident_id = incident_id


class IncidentAlreadyResolved(Exception):
    def __init__(self, incident_id: UUID) -> None:
        super().__init__(f"incident already resolved: {incident_id}")
        self.incident_id = incident_id
```

### PostgresResolutionRepository

Concrete implementation of the existing `ResolutionRepository` Protocol (`repositories.py:778`). The Protocol signature is updated to take `outbox_topic` as a per-method kwarg, matching the `PostgresIncidentRepository.ingest()` convention (line 307).

```python
class ResolutionRepository(Protocol):
    async def record(
        self,
        incident_id: UUID,
        resolution: ResolveIncidentRequest,
        *,
        outbox_topic: str,
    ) -> ResolveRecordResult: ...


@dataclass(frozen=True, slots=True)
class ResolveRecordResult:
    incident_id: UUID
    resolved_at: datetime
    event_id: UUID  # staged outbox event_id


class PostgresResolutionRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def record(
        self,
        incident_id: UUID,
        body: ResolveIncidentRequest,
        *,
        outbox_topic: str,
    ) -> ResolveRecordResult:
        async with self._session_factory() as s, s.begin():
            # 1. Lock incident; verify exists and not already resolved.
            incident = (await s.execute(
                select(IncidentModel)
                  .where(IncidentModel.id == incident_id)
                  .with_for_update()
            )).scalar_one_or_none()
            if incident is None:
                raise IncidentNotFound(incident_id)
            if incident.status in ("resolved", "closed"):
                raise IncidentAlreadyResolved(incident_id)

            now = (await s.execute(select(func.now()))).scalar_one()

            # 2. INSERT resolutions row.
            s.add(ResolutionModel(
                incident_id=incident_id,
                root_cause=body.root_cause,
                remediation=body.remediation,
                category=body.category,
                diagnosis_was_correct=body.diagnosis_was_correct,
                notes=body.notes,
                resolved_by=body.resolved_by,
                resolved_at=now,
            ))

            # 3. UPDATE incident.
            incident.status = "resolved"
            incident.resolved_at = now

            # 4. Stage `incident.resolved` outbox row in the same tx.
            event_id = uuid.uuid4()
            s.add(OutboxEventModel(
                id=event_id,
                topic=outbox_topic,
                key=str(incident_id),
                payload={
                    "event_id": str(event_id),
                    "event": "incident.resolved",
                    "incident_id": str(incident_id),
                    "ts": now.isoformat(),
                },
            ))
        # commit on context exit.
        return ResolveRecordResult(incident_id=incident_id, resolved_at=now, event_id=event_id)
```

### Route

```python
# sentinel/api/routes/resolve.py

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
    topic: str = request.app.state.outbox_topic
    try:
        result = await repo.record(incident_id, body, outbox_topic=topic)
    except IncidentNotFound:
        raise HTTPException(status_code=404, detail="incident_not_found")
    except IncidentAlreadyResolved:
        raise HTTPException(status_code=409, detail="incident_already_resolved")
    return ResolveIncidentResponse(
        incident_id=result.incident_id,
        status="resolved",
        resolved_at=result.resolved_at,
        event_id=result.event_id,
    )
```

### Key properties

- **Atomic.** One transaction; resolution INSERT + incident UPDATE + outbox INSERT commit together.
- **No embedding compute on the request path.** Handler returns ~10ms after commit; MemoryConsumer picks up the outbox event via the existing OutboxDrainer.
- **Idempotency at the API layer.** Second POST to a resolved incident returns 409. Operator action — explicit failure is correct.
- **Row-level lock (`FOR UPDATE`).** Defends against concurrent resolve calls.
- **Outbox topic = `sentinel.incidents`.** Single-topic design preserved; MemoryConsumer filters by `event` field.

### Failure modes

| Path | Failure | Returned | Logged |
|---|---|---|---|
| Incident missing | repo raises `IncidentNotFound` | 404 | INFO `resolve_incident_not_found` |
| Already resolved | repo raises `IncidentAlreadyResolved` | 409 | INFO `resolve_incident_already_resolved` |
| Invalid payload | FastAPI/Pydantic | 422 | (FastAPI default) |
| Lock wait / deadlock | sqlalchemy exception | 500 | ERROR `resolve_db_failed` exc_info |
| Outbox write fails | rolled back with the rest | 500 | ERROR `resolve_db_failed` exc_info |

Once the route returns 200, the resolution is durable. Embedding refresh is best-effort downstream; if the MemoryConsumer is stalled, the row exists, status is correct, the outbox row will eventually drain.

---

## 6. Wiring (app.py lifespan) + Settings

### `app.py` lifespan additions

Order: embedding provider → resolution repo → memory consumer + replacement of `NotConfiguredSimilarIncidents`.

```python
# After existing engine/session_factory/incident_repo/outbox_repo construction:

# ---- Memory (Phase 4) ------------------------------------------------------
memory_consumer: MemoryConsumer | None = None
memory_task: asyncio.Task[None] | None = None
mem_kafka_consumer: AIOKafkaConsumer | None = None
similar_incidents_store: SimilarIncidentRetrieval = NotConfiguredSimilarIncidents()
resolution_repo = PostgresResolutionRepository(session_factory)
app.state.resolution_repo = resolution_repo
app.state.outbox_topic = settings.kafka_topic_incidents

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

**Critical line** — replace `NotConfiguredSimilarIncidents()` in `enrich_deps` (currently `app.py:88`):

```python
enrich_deps = EnrichmentDeps(
    ...
    similar_incidents=similar_incidents_store,   # was NotConfiguredSimilarIncidents()
    runbooks=NotConfiguredRunbookRetrieval(),    # still stubbed (out of scope)
    ...
)
```

This single change brings the "gets smarter from use" loop online. Once resolved incidents have embeddings, the next opened incident's enrichment retrieves them.

### Shutdown ordering

Memory consumer joins the existing pattern — diagnoser → enricher → memory → drainer → producer/redis/engine. Order is mostly cosmetic; all three Kafka consumers are independent.

### `build_app()`

```python
app.include_router(resolve_router)   # new
```

### Settings additions (`sentinel/config/settings.py`)

```python
# Memory & embeddings (Work Area H)
embedding_model_name: str = "BAAI/bge-large-en-v1.5"   # informational; not pluggable yet
embedding_model_cache_dir: str = "/var/cache/fastembed"
embedding_compute_timeout_seconds: float = 5.0
kafka_consumer_group_memory: str = "sentinel-memory"
memory_consumer_enabled: bool = True   # mirrors diagnosis_consumer_enabled
```

`memory_consumer_enabled` gates the entire memory-construction block (provider, store, repo, consumer). When False, `enrich_deps.similar_incidents = NotConfiguredSimilarIncidents()` is used and enrichment continues with degraded similar-incident retrieval. Mirrors the `diagnosis_consumer_enabled` pattern.

### Dockerfile change

Pre-download the model at image build time so cold-start doesn't hit the network:

```dockerfile
ENV SENTINEL_EMBEDDING_MODEL_CACHE_DIR=/var/cache/fastembed
RUN python -c "from fastembed import TextEmbedding; \
               TextEmbedding('BAAI/bge-large-en-v1.5', cache_dir='/var/cache/fastembed')"
```

Adds ~400MB to the image. Justified — alternative is a 30s+ cold start every restart.

### `.env.example`

Add the five settings keys with sensible defaults. No secrets (free local model).

### `pyproject.toml`

Add `fastembed` (~50MB lib; pulls `onnxruntime` ~50MB; no torch). Runtime dependency.

---

## 7. Repository methods + PgVectorIncidentStore queries

The load-bearing SQL added this phase.

### 7.1 `IncidentRepository.set_embedding()` — MemoryConsumer write path

Added to the existing `IncidentRepository` Protocol and `PostgresIncidentRepository`:

```python
async def set_embedding(
    self,
    incident_id: UUID,
    embedding: list[float],
    *,
    event_id: UUID,
) -> Literal["written", "duplicate", "unknown_incident"]:
    """Idempotently set incidents.embedding. Conditional UPDATE keyed on
    last_embedding_event_id makes a redelivered event a no-op."""
```

SQL:
```sql
UPDATE incidents
SET embedding = :vec,
    last_embedding_event_id = :event_id
WHERE id = :incident_id
  AND (last_embedding_event_id IS DISTINCT FROM :event_id)
RETURNING id;
```

Distinguishes the three outcomes via a follow-up SELECT when `RETURNING` is empty — duplicate vs missing incident. Three-state return lets the consumer pick the right metric and log level.

### 7.2 `IncidentRepository.load_for_memory()` — read path

Single method for both event-type branches; LEFT JOIN against `resolutions`:

```python
@dataclass(frozen=True, slots=True)
class MemoryIncidentRow:
    id: UUID
    service: str
    title: str
    status: str
    resolution: ResolutionData | None


@dataclass(frozen=True, slots=True)
class ResolutionData:
    root_cause: str
    remediation: str
    diagnosis_was_correct: bool | None


async def load_for_memory(self, incident_id: UUID) -> MemoryIncidentRow | None: ...
```

Single SQL with LEFT JOIN. `resolution=None` for opened path; populated for resolved path. Keeps the join in the repo (where SQL belongs) instead of two round-trips in the consumer.

### 7.3 `PgVectorIncidentStore.top_k()` — replaces `NotConfiguredSimilarIncidents`

Implements the existing `SimilarIncidentRetrieval` Protocol.

```python
async def top_k(
    self,
    *,
    query_text: str,
    k: int,
    exclude_incident_id: UUID | None,
) -> FetcherResult[SimilarIncidentItem]:
    # 1. Embed the query. Failures degrade gracefully (fetcher contract: never raise).
    try:
        query_vec = await self._embedder.embed(query_text)
    except Exception as e:
        log.warning("similar_incidents_embed_failed", extra={"err": repr(e)})
        return FetcherResult(
            status="failed", data=[],
            error=f"embed_failed: {type(e).__name__}",
            fetched_at=datetime.now(UTC),
        )

    # 2. Cosine top-k with spec-required filters.
    sql = text("""
        SELECT i.id, i.title, r.root_cause, r.remediation,
               1 - (i.embedding <=> CAST(:qvec AS vector)) AS cosine_similarity
        FROM incidents i
        JOIN resolutions r ON r.incident_id = i.id
        WHERE i.embedding IS NOT NULL
          AND i.status IN ('resolved', 'closed')
          AND (r.diagnosis_was_correct IS NULL OR r.diagnosis_was_correct = TRUE)
          AND (CAST(:exclude_id AS uuid) IS NULL OR i.id <> CAST(:exclude_id AS uuid))
        ORDER BY i.embedding <=> CAST(:qvec AS vector)
        LIMIT :k
    """)
    rows = (await s.execute(sql, {
        "qvec": query_vec,
        "exclude_id": exclude_incident_id,
        "k": k,
    })).all()

    return FetcherResult(
        status="ok",
        data=[
            SimilarIncidentItem(
                id=similar_id(r.id), title=r.title,
                root_cause=r.root_cause, remediation=r.remediation,
                cosine_similarity=float(r.cosine_similarity),
            )
            for r in rows
        ],
        fetched_at=datetime.now(UTC),
    )
```

**Key properties:**

- **HNSW index used.** Postgres planner picks `idx_incidents_embedding` automatically for `ORDER BY embedding <=> ... LIMIT k`. Integration test asserts via `EXPLAIN`.
- **JOIN on resolutions is intentional.** The response shape `SimilarIncidentItem` requires `root_cause` and `remediation` — only resolved incidents have them. The JOIN is also the correctness gate.
- **`diagnosis_was_correct` filter** matches spec: `NULL` allowed (ungraded), `TRUE` allowed, `FALSE` excluded. The feedback loop — wrong diagnoses don't contaminate future retrieval.
- **`<=>` is cosine distance** (0 = identical). Convert to similarity = `1 - distance`.
- **Embed-failure path returns `failed`, not raises.** Honors the enrichment fetcher contract.

### Wiring summary

| New method / class | File | Consumer |
|---|---|---|
| `IncidentRepository.set_embedding` | `persistence/repositories.py` | MemoryConsumer |
| `IncidentRepository.load_for_memory` + DTOs | `persistence/repositories.py` | MemoryConsumer |
| `PgVectorIncidentStore` | `memory/store.py` | `enrich_deps.similar_incidents` |
| `PostgresResolutionRepository` | `persistence/repositories.py` | Resolve route |
| `IncidentNotFound`, `IncidentAlreadyResolved` | `persistence/errors.py` (new) | Resolve route |
| `ResolveIncidentResponse` | `schemas/api.py` (additive) | Resolve route |
| `last_embedding_event_id` column | `models.py` + migration 0005 | `set_embedding` idempotency |
| `IncidentModel.embedding`/`RunbookModel.embedding` dim change | `models.py` + migration 0005 | Provider symmetry |

---

## 8. Failure modes, observability, tests, ADRs

### Composite failure-mode table

| Path | Failure | Logged | Surfaced | Recovery |
|---|---|---|---|---|
| Model load (first embed) | ONNX file missing/corrupt | ERROR `embedding_model_load_failed` | Consumer task exits | Supervisor restart with valid model dir |
| Embedding compute | timeout (>5s) | WARN `embedding_compute_timeout` `{len, incident_id}` | Consumer commits offset (poison pill) | Manual replay if input was valid |
| Embedding compute | unexpected exception | ERROR `embedding_compute_failed` exc_info | Offset uncommitted | Redelivery; if persistent, manual intervention |
| Memory envelope invalid | Pydantic error | ERROR `memory_invalid_envelope` `{offset}` | Commit (poison pill) | Metric increment |
| Memory unknown event type | — | DEBUG | Commit | — |
| Memory unknown incident | repo returns None | WARN `memory_unknown_incident` `{incident_id}` | Commit | Metric increment |
| Memory resolved-event with missing resolution | `row.resolution is None` | WARN `memory_resolution_missing` `{incident_id}` | Commit | Metric increment — likely a rolled-back resolve |
| Memory duplicate event_id | conditional UPDATE returns 0 | INFO `memory_duplicate` | Commit | Metric increment |
| Memory DB write fails | sqlalchemy exception | ERROR `memory_write_failed` exc_info | Offset uncommitted | Redelivery; conditional UPDATE is idempotent |
| Resolve route: missing incident | repo raises `IncidentNotFound` | INFO `resolve_incident_not_found` | 404 | Operator retry on correct ID |
| Resolve route: already resolved | repo raises `IncidentAlreadyResolved` | INFO `resolve_incident_already_resolved` | 409 | Operator no-op |
| Resolve route: DB tx fails | sqlalchemy exception | ERROR `resolve_db_failed` exc_info | 500 | Operator retry; full tx rolled back |
| Retrieval: embedding fails at query time | embed() raises/timeouts | WARN `similar_incidents_embed_failed` | `FetcherResult(status="failed", ...)` | Enrichment continues with degraded section |
| Retrieval: DB query fails | sqlalchemy exception | WARN `similar_incidents_query_failed` | `FetcherResult(status="failed", ...)` | Same — fetchers never raise |

### Observability

#### Prometheus metrics added

- `sentinel_memory_events_consumed_total{type}` — counter (opened/resolved)
- `sentinel_memory_events_failed_total{reason}` — counter
- `sentinel_memory_duplicates_total` — counter
- `sentinel_memory_invalid_events_total` — counter
- `sentinel_memory_embedding_duration_seconds{event_type}` — histogram (text→vector)
- `sentinel_similar_incidents_query_duration_seconds` — histogram (DB top-k)
- `sentinel_similar_incidents_returned_total` — histogram (results per query)

#### Tracing

One `memory.embed` span per event (attrs: `event_type`, `incident.id`, `text.length`, `vector.dim`). Child of the W3C-propagated context from the producer.

#### Logging

stdlib `logging` (consistent with Phase 3/4 codebase). `incident_id` and `event_id` in `extra={...}`.

### Tests

#### Unit (`tests/unit/memory/`)

- `test_embeddings.py` — `FastEmbedProvider`: deterministic output, dim=1024, timeout enforcement (inject a slow executor), lazy load idempotency.
- `test_pipeline.py` — `compose_initial(row)` returns `f"{service} {title}"`; `compose_resolved(row, resolution)` returns `f"{title}\n{root_cause}\n{remediation}"`; UTF-8 handling.
- `test_consumer.py` — envelope validation, branch on event type, unknown event → DEBUG+commit, unknown incident → WARN+commit+metric, duplicate event_id → INFO+commit+metric, DB write fail → offset uncommitted, embedding timeout → commit+metric (poison pill), resolved-event with no resolution row → WARN+commit.
- `test_store.py` — `PgVectorIncidentStore.top_k` with fake session: query SQL has the three filters, embedding failure returns `FetcherResult(failed)`, exclude_incident_id handling.
- `test_resolve_repo.py` — `PostgresResolutionRepository.record`: missing incident raises `IncidentNotFound`, already-resolved raises `IncidentAlreadyResolved`, success commits all three writes atomically, outbox row present with correct payload.
- `test_resolve_route.py` — route maps repo exceptions to 404/409, returns 200 with correct schema on success.

#### Integration (`tests/integration/test_memory.py`, `@pytest.mark.integration`)

- Migration `0005` round-trip (`migrate-down → migrate`).
- End-to-end opened path: webhook → `incident.opened` → memory consumer → `incidents.embedding` populated (1024 floats), `last_embedding_event_id` set.
- End-to-end resolved path: `POST /incidents/{id}/resolve` → resolutions row exists, incident.status='resolved', outbox row emitted → memory consumer recomputes embedding → cosine distance to a query differs measurably from initial embedding (spec acceptance: sanity).
- Retrieval correctness: seed 5 resolved incidents (some with `diagnosis_was_correct=False`) → `top_k(query)` returns only the eligible ones in cosine order.
- Replay same `incident.resolved` event → `memory_duplicates_total` increments, no double-write.
- HNSW index usage: `EXPLAIN ANALYZE` shows Index Scan on `idx_incidents_embedding` for ≥100 seeded rows.
- Resolve idempotency: second POST to same incident → 409.

### Quality gates

- `make lint` / `make typecheck` / `make test` / `make test-integration` all green.
- Migration `0005` reversible — `make migrate-down && make migrate` round-trip in CI.
- New runtime dep: `fastembed`. New env vars: 5 (all defaulted, no secrets).
- Mandatory subagent code review before commit (per the project review policy).

### ADRs

Two non-obvious decisions warrant ADRs per CLAUDE.md:

- **ADR 0005 — Embedding on resolve.** Why we embed `title + root_cause + remediation` at resolve time (and `service + title` at open time), not at any other point. The "gets smarter from use" loop rationale; rejected alternatives (embed on every status change, embed at diagnosis time). Pre-named in the spec.
- **ADR 0006 — Local embeddings (fastembed + bge-large-en-v1.5).** Why a local CPU model rather than Voyage/OpenAI for a public portfolio repo. Tradeoffs: zero API keys + free demo vs. ~500MB install bump; no per-call cost vs. ~100ms CPU latency. Includes the column dim change rationale (1536 → 1024) and how to swap a remote provider later (single Protocol implementation).

---

## Out of scope (deferred)

Per the brainstorm scope answer:

- `RunbookRetrieval` real impl + runbook seed data (deferred to follow-up phase). `runbooks` fetcher remains degraded.
- `feedback_metrics.py` — 30-day rolling Prometheus gauges for diagnosis_correctness_rate, category_accuracy_rate, hallucinated_evidence_rate. Deferred.
- `embeddings_cache` table — YAGNI with a local free provider. Add if/when we swap to a paid provider.
- Backfill of historical incidents' embeddings — none exist yet.
- Tunable HNSW `ef_search`.
- Multi-provider settings layer — current impl hard-wires `FastEmbedProvider`. Pluggable swap is a one-impl Protocol change later.
- Tightening `ResolveIncidentRequest` field constraints (max_length).
