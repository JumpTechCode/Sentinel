# Enrichment — parallel context assembly (Work Area F) — design

**Status:** approved (brainstorm), implementation plan to follow.
**Date:** 2026-05-19.
**Scope:** Work Area F per `.claude/program-of-work.md` §F. Plan size: L. Depends on B/C/D/E (all landed).

Phase 4 of Sentinel. The enrichment pipeline is one of the two technical centerpieces of the project: it converts a Kafka `incident.opened` event into a pre-assembled `IncidentContext` that the diagnosis agent (Work Area G) reasons over. The design honors the spec's single-shot diagnosis stance — enrichment does all the data gathering up front; the LLM never fetches.

### Phase 3 patches landing with F

A fact-check against the current code surfaced three concrete additions that don't fit Work Area F's module boundary but block its acceptance. They land in the same PR:

1. **Outbox payload carries `event_id`.** `sentinel/persistence/repositories.py:429-435` builds the outbox JSON without an `event_id` field; the enricher needs it for idempotency. One-line patch: include `OutboxEventModel.id` in the payload.
2. **Outbox producer sets W3C tracecontext headers.** `sentinel/ingestion/kafka_producer.py` doesn't inject `traceparent` today; the enricher reads it to build the assemble span. Small patch in the producer's `send` path.
3. **`IncidentContext` schema gains `incident_id` and `assembled_at`.** Currently missing in `sentinel/schemas/context.py:85-96`; both are required by the orchestrator and persistence layer.

---

## 1. Module layout & responsibilities

```
sentinel/enrichment/
├── __init__.py                  # public exports: assemble(), CircuitBreaker, FetcherResult re-export
├── circuit_breaker.py           # asyncio CircuitBreaker (closed/open/half-open, rolling 5/60s, 30s cooldown)
├── orchestrator.py              # assemble(incident, deps) -> IncidentContext
├── consumer.py                  # aiokafka consumer for `sentinel.incidents`, calls assemble + persist
├── deps.py                      # EnrichmentDeps dataclass: fetchers, breakers (per fetcher), repositories, adapters
├── protocols.py                 # EmbeddingProvider, LogSearchAdapter, ActiveAlertsAdapter (Protocol classes)
├── defaults.py                  # NotConfigured* default impls — return FetcherResult(status="degraded")
└── fetchers/
    ├── __init__.py              # registers six fetchers; each: async fetch(incident, deps) -> FetcherResult[T]
    ├── deploys.py               # reads DeployRepository (DB-only, real)
    ├── related_alerts.py        # reads IncidentRepository (DB-only, real)
    ├── similar_incidents.py     # uses EmbeddingProvider + retrieval Protocol (default: degraded)
    ├── runbooks.py              # uses EmbeddingProvider + RunbookRepository (default: degraded)
    ├── recent_logs.py           # uses LogSearchAdapter (default: degraded)
    └── active_alerts.py         # uses ActiveAlertsAdapter (default: degraded)
```

**Boundary rules.**

- Fetchers receive a typed `EnrichmentDeps`; they never import DB sessions or HTTP clients directly.
- Repositories live in `persistence/` (existing pattern).
- `Protocol` implementations land in their relevant module later: `memory/` (H), `integrations/` (log/active-alerts adapters).
- Only `assemble(incident, deps) -> IncidentContext`, `CircuitBreaker`, and the three `Protocol` classes are exported from `sentinel.enrichment`.
- The consumer is wired in `app.py` lifespan (worker mode), matching the existing OutboxDrainer pattern from Phase 3.

---

## 2. Circuit breaker

`sentinel/enrichment/circuit_breaker.py`.

### State machine

Three states: `CLOSED` (normal), `OPEN` (fail-fast), `HALF_OPEN` (one trial allowed).

### Failure window

Rolling 60-second window backed by a `collections.deque[float]` of failure timestamps. On each call, prune entries older than `now - window_s` from the left before counting. **Counter-only is forbidden** — `program-of-work.md` §F-risks calls this out explicitly: "Breaker windows: use a deque of timestamps, not just a counter — counter-only fails the 'rolling' requirement."

### Transitions

- `CLOSED → OPEN` when window size ≥ 5 after a failure.
- `OPEN → HALF_OPEN` after 30s cooldown elapsed (`opened_at + cooldown_s ≤ now`).
- `HALF_OPEN → CLOSED` on a successful trial; failure → `OPEN` (cooldown re-armed).
- Only one in-flight call permitted in `HALF_OPEN`. Concurrent callers in HALF_OPEN see `OPEN` semantics and short-circuit.

### API

```python
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
    ): ...

    @property
    def state(self) -> Literal["closed", "open", "half_open"]: ...

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        """Raises CircuitOpenError when short-circuiting; otherwise invokes fn and records outcome."""
```

### Metrics

On every state change, set `sentinel_circuit_breaker_state{integration=<name>}` (0=closed, 1=open, 2=half_open). Wired via the `on_state_change` callback so the breaker doesn't import the metrics module directly (testability).

### What counts as a failure

Any exception raised by `fn` except `asyncio.CancelledError` (cancellation isn't an integration fault). Timeouts (`asyncio.TimeoutError`) count.

### Concurrency

One `asyncio.Lock` per breaker guards the state read-modify-write path. Calls themselves run outside the lock — the lock is held only across the few statements that mutate the deque and state.

### Tests

Pure asyncio, no I/O. Drive the clock via `time_fn` injection (default `time.monotonic`) so tests can fast-forward.

---

## 3. Fetcher contract & orchestrator

### Fetcher contract

```python
class Fetcher(Protocol[T]):
    name: str             # used for metrics labels + breaker key, e.g. "deploys"
    timeout_s: float      # default 5.0; per-fetcher override allowed
    async def fetch(self, incident: IncidentRow, deps: EnrichmentDeps) -> FetcherResult[T]: ...
```

Fetchers **never raise** — they return `FetcherResult(status=...)`. The orchestrator wraps every fetcher call regardless, so a misbehaving fetcher still degrades gracefully.

### Orchestrator algorithm

```python
async def assemble(incident: IncidentRow, deps: EnrichmentDeps) -> IncidentContext:
    started = time.monotonic()
    coros = [_run(f, incident, deps) for f in deps.fetchers]
    results = await asyncio.gather(*coros, return_exceptions=True)
    # Any Exception slipped through? Convert to FetcherResult(status="failed", error=repr(e)).
    sections = {name: _coerce(r) for name, r in zip(deps.fetcher_names, results)}
    ctx = IncidentContext(
        incident_id=incident.id,
        assembled_at=utcnow(),
        recent_deploys=sections["deploys"],
        related_alerts=sections["related_alerts"],
        similar_incidents=sections["similar_incidents"],
        runbooks=sections["runbooks"],
        recent_logs=sections["recent_logs"],
        active_alerts=sections["active_alerts"],
    )
    record_enrichment_metrics(ctx, duration=time.monotonic() - started)
    return ctx
```

### `_run` helper

Wraps the per-fetcher call with:

1. `breaker = deps.breakers[f.name]` — per-fetcher breaker (named after the fetcher).
2. `await breaker.call(lambda: asyncio.wait_for(f.fetch(incident, deps), timeout=f.timeout_s))`.
3. Catches `CircuitOpenError`, `asyncio.TimeoutError`, and any other `Exception` → returns `FetcherResult(status="failed", error=...)`. Logs at WARN with structured fields `{fetcher, reason, incident_id}`.
4. Increments `sentinel_enrichment_failures_total{fetcher, reason}` on failed/degraded; observes `sentinel_enrichment_duration_seconds{fetcher}`.

### Per-fetcher (not per-integration) breakers

Two fetchers can target the same external system (e.g. `recent_logs` and `active_alerts` both via Datadog). For V1 we key by fetcher name (matches metrics label, simplest mental model). If a real shared-integration breaker becomes necessary, that's a future refactor; the interface stays.

### Stable IDs

Each fetcher generates IDs in its own format:

- `deploy:<sha>`
- `related:<uuid>`
- `similar:<uuid>`
- `runbook:<uuid>`
- `log:<idx>` (idx is the log line index within the section)
- `active_alerts` IDs: deferred — the fetcher is a stub for F.

Helpers already live in `sentinel/schemas/ids.py` (`deploy_id`, `related_id`, `similar_id`, `runbook_id`, `log_id`, `parse_context_id`). F adds round-trip tests.

### `IncidentContext` schema additions

`sentinel/schemas/context.py:85-96` defines `IncidentContext` with the six `FetcherResult` section fields only — it is **missing** `incident_id` and `assembled_at`. F adds both fields (the orchestrator and persistence layer both require them). Section IDs `deploy:<sha>`, `related:<uuid>`, `similar:<uuid>`, `runbook:<uuid>`, `log:<idx>` already have helpers in `sentinel/schemas/ids.py`; `active_alerts` IDs are out of scope for F (the fetcher is a stub returning `degraded`) and will be added when the source-platform adapter lands.

---

## 4. Persistence & migration

### Migration 0003 — add incident enrichment context columns

```sql
ALTER TABLE incidents
  ADD COLUMN context_json JSONB,
  ADD COLUMN context_assembled_at TIMESTAMPTZ,
  ADD COLUMN context_version INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN last_enrichment_event_id UUID;

CREATE INDEX ix_incidents_last_enrichment_event_id
  ON incidents (last_enrichment_event_id)
  WHERE last_enrichment_event_id IS NOT NULL;
```

- `context_json` — serialized `IncidentContext` (sections + freshness).
- `context_assembled_at` — when this snapshot was built; lets diagnosis judge staleness.
- `context_version` — monotonic counter, incremented on every fresh write. Audit ("v3 superseded v2 at T") and lets diagnosis refuse stale contexts.
- `last_enrichment_event_id` — UUID of the Kafka event whose processing wrote this snapshot. Idempotency key.

`downgrade()` drops all four columns and the index. Round-trip tested in CI per non-negotiable invariant #8.

### Repository extension

`IncidentRepository` Protocol today (`sentinel/persistence/repositories.py:242-259`): `create_from_alert`, `get`, `list_recent`, `mark_diagnosing`, `ingest`. F adds two methods:

```python
async def write_enrichment_context(
    self,
    *,
    incident_id: UUID,
    event_id: UUID,
    context: IncidentContext,
    assembled_at: datetime,
    outbox_event: OutboxEvent | None = None,  # staged in the same tx
) -> EnrichmentWriteResult:
    """
    Idempotent. If incidents.last_enrichment_event_id == event_id, return status='duplicate'
    and no write. Otherwise UPDATE context_json/assembled_at/version=version+1/last_enrichment_event_id
    in a single statement. If outbox_event provided, INSERT it in the same transaction.
    """

async def get_enrichment_context(
    self,
    incident_id: UUID,
) -> StoredEnrichmentContext | None:
    """Returns the persisted context + version + assembled_at, or None if never enriched."""
```

### Idempotency mechanism

```sql
UPDATE incidents
SET context_json = :ctx,
    context_assembled_at = :assembled_at,
    context_version = context_version + 1,
    last_enrichment_event_id = :event_id
WHERE id = :incident_id
  AND (last_enrichment_event_id IS DISTINCT FROM :event_id)
RETURNING context_version;
```

- If `RETURNING` yields no row → duplicate event → no-op.
- Otherwise → the new version is the write confirmation.
- Atomic; no read-then-write race even with concurrent enricher replicas.

### Why not a separate `incident_contexts` table

Single-row-per-incident matches the spec wording ("persist as JSONB on the incident"). If we later need history, add an `incident_context_versions` table populated from a trigger or app-level write. Not in scope for F.

### Serialization

`context.model_dump(mode="json")`. Pydantic JSON encoders handle datetimes/UUIDs. JSONB stores it natively; querying it is not part of F's contract.

---

## 5. Kafka consumer

`sentinel/enrichment/consumer.py`.

### Subscription

- Topic: `sentinel.incidents`.
- Consumer group: `sentinel-enricher`.
- Events of interest: `incident.opened`, `incident.recurred`.
- Other event types: skipped (DEBUG log, offset committed).

### Event shape — and required Phase 3 patch

The current Phase 3 outbox payload (built at `sentinel/persistence/repositories.py:429-435`) has fields `event, incident_id, fingerprint, source, ts` — it does **not** include `event_id`. The DB row's `OutboxEventModel.id` is stable across retries but is not serialized into the Kafka message, so a downstream consumer cannot dedup on it.

**F adds `event_id` to the outbox JSON payload** (one-line change to the payload builder: set `event_id` = the `OutboxEventModel.id` being inserted). This is a required Phase 3 patch landing in the same PR as F. Post-patch envelope:

```json
{
  "event_id": "<uuid>",           // OutboxEventModel.id, stable across retries
  "event": "incident.opened",      // existing field, renamed in spec text as "type" but actual key is "event"
  "incident_id": "<uuid>",
  "fingerprint": "...",
  "source": "...",
  "ts": "..."
}
```

(Spec uses "type" colloquially; the JSON key stays `event` to match the existing field name. No other consumer is affected — the only existing consumer is the soon-to-be-built enricher.)

F-acceptance "re-consuming the same event does not duplicate the persisted context" then hinges on `event_id` being stable across retries, which `OutboxEventModel.id` provides by construction (it's a DB-generated UUID, not re-rolled on retry).

### Processing loop

```python
async def run(self) -> None:
    async for msg in self._consumer:
        try:
            await self._handle(msg)
        except Exception:
            log.exception("enricher_event_failed", offset=msg.offset, partition=msg.partition)
            # Do NOT commit. Re-deliver on next poll; outbox + at-least-once guarantees survival.
            continue
        await self._consumer.commit()
```

### `_handle(msg)` steps

1. Parse + Pydantic-validate envelope. Invalid → log ERROR, **commit** (poison pill, don't loop forever), increment `sentinel_enrichment_invalid_events_total`.
2. Skip if `event` not in `{incident.opened, incident.recurred}`; commit.
3. Load `IncidentRow` via repository. Missing → WARN + commit (incident may have been deleted; not a system fault).
4. `ctx = await assemble(incident, deps)`.
5. `result = await repo.write_enrichment_context(incident_id, event_id, ctx, assembled_at, outbox_event=...)`.
6. If `result.status == "written"`: the `incident.enriched` `OutboxEvent` the consumer constructed and passed into step 5 was staged in the same tx. If `duplicate`: INFO log, `sentinel_enrichment_duplicates_total` increment. The consumer is the only place that constructs the outbox event — the repository just persists it.

### Outbox for `incident.enriched`

To keep the "publish + persist atomic" invariant from Phase 3, `write_enrichment_context` accepts an optional `OutboxEvent` and INSERTs it in the same `BEGIN/COMMIT` as the context UPDATE. The existing `OutboxDrainer` (Phase 3) publishes it. No new producer wiring needed.

### Backpressure & failure modes

- **Assemble takes > 30s** (all six fetchers hit timeout): still returns an IncidentContext with `degraded`/`failed` sections, never blocks. Kafka heartbeat survives because aiokafka heartbeats in a background task.
- **DB write fails**: offset not committed; re-deliver on next poll; the conditional UPDATE makes retry idempotent.
- **Outbox event write fails after context write**: impossible — both are in one tx.

### Worker lifecycle

Started **unconditionally** in `app.py` lifespan, matching the existing `OutboxDrainer` pattern (`sentinel/api/app.py:46-47`). A single Sentinel process runs the API plus the drainer plus the enrichment consumer; this is intentional for V1 (single-binary deploy). No new settings flag.

### Metrics (consumer-specific)

- `sentinel_enrichment_events_consumed_total{type}` — counter.
- `sentinel_enrichment_events_failed_total{reason}` — counter (parse, missing-incident, db, …).
- `sentinel_enrichment_duplicates_total` — counter.
- `sentinel_enrichment_invalid_events_total` — counter.
- `sentinel_enrichment_assemble_duration_seconds` — histogram (end-to-end, distinct from per-fetcher duration).

---

## 6. Failure modes, observability, tests

### Failure modes

Every code path articulates what goes wrong, what's logged, what's surfaced, and how it recovers — per the CLAUDE.md mantra.

| Path | Failure | Logged | Surfaced | Recovery |
|---|---|---|---|---|
| Fetcher timeout (>5s) | `asyncio.TimeoutError` inside breaker | WARN `fetcher_timed_out` `{fetcher, incident_id}` | `FetcherResult(status="failed", error="timeout")` | Breaker tracks; next event retries |
| Fetcher raises | exception | WARN `fetcher_errored` `{fetcher, incident_id, exc_type}` | `FetcherResult(status="failed", error=repr)` | Breaker tracks |
| Breaker open | short-circuit | DEBUG `breaker_short_circuit` `{fetcher}` | `FetcherResult(status="failed", error="circuit_open")` | After 30s cooldown, 1 trial |
| Adapter not configured (H, log, alerts) | default `NotConfigured*` returns degraded | once-per-process INFO `enrichment_adapter_not_configured` `{fetcher}` | `FetcherResult(status="degraded", error="not_configured")` | Wire real impl in H/integration phase |
| Event envelope invalid | Pydantic ValidationError | ERROR `enricher_invalid_envelope` `{offset}` | Metric increment; offset committed (poison pill) | Manual replay if needed |
| Incident missing in DB | repo returns None | WARN `enricher_unknown_incident` `{incident_id}` | Metric increment; offset committed | None — incident was deleted |
| Context write conflict | `event_id` already applied | INFO `enricher_duplicate` `{incident_id, event_id}` | Metric increment | Idempotent no-op |
| DB write fails (connectivity, deadlock) | sqlalchemy exception | ERROR `enricher_write_failed` exc_info | Offset **not** committed | At-least-once redelivery |
| Kafka consumer dies | aiokafka exception | ERROR + process exits | Process supervisor restarts (k8s/compose) | Resumes from last committed offset |

### Observability

#### Metrics

- `sentinel_enrichment_duration_seconds{fetcher}` — histogram (per fetcher).
- `sentinel_enrichment_assemble_duration_seconds` — histogram (end-to-end).
- `sentinel_enrichment_failures_total{fetcher,reason}` — counter (timeout / circuit_open / error / not_configured).
- `sentinel_enrichment_section_status_total{fetcher,status}` — counter (ok / degraded / failed).
- `sentinel_circuit_breaker_state{integration}` — gauge (0=closed, 1=open, 2=half_open).
- `sentinel_enrichment_events_consumed_total{type}` — counter.
- `sentinel_enrichment_events_failed_total{reason}` — counter.
- `sentinel_enrichment_duplicates_total` — counter.
- `sentinel_enrichment_invalid_events_total` — counter.

#### Tracing

OTel is configured (`sentinel/observability/tracing.py`) but no spans are emitted by Phase 3 code today. F is the first place to emit production spans:

- One span `enrichment.assemble` per incident with `incident.id` attribute.
- Child span per fetcher with `fetcher.name`, `fetcher.status`, `fetcher.duration_ms`.

The Phase 3 outbox producer (`sentinel/ingestion/kafka_producer.py`) does **not** currently set W3C tracecontext headers on outgoing Kafka messages. F adds this in the same PR: inject `traceparent` (and `tracestate` if present) into the Kafka message headers when the OutboxDrainer publishes. The enrichment consumer reads them back and uses them as the parent context for `enrichment.assemble`. This is a small, contained Phase 3 patch alongside the `event_id` patch.

#### Logging

Phase 3 code uses stdlib `logging` (e.g. `log.warning("msg", extra={...})`), not structlog event-name style — even though structlog is configured in `sentinel/observability/logging.py`. F **stays consistent with stdlib logging** to avoid mixing styles within the same release. Migrating the codebase to structlog event-name style is a separate cleanup, not part of F. F log lines still include `incident_id` and `event_id` via `extra={...}` so they remain queryable.

### Tests

#### Unit (`tests/unit/enrichment/`)

- `test_circuit_breaker.py` — state transitions, rolling-window pruning (inject clock), 5 failures in 61s does NOT open, half-open single-trial enforcement, cancellation does not count as failure.
- `test_orchestrator.py` — gather returns Exception (non-FetcherResult) → coerced to failed; one slow fetcher doesn't block others (use `asyncio.sleep` with mocked clock); per-fetcher timeout enforced; metrics emitted; IDs round-trip via `schemas/ids.py`.
- `test_fetchers/test_deploys.py`, `…related_alerts.py` — exercise the two real DB fetchers against `FakeDeployRepository` / `FakeIncidentRepository`.
- `test_fetchers/test_defaults.py` — `NotConfigured*` returns `degraded` with stable error.
- `test_consumer.py` — envelope validation, duplicate event_id is no-op, unknown incident is no-op-with-commit, db-write-failure leaves offset uncommitted (fake aiokafka).

#### Integration (`tests/integration/test_enrichment.py`, `@pytest.mark.integration`)

- End-to-end: produce `incident.opened` to Kafka → consumer runs → `context_json` populated → `incident.enriched` appears in outbox.
- Replay same event → version unchanged, duplicate metric increments.
- One fetcher hangs (delaying fake adapter) → context still persisted within ~6s, that section is `failed`.
- Breaker opens after repeated fetcher failures → metric reflects state.

### Quality gates per CLAUDE.md

- `make lint` / `make typecheck` / `make test` / `make test-integration` all green.
- New env vars: none expected for F itself (adapter env vars land with their respective adapters in later phases).
- Migration `0003` reversible — `make migrate-down && make migrate` round-trip tested in CI.
- ADR: not required for F — framework choices match the spec literally; no novel decisions warrant an ADR. If implementation deviates, we add one then.
- Mandatory subagent code review before commit (per [[sentinel-review-before-commit]] memory).

---

## Out of scope (deferred)

- **Real implementations of H-dependent fetchers (`similar_incidents`, `runbooks`)** — Protocols defined here; concrete `EmbeddingProvider`-backed implementations land in Work Area H (Memory & feedback loop).
- **Real log adapter (`recent_logs`)** and **active-alerts source-platform query (`active_alerts`)** — Protocols defined here; concrete adapters land with the relevant integration work area.
- **Pluggable per-source breaker configuration** (per-environment thresholds/windows) — F ships with the spec defaults (5/60s/30s). Adjustable via constructor for tests.
- **Backfill / re-enrichment of historical incidents** — manual replay via Kafka producer is sufficient for V1.
- **Three deferred Phase 3 review items** — drainer `outbox_event_stuck_total` semantics, fingerprint golden-table polish, Kafka-kill-then-restart integration test. To be filed as GitHub issues before F implementation starts.
