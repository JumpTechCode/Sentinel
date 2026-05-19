# Phase 3 — Webhook ingestion & integration adapters (Work Areas D + E)

**Status:** Design, ready for implementation plan
**Depends on:** A (foundation), B (persistence), C (schemas), J (observability) — all merged on `main`
**Unblocks:** F (enrichment) — webhook → Kafka event is the trigger that F consumes

## Goal

End-to-end vertical slice: a curl against `POST /webhooks/{source}` returns 202, persists an incident, and emits an `incident.opened` (or `incident.recurred`) event to Kafka — all under a strict failure-mode contract (HMAC verify, Redis idempotency, fingerprint-based dedup, transactional outbox).

Scope is **webhook adapters only** for Work Area D. `LogFetcher` protocol + log fetchers (sentry/datadog/loki) + github deploy fetcher are deferred to Work Area F, where they're actually consumed.

## Architecture

### Module layout

```
sentinel/
├── integrations/
│   ├── base.py              # WebhookAdapter Protocol, HMAC helper, errors
│   ├── severity_map.py      # per-source → SeverityType mapping tables
│   ├── sentry.py
│   ├── pagerduty.py
│   ├── datadog.py
│   ├── generic.py
│   └── registry.py          # ADAPTERS: dict[str, WebhookAdapter]
├── ingestion/
│   ├── fingerprint.py       # normalize_title, fingerprint
│   ├── idempotency.py       # Redis SETNX wrapper
│   ├── kafka_producer.py    # AIOKafkaProducer wrapper (used by drainer only)
│   ├── outbox_drainer.py    # background task: outbox → Kafka
│   └── webhook.py           # orchestration: verify→idempotent→normalize→ingest→202
├── api/routes/webhooks.py   # thin POST /webhooks/{source}
└── persistence/
    ├── models.py            # + OutboxEventModel
    ├── repositories.py      # + IncidentRepository.ingest(), OutboxRepository
    └── (no schema changes to existing tables)

migrations/versions/0002_outbox_events.py

tests/
├── unit/
│   ├── integrations/{source}/test_normalize.py, test_signature.py
│   ├── integrations/test_severity_map.py
│   └── ingestion/test_fingerprint.py, test_idempotency.py,
│                 test_kafka_producer.py, test_outbox_drainer.py,
│                 test_webhook_orchestration.py
├── integration/
│   ├── ingestion/conftest.py      # Postgres + Redis + Kafka testcontainers
│   ├── ingestion/test_webhook_endpoint.py
│   ├── ingestion/test_outbox_drainer.py
│   └── ingestion/test_migration_0002.py
└── fixtures/webhooks/{sentry,pagerduty,datadog,generic}.json
```

### Request lifecycle: `POST /webhooks/{source}`

```
1. Registry lookup on `source`               → 404 if unknown
2. Read raw body bytes (needed for HMAC + idempotency hash)
3. adapter.verify_signature(headers, body, secret)
     - Secret unset for this source → 401 + structured log (never silently accept)
     - Mismatch                     → 401 (no further work)
4. idempotency.check_and_mark(source, body)
     - SETNX webhook:{source}:{sha256(body)} TTL 24h
     - Duplicate → 200 {status:"duplicate", incident_id?:null}
     - Redis down → 503 (fail closed; source will retry)
5. Parse JSON; adapter.normalize(payload) → NormalizedAlert
     - ValidationError / AdapterParseError → 422
6. fingerprint(alert.service, normalize_title(alert.title), alert.severity)
7. incident_repo.ingest(alert, fingerprint, outbox_topic)
     - one DB tx: dedup-window lookup + insert-or-update-incident + outbox.enqueue
     - returns IngestResult{incident_id, event_kind: "opened"|"recurred"}
8. Return 202 {status: event_kind, incident_id}

Background (separately, in lifespan task):
  Outbox drainer polls every 250ms, claims rows under SKIP LOCKED, emits to Kafka,
  marks published. Webhook request path never touches Kafka.
```

**Failure modes (named per the "articulate failure mode" mantra):**

| Stage | Failure | HTTP | DB state | Recovery |
|---|---|---|---|---|
| 1 unknown source | 404 | clean | source retries / 4xx, no-op |
| 3 bad sig / missing secret | 401 | clean | sender fixes |
| 4 Redis down | 503 | clean | sender retries (Kafka not reached) |
| 5 bad JSON / shape | 422 | clean | sender fixes |
| 7 DB error | 500 | tx rollback | sender retries |
| 8 (background) Kafka down | n/a | outbox row stays unpublished, attempts++ | drainer retries with backoff |
| 8 (background) Kafka stuck > N attempts | n/a | row remains, metric fires | operator action |

The "single dual-write problem" (DB committed, Kafka emit failed) is dissolved by the outbox pattern: the route never touches Kafka. Kafka outage delays enrichment; it does not lose events.

## Architectural decision: transactional outbox

We adopt the **transactional outbox pattern**: webhook handler writes incident + outbox row in one Postgres transaction; a background drainer publishes outbox rows to Kafka with at-least-once semantics.

**Alternatives considered:**
- *Emit-then-commit:* simpler but loses events on Kafka outage. Acceptable for prototypes; not for a CTO-level portfolio repo whose audience asks "what about the dual-write problem?" within five seconds.
- *Commit-then-emit, log on failure:* same loss profile, with a metric. Same problem.

**Cost:** one new table, one repository, one background task, one migration, ~250ms additional p99 latency from webhook→Kafka under nominal conditions. Reusable substrate when diagnosis or memory later emit events.

This decision warrants an ADR landing with this PR: `docs/adr/0001-transactional-outbox.md`. (User: ADRs live under `docs/adr/` per spec; only specs/plans go in `plans/`.)

## Component design

### `sentinel/integrations/base.py`

```python
class WebhookAdapter(Protocol):
    source: ClassVar[str]
    signature_headers: ClassVar[tuple[str, ...]]

    def verify_signature(
        self,
        headers: Mapping[str, str],
        body: bytes,
        secret: SecretStr,
    ) -> bool: ...

    def normalize(self, payload: Mapping[str, Any]) -> NormalizedAlert: ...


class AdapterParseError(Exception): ...
class MissingSecretError(Exception): ...


def compute_hmac_sha256(body: bytes, secret: bytes) -> str:
    """Hex digest; compare with hmac.compare_digest only."""
```

- `verify_signature` returns bool, never raises.
- `compare_digest` is the only allowed comparison primitive.
- All adapters consume `body: bytes`; signature is computed over the raw bytes (not the parsed JSON), so the route must capture body before parsing.

### Per-source signature schemes

| Source | Header | Algorithm | Wire format | Notes |
|---|---|---|---|---|
| Sentry | `Sentry-Hook-Signature` | HMAC-SHA256(secret, body) | raw hex | Single hex string. |
| PagerDuty | `X-PagerDuty-Signature` | HMAC-SHA256(secret, body) | `v1=<hex>,v1=<hex>` | Strip `v1=` prefix; accept if any element matches. Reject unknown version tags. |
| Datadog | `X-Datadog-Signature` + `X-Datadog-Timestamp` | HMAC-SHA256(secret, timestamp + body) | raw hex | Timestamp included in signed payload; reject if older than 5 min (clock skew). |
| Generic | `X-Sentinel-Signature` | HMAC-SHA256(secret, body) | `sha256=<hex>` (GitHub-style) | Our own scheme — familiar curl-able form. |

### `NormalizedAlert` mapping per source

`NormalizedAlert` fields: `source, external_id, service, severity, title, received_at, raw_payload` (per `sentinel/schemas/alert.py:18-25` — no `description` field). `received_at` validator requires tz-aware datetime.

| Field | Sentry | PagerDuty | Datadog | Generic |
|---|---|---|---|---|
| `source` | const `"sentry"` | const `"pagerduty"` | const `"datadog"` | const `"generic"` |
| `external_id` | `data.issue.id` (or `data.event.event_id`) | `incident.id` | `alert_id` | `id` |
| `service` | `data.issue.project.slug` | `incident.service.summary` | parsed from `tags` (`service:<x>`) | `service` |
| `severity` | severity_map.sentry(`level`) | severity_map.pagerduty(`urgency`) | severity_map.datadog(`alert_priority`) | direct (validated against enum) |
| `title` | `data.issue.title` | `incident.title` | `event_title` | `title` |
| `received_at` | `datetime.now(UTC)` | `datetime.now(UTC)` | parsed from `X-Datadog-Timestamp` as UTC | `datetime.now(UTC)` |
| `raw_payload` | full payload | full payload | full payload | full payload |

`severity_map.py` exposes a function per source mapping vendor severity → `SeverityType`. Unknown vendor values map to `SEV3` with a `log.warning` (documented default; not silent).

### `sentinel/ingestion/fingerprint.py`

```python
def normalize_title(s: str) -> str: ...
def fingerprint(service: str, normalized_title: str, severity: str) -> str: ...
```

`normalize_title` applies, in order:
1. Lowercase.
2. Strip RFC3339 / ISO-8601 timestamps.
3. Strip UUIDs.
4. Strip hex IDs ≥8 chars (commit SHAs, request IDs).
5. Strip standalone integers ≥4 digits.
6. Strip URLs and bracketed `[ctx]` segments.
7. Collapse whitespace, trim.

`fingerprint` = `sha256(f"{service}\x1f{normalized_title}\x1f{severity}".encode()).hexdigest()`. `\x1f` (unit separator) prevents delimiter collisions.

**Tests:** golden table (20+ adversarial titles) + hypothesis property (UUID/timestamp permutations produce stable fingerprints; differing service/severity produces differing fingerprints).

### `sentinel/ingestion/idempotency.py`

```python
class IdempotencyStore(Protocol):
    async def check_and_mark(self, source: str, body: bytes) -> bool: ...
    # returns True if already seen
```

Postgres-free; Redis only. Key: `webhook:{source}:{sha256(body).hexdigest()}`. TTL 24h. Implementation uses `redis.asyncio` with `SET key value NX EX 86400`.

### `IncidentRepository.ingest()` — the atomicity primitive

The existing repository methods (`sentinel/persistence/repositories.py:43-49`) each open their own session, which prevents incident-insert and outbox-insert from sharing a transaction. We add a single orchestration method that does dedup + persist + outbox enqueue in one tx:

```python
@dataclass(frozen=True, slots=True)
class IngestResult:
    incident_id: UUID
    event_kind: Literal["opened", "recurred"]


class IncidentRepository(Protocol):
    # ... existing methods unchanged ...

    async def ingest(
        self,
        alert: NormalizedAlert,
        *,
        fingerprint: str,
        outbox_topic: str,
        payload_hash: str,
    ) -> IngestResult: ...
```

Concrete `PostgresIncidentRepository.ingest` flow (single tx):

```sql
-- 1. Dedup-window lookup, locked
SELECT id, raw_payload FROM incidents
 WHERE fingerprint = $1
   AND status NOT IN ('resolved','closed')
   AND opened_at > now() - interval '1 hour'
 ORDER BY opened_at DESC
 LIMIT 1
 FOR UPDATE SKIP LOCKED;

-- 2a. HIT → bump occurrence + append event_log inside raw_payload._sentinel
UPDATE incidents SET raw_payload = jsonb_set(...)
 WHERE id = $hit.id;
-- enqueue outbox row with event = "incident.recurred"
INSERT INTO outbox_events (topic, key, payload) VALUES (...);
-- event_kind = "recurred"

-- 2b. MISS → insert incident with _sentinel.occurrence_count = 1
INSERT INTO incidents (...) RETURNING id;
-- enqueue outbox row with event = "incident.opened"
INSERT INTO outbox_events (topic, key, payload) VALUES (...);
-- event_kind = "opened"
```

`raw_payload._sentinel` namespace holds `{occurrence_count, last_seen_at, event_log: [{ts, source, payload_hash}]}`. Read-modify-write inside the `FOR UPDATE` lock (the row is exclusively locked; no race). No schema change to `incidents`.

**Rationale for the orchestration method:** the existing repository shape opens a session per call; we cannot span the two writes from the ingestion module without leaking session management out of `persistence/`. The spec invariant ("non-`persistence/` modules never touch the DB directly") rules out Unit-of-Work patterns that would expose sessions to callers. A named orchestration method keeps SQL inside `persistence/` and gives callers a single atomic primitive. The method name `ingest` is honest: it dedup-locates + persists + emits-intent in one logical operation.

### `OutboxRepository` and migration 0002

New table `outbox_events`:

```python
op.create_table(
    "outbox_events",
    sa.Column("id", PgUUID(as_uuid=True), primary_key=True,
              server_default=sa.text("gen_random_uuid()")),
    sa.Column("topic", sa.Text, nullable=False),
    sa.Column("key", sa.Text, nullable=False),
    sa.Column("payload", JSONB, nullable=False),
    sa.Column("created_at", TIMESTAMP(timezone=True),
              nullable=False, server_default=sa.func.now()),
    sa.Column("published_at", TIMESTAMP(timezone=True), nullable=True),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_attempt_at", TIMESTAMP(timezone=True), nullable=True),
    sa.Column("last_error", sa.Text, nullable=True),
)
op.create_index(
    "idx_outbox_unpublished",
    "outbox_events",
    [sa.text("created_at")],
    postgresql_where=sa.text("published_at IS NULL"),
)
```

Partial index keeps the drainer's scan tight as published rows accumulate. Published-row pruning is a separate concern (operator cron, not in this PR).

`downgrade()` drops index then table. Tested via integration round-trip.

Repository contract:

```python
@dataclass(frozen=True, slots=True)
class OutboxEvent:
    id: UUID
    topic: str
    key: str
    payload: dict[str, Any]
    attempts: int


class OutboxRepository(Protocol):
    async def enqueue(
        self, *, topic: str, key: str, payload: dict[str, Any],
        session: AsyncSession | None = None,  # optional: piggyback on caller's tx
    ) -> UUID: ...

    def claim_batch(self, *, limit: int) -> AsyncContextManager[OutboxBatch]: ...


class OutboxBatch:
    events: list[OutboxEvent]
    def mark_published(self, id_: UUID) -> None: ...
    def mark_failed(self, id_: UUID, *, error: str) -> None: ...
    # __aexit__ commits all marks atomically; tx scope = batch lifetime
```

`enqueue(session=...)` accepts an external session so `IncidentRepository.ingest` can write the outbox row inside the incident's transaction. `claim_batch` is a context manager because `FOR UPDATE SKIP LOCKED` only holds locks until commit; we need the drainer's "claim → emit → mark" cycle inside one tx.

### `sentinel/ingestion/kafka_producer.py`

```python
class KafkaProducer:
    def __init__(self, brokers: str): ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...   # flush + stop
    async def emit(self, *, topic: str, key: str,
                   payload: dict[str, Any]) -> None: ...
```

`AIOKafkaProducer` config: `enable_idempotence=True`, `acks="all"`, `compression_type="gzip"`, `max_in_flight_requests_per_connection=5`, `linger_ms=10`. Key is the incident UUID string (consistent partitioning). Value is `json.dumps(payload).encode()`.

Used **only** by the outbox drainer; never imported by the route.

### `sentinel/ingestion/outbox_drainer.py`

Background task started in app `lifespan`:

```python
class OutboxDrainer:
    def __init__(self, *, outbox_repo, producer, settings):
        self.poll_interval = 0.25     # 250ms
        self.batch_size = 100
        self.emit_timeout = 5.0
        self.max_attempts = 10

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._tick()
            except Exception:
                log.exception("outbox_drainer_tick_failed")
            await asyncio.wait_for(self._stop.wait(),
                                   timeout=self.poll_interval, ...)

    async def _tick(self) -> None:
        async with self.outbox_repo.claim_batch(limit=self.batch_size) as batch:
            for event in batch.events:
                if event.attempts >= self.max_attempts:
                    continue   # stuck; metric will fire
                try:
                    await asyncio.wait_for(
                        self.producer.emit(
                            topic=event.topic, key=event.key, payload=event.payload),
                        timeout=self.emit_timeout)
                    batch.mark_published(event.id)
                except Exception as e:
                    batch.mark_failed(event.id, error=repr(e))
```

Backoff: `claim_batch` query filters `(last_attempt_at IS NULL OR last_attempt_at < now() - interval '<computed>')` where `<computed>` = `LEAST(2^attempts, 300)` seconds. Stuck rows (`attempts >= 10`) are visible but skipped; metric `sentinel_outbox_event_stuck_total` fires per scan.

Lifecycle: started in `lifespan`, stopped on shutdown with timeout. Tx commits inside each `_tick`; shutdown between ticks is clean.

### Settings additions

```python
class Settings(BaseSettings):
    # ... existing ...
    sentry_webhook_secret: SecretStr | None = None
    pagerduty_webhook_secret: SecretStr | None = None
    datadog_webhook_secret: SecretStr | None = None
    generic_webhook_secret: SecretStr | None = None
```

`extra="ignore"` already set (`settings.py:55`) — fields are additive. Each is `None` by default; adapter rejects 401 when secret is missing (never silently accepts).

Mirror in `.env.example` (empty values) and `config/dev.yaml`.

### `WebhookAcceptedResponse` schema amendment (C)

Current (`sentinel/schemas/api.py:30`):

```python
class WebhookAcceptedResponse(BaseModel):
    status: Literal["accepted", "duplicate"]
    incident_id: UUID | None = None
```

Add `"recurred"`:

```python
status: Literal["accepted", "recurred", "duplicate"]
```

`"accepted"` = new incident opened; `"recurred"` = dedup hit, existing incident updated; `"duplicate"` = same body seen within 24h (Redis idempotency).

## Observability

Metrics (registered via Work Area J's `sentinel/observability/metrics.py` patterns):

- `sentinel_webhooks_received_total{source,outcome}` — outcome ∈ {accepted, recurred, duplicate, unauthorized, bad_request, unknown_source}
- `sentinel_webhook_handler_duration_seconds{source}` (histogram)
- `sentinel_outbox_events_enqueued_total{topic}`
- `sentinel_outbox_events_published_total{topic}`
- `sentinel_outbox_events_failed_total{topic}`
- `sentinel_outbox_publish_latency_seconds{topic}` (histogram: `created_at` → `published_at`)
- `sentinel_outbox_unpublished_count` (gauge)
- `sentinel_outbox_oldest_unpublished_age_seconds` (gauge)
- `sentinel_outbox_event_stuck_total` (counter; fires per drainer scan with stuck rows)

Structured logs:

- Webhook: `{source, fingerprint, incident_id, outcome, latency_ms}`
- Signature failure: `{source, header_present, reason}`
- Drainer cycle: `{batch_size, published, failed, stuck}`

## Testing strategy

### Unit (no infra)

- `integrations/{source}/test_normalize.py` — committed fixture → expected `NormalizedAlert`.
- `integrations/{source}/test_signature.py` — positive + tampered + missing-header + wrong-secret + scheme-specific negatives (PagerDuty multi-sig, Datadog stale timestamp).
- `integrations/test_severity_map.py` — every vendor value maps to valid `SeverityType`; unknown → SEV3.
- `ingestion/test_fingerprint.py` — golden table + hypothesis properties.
- `ingestion/test_idempotency.py` — `fakeredis` for SETNX semantics, TTL set, duplicate detection.
- `ingestion/test_kafka_producer.py` — `AIOKafkaProducer` mocked: config, key/value serialization, lifecycle.
- `ingestion/test_outbox_drainer.py` — repository + producer mocked: claim→emit→publish, failure→mark-failed+backoff, stop responsiveness, attempts cap.
- `ingestion/test_webhook_orchestration.py` — handler logic with mocks: failure-mode table (one test per row).

### Integration (testcontainers: Postgres + Redis + Kafka)

- `tests/integration/ingestion/conftest.py` — extends existing pg fixture with `RedisContainer` and `KafkaContainer`.
- `test_webhook_endpoint.py`:
  - Happy path: POST sentry fixture → 202, incident row, outbox row enqueued.
  - Duplicate body: second POST → 200 duplicate, no second incident, no second outbox row.
  - Same fingerprint within 1h, different body: second updates first incident (`_sentinel.occurrence_count == 2`, `event_log` has 2 entries), `recurred` outbox row.
  - Same fingerprint after 1h: new incident (use direct insert to backdate the first).
  - Bad signature → 401, no DB state, no Redis key.
  - Unknown source → 404.
  - Missing secret → 401 + log.
  - Malformed JSON → 422.
- `test_outbox_drainer.py`:
  - Insert outbox rows, start drainer, assert Kafka receives, `published_at` set.
  - Kill Kafka mid-test → attempts increment, no rows lost; restart → rows drain.
  - Concurrent drainers (two instances) → no duplicate emits (verified by consumer count).
- `test_migration_0002.py` — upgrade/downgrade clean; round-trip insert/claim/publish.

### Smoke (extends `tests/integration/test_smoke.py`)

- `test_e2e_webhook.py` — fire each source fixture against a running app (uses existing `settings()` fixture that skips when env unset); assert 202 + Kafka receives within 2s.

## Acceptance criteria

The PR is mergeable when:

1. `make lint && make typecheck && make test && make test-integration` all green.
2. Coverage ≥ 90% on `sentinel/integrations/` and `sentinel/ingestion/`; project baseline ratchet allowed.
3. Every adapter ships with a committed fixture + parse test + signature test (positive + ≥3 negatives).
4. End-to-end demo: `make compose-up && curl -X POST -H "Sentry-Hook-Signature: <hex>" --data @tests/fixtures/webhooks/sentry.json localhost:8000/webhooks/sentry` returns 202; second invocation returns 200 duplicate; `psql` shows the incident; a Kafka consumer (kcat or aiokafka in a one-off script) shows the `incident.opened` event.
5. Webhook handler unit-level timing test asserts < 50ms median with mocked infra (proxy for the fast-202 contract; full load test deferred to Work Area M).
6. Bad signature, missing secret, unknown source, malformed JSON all return their documented codes and emit the right metric labels.
7. `alembic upgrade head` + `alembic downgrade -1` clean on migration 0002.
8. ADR `docs/adr/0001-transactional-outbox.md` committed alongside the implementation.
9. No `dict[str, Any]` on the webhook route's request/response models; Pydantic types throughout.
10. New env vars added to `Settings`, `.env.example`, and `config/dev.yaml`.

## Non-goals for this PR

- `LogFetcher` protocol and log fetchers (sentry/datadog/loki) — deferred to F.
- GitHub deploy fetcher — deferred to F.
- Outbox row pruning / archival — separate operator concern.
- Real load test (`make load` placeholder) — Work Area M.
- Reconciler for orphan incidents (incidents with no outbox row) — outbox prevents this; not needed.
