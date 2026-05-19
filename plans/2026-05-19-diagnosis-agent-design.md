# Diagnosis agent — single-shot LLM reasoning over pre-assembled context (Work Area G) — design

**Status:** approved (brainstorm), implementation plan to follow.
**Date:** 2026-05-19.
**Scope:** Work Area G per `.claude/program-of-work.md` §G. Plan size: L. Depends on B/C/F (all landed).

Phase 5 of Sentinel. Work Area G is the second technical centerpiece: it consumes the `incident.enriched` event emitted by Phase 4, runs a single-shot LLM call over the pre-assembled `IncidentContext`, validates the structured output against the schema and against the supplied context (the headline quality signal), persists the diagnosis, and emits `incident.diagnosed` downstream. No HTTP surface in this phase — that lands in Work Area I and calls the same agent core.

### Sibling patches landing with G

A fact-check against the current code surfaced four concrete additions that don't fit Work Area G's module boundary but are needed for its acceptance. They land in the same PR:

1. **`diagnoses` table gets `hallucinated_evidence` column and a unique constraint.** Migration `0004_diagnoses_idempotency.py` — adds `hallucinated_evidence BOOLEAN NOT NULL DEFAULT false` and `UNIQUE (incident_id, prompt_version, model)`. Reversible. Phase 2 created the table; this patch makes it usable by G.
2. **`hallucinated_evidence_rate` gauge → counter.** `sentinel/observability/metrics.py:95-98` registers a gauge today. No callers anywhere. We remove the gauge and register `sentinel_hallucinated_evidence_total` (Counter) and `sentinel_diagnoses_total{status}` (Counter) as the denominator. Documented spec deviation — spec §quality calls it a "rate"; we expose the primitives and let Grafana compute.
3. **`DiagnosisRepository` Protocol gets a real signature.** Current Protocol (`sentinel/persistence/repositories.py:745-757`) takes a `Diagnosis` Pydantic model whose `evidence` field has `min_length=1`. That makes a 100%-hallucinated case unpersistable. We replace `save(...)` with `save_with_outbox(*, incident_id, record: PersistedDiagnosis, upstream_event_id, outbox_event) -> tuple[UUID, Literal["new","duplicate"]]`. No concrete impl exists yet, so no production callers break.
4. **`anthropic` SDK is already pinned** (`pyproject.toml:28` → `anthropic>=0.39,<0.40`). No dep change.

### Deliberate non-goals in G

- No HTTP / SSE endpoint (Work Area I).
- No W3C `traceparent` injection across services — not in code today, and stitching `webhook → enrichment → diagnosis` traces is a small cross-cutting follow-up issue, not part of G. Spec doesn't mandate it.
- No per-LLM circuit breaker — only one upstream provider; revisit when a second lands. ADR placeholder.
- No SSE streaming fan-out to other consumers. Internal streaming foundations land in G; emission to clients is Work Area I.
- No "re-diagnose with newer prompt" automation. The unique constraint allows it; orchestration lands later.
- No enforced cost cap — observed, not enforced.

---

## 1. Module layout & responsibilities

```
sentinel/diagnosis/
├── __init__.py              # public: diagnose(), DiagnosisAgent, error types
├── agent.py                 # diagnose(incident, context, deps) -> PersistedDiagnosis
├── llm_client.py            # AnthropicClient wrapper (streaming, tool-use, timeout)
├── prompt.py                # load + hash v1.md; serialize(incident, context) -> str
├── prompts/
│   ├── v1.md                # versioned system prompt (rubric + tool contract)
│   └── v1.sha256            # baseline hash, checked at startup
├── validation.py            # evidence-citation gate (verify EvidenceRef.id ∈ context)
├── truncation.py            # context truncation strategy under input-token cap
├── consumer.py              # aiokafka consumer for `incident.enriched`
├── deps.py                  # DiagnosisDeps dataclass: client, repos, settings
└── persisted.py             # PersistedDiagnosis dataclass (no min_length constraints)

migrations/versions/
└── 0004_diagnoses_idempotency.py  # +hallucinated_evidence col, +unique(incident_id, prompt_version, model)

sentinel/persistence/repositories.py
└── PostgresDiagnosisRepository    # concrete impl; INSERT ON CONFLICT DO NOTHING, atomic outbox enqueue

sentinel/observability/metrics.py
└── (revised diagnosis metrics — see §6)
```

### Boundary rules

- `agent.diagnose(incident, context, deps) -> PersistedDiagnosis` is pure-async and stateless. No DB, no Kafka. Takes the fully-formed `IncidentContext` and `IncidentDetailResponse`, calls the LLM, validates, returns.
- The consumer owns all I/O: loads `(IncidentDetailResponse, IncidentContext)` via the two existing repository methods, calls the agent, persists via `DiagnosisRepository.save_with_outbox(...)`.
- `llm_client.py` is the **only** file that imports `anthropic`. Everything else uses the wrapper interface.
- No HTTP surface — `POST /incidents/{id}/diagnose` and SSE land in Work Area I and call the same `agent.diagnose()`.

---

## 2. Prompt versioning & structured output

### `prompts/v1.md` — checked into git

- Loaded once at startup, SHA-256'd. The hash is compared against `prompts/v1.sha256` (also checked in); mismatch logs a WARN. Modifying `v1.md` without regenerating `v1.sha256` is a tripwire that fires loudly in dev/CI.
- The hash is stamped on every persisted diagnosis as part of `token_usage` JSONB (`{"prompt_sha": "<hex>"}`) alongside `prompt_version='v1'` (existing column).
- Bumping the prompt = `cp v1.md v2.md`, edit, regenerate `v2.sha256`, change `Settings.diagnosis_prompt_version='v2'`. Avoids in-place edits.

### Prompt contents (v1)

1. **Role**: SRE diagnosis agent. Single-shot, reason over supplied context only, never invent facts.
2. **Confidence rubric** verbatim (0.0–0.3 speculation / 0.4–0.6 plausible / 0.7–0.85 strong evidence / 0.86–1.0 direct causal link in evidence). This is encoded in the system prompt because the spec invariant says it must be.
3. **Citation rules**: every claim must cite a context ID using the literal token format already produced by `IncidentContext` serialization: `[deploy:<sha>]`, `[similar:<uuid>]`, `[runbook:<uuid>]`, `[log:<idx>]`, `[related:<uuid>]`. Inventing an ID will cause the diagnosis to be flagged and confidence capped at 0.4.
4. **Tool contract**: must call `submit_diagnosis` exactly once with the `Diagnosis` schema. Free text replies are an error.

### User message — `prompt.serialize(incident, context) -> str`

The serializer takes both an `IncidentDetailResponse` (for service/severity/title/opened_at; `IncidentContext` doesn't carry these) and the `IncidentContext` (for the six fetcher sections).

```
INCIDENT
  service:  payments-api
  severity: SEV1
  title:    "Elevated 5xx errors..."
  opened:   2026-05-19T13:42:00Z
  fingerprint: <sha>

DEPLOYS (last 60min) — status=ok
  [deploy:abc123] payments-api @ 2026-05-19T13:38Z by alice
    PR #4421 "Switch idempotency key store to Redis Cluster"
    diff: src/idempotency/{store.py,redis_cluster.py}

SIMILAR INCIDENTS (top 3 by cosine) — status=degraded (embedding provider unavailable)
  [similar:0193-...] cosine=0.81
    title: "5xx after Redis client upgrade"
    root_cause: "connection pool exhausted on TLS rotation"
    remediation: "downgrade redis-py to 5.0.x"

[ runbooks ... related_alerts ... recent_logs ... active_alerts ... ]
```

Each section header includes its `FetcherStatus` so the model can see what's missing and lower confidence accordingly. The prompt instructs it explicitly: "When a relevant section is `degraded` or `failed`, treat your hypothesis as less supported and lower confidence."

### Structured output — Anthropic tool-use, forced single tool

```python
TOOL_SCHEMA = {
    "name": "submit_diagnosis",
    "description": "Submit your structured diagnosis.",
    "input_schema": Diagnosis.model_json_schema(),  # from sentinel/schemas/diagnosis.py
}
# client.messages.stream(..., tools=[TOOL_SCHEMA],
#                        tool_choice={"type": "tool", "name": "submit_diagnosis"})
```

The model must call the tool. We never parse free text. The tool input goes straight into `Diagnosis.model_validate(tool_use.input)`. On `ValidationError`, retry once with the error string appended as a corrective user turn. Second failure → `DiagnosisInvalid`; the consumer logs, increments `sentinel_diagnosis_failures_total{reason="schema"}`, and commits the offset (no infinite retry on a permanently bad prompt+context combo).

---

## 3. LLM client wrapper, streaming, truncation, timeout

### `llm_client.py`

```python
class AnthropicClient:
    def __init__(
        self,
        *,
        api_key: SecretStr,
        model: str,
        timeout_s: float = 30.0,
        max_output_tokens: int = 2048,
        client: AsyncAnthropic | None = None,   # injectable for tests
    ): ...

    async def diagnose_call(
        self,
        *,
        system: str,
        user: str,
        tool_schema: dict[str, Any],
        tool_name: str,
    ) -> LLMResult:
        """Single streaming call with forced tool-use.

        Returns LLMResult with tool_input (dict), input_tokens, output_tokens,
        stop_reason, latency_ms.
        Raises LLMTimeout, LLMTransport, LLMNoToolCall.
        """
```

**Streaming.** Uses `client.messages.stream(...)` (anthropic SDK 0.39 async context manager). We consume events until `MessageStopEvent`, capture the final tool block's `input`, and pull token usage from `MessageStopEvent.usage`. Internally streaming; we expose only the final result to callers. SSE fan-out to HTTP clients lands in Work Area I and will build on this same client.

**Timeout.** `asyncio.wait_for(self._stream_once(...), timeout=self.timeout_s)`. On `TimeoutError` → raise `LLMTimeout`. **No retry on timeout** — Kafka redelivery is the safety net at the consumer level.

**Transport retry inside the client.** HTTP 5xx and 429 get one short backoff (0.5s) inside the client before bubbling up. No other retries here — the schema-invalid retry policy lives in `agent.py`. The client stays dumb on purpose.

**Why no per-LLM circuit breaker for V1**: one upstream provider only. Spec invariant 1 ("every external call has a timeout and a circuit breaker") is partially honored — timeout + transport-level short backoff exist. A per-process breaker is wired when a second provider lands. ADR-pending.

### `truncation.py` — standalone, unit-testable without an LLM

```python
def truncate_for_budget(
    ctx: IncidentContext,
    *,
    max_input_tokens: int,
) -> tuple[IncidentContext, TruncationStats]: ...
```

- Token estimate: `len(text) // 4` heuristic with a 10% safety margin. The `anthropic` SDK at 0.39 does not expose a local tokenizer. Heuristic is fine for V1 — we observe truncation stats and tighten if reality drifts from the model.
- **Drop order when over budget**: `recent_logs` (oldest first) → `related_alerts` (oldest first) → `runbooks` (lowest cosine first) → `similar_incidents` (lowest cosine first) → `deploys` (oldest first, but always keep the newest deploy per service).
- Always preserve ≥ 1 item per section if the section had any (so the model sees the section exists, even if degraded by truncation).
- `TruncationStats` records per-section drop counts → emitted as `extra={}` on `diagnosis_completed` log and as `sentinel_diagnosis_input_truncated_total{section}` counter.

Default `max_input_tokens=12_000`. Output `max_tokens=2048`. At sonnet-4-5 list rates ($3/M in, $15/M out) the cost ceiling per call is ~$0.07.

### Token + cost accounting

After every call (success or failure):
- Record `input_tokens`, `output_tokens`. Compute `usd_cost(model, in, out)` via the existing helper in `sentinel/observability/cost.py`.
- Bump `sentinel_llm_cost_usd_total{model}` (existing Counter).
- Persist to `diagnoses.token_usage` JSONB: `{"input": int, "output": int, "cost_usd": "0.0123", "prompt_sha": "<hex>", "truncated": {"logs": 12, ...}}`.

---

## 4. Validation gate, agent flow, persistence

### `validation.py` — the headline quality signal

```python
def verify_evidence(
    diagnosis: Diagnosis,
    context: IncidentContext,
) -> EvidenceVerdict: ...

@dataclass(frozen=True)
class EvidenceVerdict:
    verified:  list[EvidenceRef]   # citations that resolved to a context ID with matching kind
    invented:  list[EvidenceRef]   # everything else
    hallucinated: bool             # True iff invented is non-empty
```

Implementation: walk `context` once to build `dict[EvidenceKind, set[str]]` from the IDs each fetcher emits. Bucket-match by `EvidenceRef.kind`: wrong-kind references are counted as invented even when the bare ID exists somewhere else in the context.

ID inventory built from `IncidentContext`:
- `deploy:<sha>` from `recent_deploys.data[*].id`
- `related:<uuid>` from `related_alerts.data[*].id` and `active_alerts.data[*].id`
- `similar:<uuid>` from `similar_incidents.data[*].id`
- `runbook:<uuid>` from `runbooks.data[*].id`
- `log:<idx>` from `recent_logs.data[*].id`

### `persisted.py` — the unconstrained record

```python
@dataclass(frozen=True, slots=True)
class PersistedDiagnosis:
    hypothesis:      str
    confidence:      Decimal       # already capped if hallucinated
    reasoning:       str
    evidence:        list[EvidenceRef]   # verified only; may be empty
    suggested_actions: list[SuggestedAction]
    likely_category: CategoryType
    hallucinated_evidence: bool
    model:           str
    prompt_version:  str
    latency_ms:      int
    token_usage:     dict[str, Any]      # includes prompt_sha, truncated stats
```

Needed because `Diagnosis.evidence` has `min_length=1` and a 100%-hallucinated case can't be represented after we drop the invented citations. `PersistedDiagnosis` is the boundary record between the agent and the repository; the wire-level Pydantic `Diagnosis` model still enforces `min_length=1` on what the LLM is allowed to output (and the retry-once correction handles the case where the model gives us zero citations).

### `agent.py` flow

```python
async def diagnose(
    incident: IncidentDetailResponse,
    context: IncidentContext,
    deps: DiagnosisDeps,
) -> PersistedDiagnosis:
    ctx, trunc = truncate_for_budget(context, max_input_tokens=deps.max_input_tokens)
    system = deps.prompt.system_text          # cached
    user   = prompt.serialize(incident, ctx)

    for attempt in (1, 2):
        try:
            result = await deps.llm.diagnose_call(
                system=system, user=user,
                tool_schema=TOOL_SCHEMA, tool_name="submit_diagnosis",
            )
            d = Diagnosis.model_validate(result.tool_input)
            break
        except ValidationError as e:
            if attempt == 2:
                raise DiagnosisInvalid(str(e))
            user = user + f"\n\nYour previous response was invalid: {e}.\nReturn a valid submit_diagnosis call."

    verdict = verify_evidence(d, ctx)
    confidence = min(d.confidence, Decimal("0.4")) if verdict.hallucinated else d.confidence

    diagnosis_latency_seconds.observe(result.latency_ms / 1000.0)
    diagnosis_confidence.observe(float(confidence))
    if verdict.hallucinated:
        hallucinated_evidence_total.inc()

    return PersistedDiagnosis(
        hypothesis=d.hypothesis,
        confidence=confidence,
        reasoning=d.reasoning,
        evidence=verdict.verified,
        suggested_actions=d.suggested_actions,
        likely_category=d.likely_category,
        hallucinated_evidence=verdict.hallucinated,
        model=deps.llm.model,
        prompt_version=deps.prompt.version,
        latency_ms=result.latency_ms,
        token_usage={
            "input":  result.input_tokens,
            "output": result.output_tokens,
            "cost_usd": str(usd_cost(deps.llm.model, result.input_tokens, result.output_tokens)),
            "prompt_sha": deps.prompt.sha256_hex,
            "truncated": trunc.to_dict(),
        },
    )
```

### Persistence — `PostgresDiagnosisRepository`

**Migration `0004_diagnoses_idempotency.py`**:

```python
def upgrade() -> None:
    op.add_column(
        "diagnoses",
        sa.Column("hallucinated_evidence", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.alter_column("diagnoses", "hallucinated_evidence", server_default=None)
    op.create_unique_constraint(
        "uq_diagnoses_incident_prompt_model",
        "diagnoses",
        ["incident_id", "prompt_version", "model"],
    )

def downgrade() -> None:
    op.drop_constraint("uq_diagnoses_incident_prompt_model", "diagnoses", type_="unique")
    op.drop_column("diagnoses", "hallucinated_evidence")
```

**Repository method**, mirroring the `write_enrichment_context` atomic pattern (caller pre-constructs the full `OutboxEvent` including its `id`; repo inserts it verbatim into the outbox table inside the same session, *not* via `OutboxRepository.enqueue` which would generate a new id and break the `payload.event_id ↔ outbox_events.id` invariant):

```python
async def save_with_outbox(
    self,
    *,
    incident_id: UUID,
    record: PersistedDiagnosis,
    upstream_event_id: UUID,       # the incident.enriched event_id, for log lineage
    outbox_event: OutboxEvent,     # caller-allocated id; payload missing diagnosis_id
) -> tuple[UUID, Literal["new", "duplicate"]]:
    async with self._session_factory() as session, session.begin():
        result = await session.execute(
            insert(DiagnosisModel)
            .values(
                incident_id=incident_id,
                hypothesis=record.hypothesis,
                confidence=record.confidence,
                reasoning=record.reasoning,
                evidence=[ref.model_dump(mode="json") for ref in record.evidence],
                suggested_actions=[a.model_dump(mode="json") for a in record.suggested_actions],
                likely_category=record.likely_category,
                hallucinated_evidence=record.hallucinated_evidence,
                model=record.model,
                prompt_version=record.prompt_version,
                latency_ms=record.latency_ms,
                token_usage=record.token_usage,
            )
            .on_conflict_do_nothing(constraint="uq_diagnoses_incident_prompt_model")
            .returning(DiagnosisModel.id)
        )
        diagnosis_id = result.scalar_one_or_none()
        if diagnosis_id is None:
            existing = await session.execute(
                select(DiagnosisModel.id).where(
                    DiagnosisModel.incident_id == incident_id,
                    DiagnosisModel.prompt_version == record.prompt_version,
                    DiagnosisModel.model == record.model,
                )
            )
            return existing.scalar_one(), "duplicate"

        # Atomic outbox insert in the same session — preserves the caller's
        # outbox_event.id so payload.event_id == outbox_events.id.
        payload = {**outbox_event.payload, "diagnosis_id": str(diagnosis_id)}
        await session.execute(
            insert(OutboxEventModel).values(
                id=outbox_event.id,
                topic=outbox_event.topic,
                key=outbox_event.key,
                payload=payload,
                attempts=0,
                created_at=outbox_event.created_at,
            )
        )
        return diagnosis_id, "new"
```

The outbox row inserts only on the `new` branch — re-deliveries don't produce duplicate `incident.diagnosed` events.

---

## 5. Consumer flow, outbox emission, idempotency

### `consumer.py` — `DiagnosisConsumer`

Same shape as `EnrichmentConsumer`. Deliberate — repeating a known-good pattern is more valuable than novelty here.

```python
class DiagnosisConsumer:
    def __init__(
        self,
        *,
        consumer: Any,                  # aiokafka.AIOKafkaConsumer (injected)
        deps: DiagnosisDeps,
        agent_fn: DiagnoseFn,
        topic: str = "sentinel.incidents",
        downstream_topic: str = "sentinel.incidents",
    ): ...
```

`_ACCEPTED_EVENTS = frozenset({"incident.enriched"})` — the enrichment consumer's filter already excludes this event type, so the two consumers don't process each other's emissions even on the same topic. The diagnosis consumer's filter mirrors that contract from the other side.

### `_handle` flow

```
1. parse envelope JSON               → ValueError: diagnosis_invalid_events_total.inc(); return
2. filter event_type ∈ _ACCEPTED     → reject others silently (debug log)
3. IncidentEvent.model_validate      → ValidationError: invalid_events.inc(); return
4. incident = await incident_repo.get(envelope.incident_id)
   stored   = await incident_repo.get_enrichment_context(envelope.incident_id)
   if incident is None:
       failed_total.labels(reason="missing_incident").inc(); commit offset; return
   if stored is None:
       failed_total.labels(reason="missing_context").inc(); commit offset; return
   context = stored.context
5. record = await agent_fn(incident, context, deps)        # the LLM call
6. outbox_event_id = uuid4()
   outbox_event_template = OutboxEvent(
       id=outbox_event_id,
       topic=downstream_topic,
       key=str(envelope.incident_id),
       payload={
           "event_id":    str(outbox_event_id),
           "event":       "incident.diagnosed",
           "incident_id": str(envelope.incident_id),
           "ts":          now_iso(),
           # diagnosis_id filled in by save_with_outbox after the INSERT
       },
       attempts=0, created_at=now,
   )
7. diagnosis_id, status = await diagnosis_repo.save_with_outbox(
       incident_id=envelope.incident_id,
       record=record,
       upstream_event_id=envelope.event_id,
       outbox_event=outbox_event_template,
   )
8. status == "duplicate": diagnoses_total.labels(status="duplicate").inc(); log info
   status == "new":       diagnoses_total.labels(status="new").inc();       log info
9. commit Kafka offset (run loop, after _handle returns successfully)
```

### Idempotency — two layers

1. **DB layer (durable):** `uq_diagnoses_incident_prompt_model` unique constraint. `ON CONFLICT DO NOTHING` makes re-delivery a cheap no-op even if Kafka commits get lost.
2. **Outbox layer:** the outbox insert happens **only on the `new` branch**, inside the same transaction as the diagnosis insert. Duplicate diagnosis → no outbox row → no duplicate `incident.diagnosed` event. The outbox itself remains idempotent on `OutboxEvent.id`.

No `consumed_events` ledger is introduced — the enrichment consumer doesn't have one either; the equivalent check is the upstream `(incident_id, event_id)` guard inside `write_enrichment_context`, and for diagnosis the equivalent is `(incident_id, prompt_version, model)` inside `save_with_outbox`. The unique constraint is the durable dedup; commit-after-success is the at-least-once guarantee.

### Worker wiring

`sentinel/api/app.py` lifespan picks up `DiagnosisConsumer` exactly like `EnrichmentConsumer`. Gated by `settings.diagnosis_consumer_enabled: bool = True`. Same shutdown path — `consumer.stop()` in the finally block, await the task with a 5s timeout.

### `incident.diagnosed` downstream consumers

None yet in this PR. The event is emitted for:
- Work Area H (memory: optional signal for embedding refresh strategy).
- Work Area I (UI: pushes the diagnosis to listening SSE clients).

Emitting now avoids a backfill later. The `EnrichmentConsumer`'s `_ACCEPTED_EVENTS` set continues to ignore `incident.diagnosed`.

---

## 6. Observability, tests, failure modes

### Metrics

**Already pre-registered in `sentinel/observability/metrics.py`** (no change):
- `sentinel_diagnosis_latency_seconds` (Histogram) — LLM latency per successful call.
- `sentinel_diagnosis_confidence` (Histogram) — observed per persisted diagnosis.
- `sentinel_llm_cost_usd_total{model}` (Counter) — bumped per LLM call.
- `sentinel_diagnosis_correctness_rate_30d` (Gauge) — written by Work Area H, not touched here.

**Removed in this PR**:
- `sentinel_hallucinated_evidence_rate` (Gauge) — no callers; replaced by counter (see below).

**New in this PR**:
- `sentinel_hallucinated_evidence_total` (Counter) — bumped when a diagnosis is flagged.
- `sentinel_diagnoses_total{status}` (Counter) — `new | duplicate`.
- `sentinel_diagnosis_failures_total{reason}` (Counter) — `schema | timeout | transport | no_tool_call | missing_context | missing_incident | exception`. `exception` is the outer catch-all for errors not caught by a named branch (repository / outbox failures, etc.); recurring `exception`-labelled failures should be decomposed into named branches.
- `sentinel_diagnosis_input_truncated_total{section}` (Counter) — bumped per section per call when truncation drops anything.
- `sentinel_diagnosis_llm_tokens_total{kind}` (Counter) — `input | output`.
- `sentinel_diagnosis_invalid_events_total` (Counter) — envelope/JSON parse failures.

The rate ("fraction of diagnoses with invented citations") is then `rate(sentinel_hallucinated_evidence_total[5m]) / rate(sentinel_diagnoses_total{status="new"}[5m])` in Grafana. Documented spec deviation.

### Tracing

Local OTel span per `_handle` invocation. Span name `diagnosis.handle`, child span `diagnosis.llm_call` with attributes `model`, `prompt_version`, `prompt_sha`, `input_tokens`, `output_tokens`, `cost_usd`, `truncated_sections`. No cross-process trace propagation in this PR — see "Deliberate non-goals" above.

### Structured logs

structlog, JSON:
- `diagnosis_received` (incident_id, event_id, upstream offset/partition)
- `diagnosis_skipped` (reason, event_type) — debug, for non-`incident.enriched` events
- `diagnosis_completed` (incident_id, diagnosis_id, confidence, hallucinated_evidence, latency_ms, cost_usd, truncated)
- `diagnosis_duplicate` (incident_id, existing_diagnosis_id, prompt_version, model)
- `diagnosis_failed` (reason, error excerpt)
- `llm_audit` — separate logger writing to `settings.llm_audit_log_path` (existing setting); one line per LLM call carrying `{ts, incident_id, model, prompt_version, prompt_sha, input_tokens, output_tokens, cost_usd, retry_attempt}`. Spec invariant: every LLM call is audit-logged.

### Test plan

**Unit (`tests/unit/diagnosis/`):**
- `test_validation.py` — verified citations; invented IDs; wrong-kind citations (kind mismatch flags); 100%-invented (drops to empty + caps confidence); empty context sections; multiple kinds.
- `test_truncation.py` — under-budget passes through; over-budget drops in priority order; preserves ≥1 per non-empty section; `TruncationStats` accurate; deploys section always keeps the newest entry.
- `test_prompt.py` — `serialize(incident, ctx)` produces stable IDs matching the validation gate; degraded sections render with `FetcherStatus` header; snapshot test against a fixed fixture; SHA-256 baseline file matches on startup, mismatch logs WARN.
- `test_agent.py` — happy path with a `FakeLLMClient`; schema-invalid first attempt → retry with correction → success; second schema-invalid → `DiagnosisInvalid`; hallucinated → confidence cap + flag; timeout → bubbles `LLMTimeout`; tokens/cost accounting populated.
- `test_repository.py` — `save_with_outbox` happy path inserts diagnosis row and outbox row in one txn; duplicate path (uniq violation) returns `("duplicate", existing_id)` and skips outbox insert; outbox payload contains `diagnosis_id`.
- `test_consumer.py` — `_handle` accepts `incident.enriched` only, skips others; missing incident / missing context counted with right reason and offset committed; happy path increments `new`; duplicate path increments `duplicate` and skips outbox; agent exception propagates → exception logged, metric bumped, offset NOT committed.
- `test_llm_client.py` — timeout raises `LLMTimeout` within `timeout_s`; HTTP 5xx retried once with 0.5s backoff; no-tool-call raises `LLMNoToolCall`; usage parsed from `MessageStopEvent`.
- `test_metrics_migration.py` — `hallucinated_evidence_total` counter exists; `sentinel_hallucinated_evidence_rate` gauge removed.

**Integration (`tests/integration/diagnosis/`, `@pytest.mark.integration`):**
- Real Postgres: `0004` upgrade/downgrade round-trip; unique constraint actually fires on duplicate insert.
- Real Kafka + real Postgres + recorded Anthropic fixture (no live LLM in CI): publish `incident.enriched` → consumer writes diagnosis row → outbox emits `incident.diagnosed` → drainer publishes downstream. Verify with consumer-side assertions on the downstream topic.
- Extend the existing `make compose-up` end-to-end smoke: curl one-liner posts a sample webhook → integration test polls until a diagnosis row appears → asserts schema and that `hallucinated_evidence=false` for a known-good context fixture.

**No live LLM in CI** — `FakeLLMClient` in unit, recorded JSON fixture in integration. Real LLM calls happen in the eval harness (Work Area K) on schedule.

### Failure modes — explicit (the "articulate or it isn't done" mantra)

| Failure | What's logged | What's surfaced | What happens |
|---|---|---|---|
| Schema-invalid (twice) | `diagnosis_failed{reason=schema}` | counter, span error | Offset committed, no diagnosis row, no downstream event. Manual re-run via Work Area I once it lands. |
| LLM timeout (30s) | `diagnosis_failed{reason=timeout}` | counter, span error | Offset **not** committed → Kafka redelivers. Bounded by Kafka's max attempts; DLQ pattern from Phase 3 handles poison. |
| LLM transport (5xx/429 after one backoff) | `diagnosis_failed{reason=transport}` | counter | Same as timeout — Kafka redelivery. |
| No-tool-call (model returns plain text) | `diagnosis_failed{reason=no_tool_call}` | counter | Treated as schema-invalid → one retry with correction → fail. |
| Missing incident row | `diagnosis_failed{reason=missing_incident}` | counter | Commit offset; warn. Likely deleted between enrichment and diagnosis — won't be fixed by retry. |
| Missing context (incident exists, `context_json IS NULL`) | `diagnosis_failed{reason=missing_context}` | counter | Commit offset; warn. Indicates enrichment never wrote context — operator issue. |
| Hallucinated evidence | `diagnosis_completed{hallucinated=true}` | `hallucinated_evidence_total` | Persisted with `confidence ≤ 0.4` and verified citations only. Healthy operation; this is the quality signal. |
| Duplicate (re-delivery, same prompt + model) | `diagnosis_duplicate` | `diagnoses_total{status=duplicate}` | No-op insert; no outbox event; offset committed. |
| Token budget overflow | (impossible — truncation runs first) | `input_truncated_total{section}` | Truncated, logged, processed normally. |
| Unexpected error (repository, outbox-insert, anything not caught above) | `diagnoser_event_failed` w/ traceback | `diagnosis_failures_total{reason=exception}` | Offset **not** committed → Kafka redelivers. A recurring `exception`-labelled failure means a new named branch should be added. |

### Acceptance criteria

- `make lint typecheck test` green.
- `make test-integration` green; new diagnosis integration test asserts end-to-end flow.
- `make migrate` applies `0004` cleanly on a fresh DB; `make migrate-down` reverses it.
- `docker compose up` boots; curl sample webhook → diagnosis row appears within 30s; OpenAPI schema (Work Area I tracks the diagnosis endpoint) remains unchanged.
- Test coverage on `sentinel/diagnosis/` ≥ 90% lines.
- Subagent code review (per repo policy) approves before commit/push.
