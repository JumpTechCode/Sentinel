# Sentinel

**AI on-call copilot.** Sentinel ingests production alerts (Sentry, PagerDuty, Datadog, generic webhooks), assembles incident context in parallel from your existing tooling, and produces a structured diagnosis — with confidence scoring, evidence citations against the supplied context, and suggested remediations. Single-shot, schema-validated, deterministic. Learns from resolutions via a pgvector incident memory.

Public engineering portfolio. Held to a production bar — every external call has a timeout and a circuit breaker, every LLM response is schema-validated, every evidence citation is verified, and the eval harness blocks regressions in CI.

## Why it's built this way

A few load-bearing choices that distinguish Sentinel from a typical "LLM wrapper":

- **Single-shot diagnosis, not agentic.** Enrichment pre-assembles all context in parallel; the LLM only reasons over what it was given. Predictable latency, deterministic evals, bounded cost. No tool-calling loops in V1.
- **Webhook → Kafka → out-of-band diagnosis.** `POST /webhooks/{source}` returns `202` immediately after persisting the incident and emitting `incident.opened` via a transactional outbox. Diagnosis runs off the event, never in the request path.
- **Fingerprint dedup, not external-ID dedup.** `sha256(service ‖ normalized_title ‖ severity)`. Same fingerprint within 1h updates the existing incident and appends to the event log; it does not create a new row. Raw-payload idempotency (`SETNX webhook:{source}:{sha256(body)}`, 24h TTL) defends against retried-with-changes.
- **Evidence-citation gate.** After the LLM returns, every `EvidenceRef.id` must resolve to an item that was actually in the assembled context. Hallucinated citation → `hallucinated_evidence: true`, confidence capped at 0.4, metric incremented. This is the project's headline quality signal.
- **Embedding on resolve, not on open.** Similar-incident retrieval improves once embeddings reflect actual root cause, not surface symptom. The "gets smarter from use" loop.
- **Monolith with strict module boundaries.** Inter-module calls go through repository or service interfaces; non-`persistence/` modules never touch the DB directly. Splittable into services later without rewriting.

ADRs for the non-obvious calls live in [`docs/adr/`](docs/adr/).

## Stack

Python 3.12 · FastAPI · asyncio · Pydantic v2 · Postgres 16 + `pgvector` · Redis 7 · Kafka (KRaft) · Anthropic (`claude-sonnet-4-5` default) · OpenTelemetry + Prometheus + structlog · Docker Compose. Next.js 14 + Tailwind for the demo UI (later phase).

## Architecture

```
        ┌──────────────────────────────────────────────────────────┐
        │  POST /webhooks/{source}  (HMAC verified, payload-hash   │
        │  idempotent, normalized → NormalizedAlert)               │
        └───────────────┬──────────────────────────────────────────┘
                        │  202 Accepted (returns fast)
                        ▼
        ┌──────────────────────────────────────────────────────────┐
        │  Postgres  ←→  outbox_events                             │
        │     incidents (fingerprint dedup, event_log appended)    │
        └───────────────┬──────────────────────────────────────────┘
                        │  outbox drainer
                        ▼
        ┌──────────────────────────────────────────────────────────┐
        │  Kafka topic: sentinel.incidents                         │
        │   incident.opened / incident.recurred / incident.enriched│
        └───────────────┬──────────────────────────────────────────┘
                        ▼
        ┌──────────────────────────────────────────────────────────┐
        │  Enrichment worker — parallel fetchers behind            │
        │  per-integration circuit breakers                        │
        │  (deploys · related alerts · active alerts · recent logs │
        │  · runbooks · similar incidents from pgvector)           │
        │  → IncidentContext written back, incident.enriched event │
        └───────────────┬──────────────────────────────────────────┘
                        ▼
        ┌──────────────────────────────────────────────────────────┐
        │  Diagnosis worker — versioned prompt + Anthropic call,   │
        │  schema-validated response, evidence-citation gate,      │
        │  idempotent persistence keyed on (incident, prompt_ver)  │
        └──────────────────────────────────────────────────────────┘
```

## Module map

```
sentinel/
├── ingestion/      webhook receivers, HMAC verify, fingerprinting, idempotency, outbox drainer
├── integrations/   sentry · pagerduty · datadog · generic — adapters normalize to NormalizedAlert
├── enrichment/     parallel fetchers + circuit breaker, orchestrator, Kafka consumer
├── diagnosis/      LLM client, versioned prompt bundle, validation gate, persistence, consumer
├── workers/        long-running worker entrypoints (diagnosis_worker)
├── memory/         pgvector incident store + embedding pipeline                       (stub — H)
├── persistence/    SQLAlchemy models, Alembic migrations, repositories
├── observability/  Prometheus metrics, OTel tracing, structlog, LLM audit log + cost meter
├── schemas/        Pydantic v2 contracts shared across modules (NormalizedAlert, Diagnosis, …)
├── api/            FastAPI app (routes/health, routes/webhooks)
├── evals/          eval harness against public postmortems                            (stub — K)
└── config/         pydantic-settings, per-env defaults, secret loading
```

## Project status

Built phase by phase. Each phase's CI gate must be green before the next starts.

| Phase | Work area | Status |
|------:|-----------|--------|
| 1 | Foundation & tooling (A) — Dockerfile, compose, Makefile, ruff/mypy/pytest, pre-commit, CI | ✅ Landed |
| 2 | Persistence + core schemas + observability skeleton (B, C, J) — Alembic, models, metrics, OTel, structlog | ✅ Landed |
| 3 | Webhook ingestion + integration adapters (D, E) — `/webhooks/{source}`, HMAC, fingerprinting, dedup, transactional outbox | ✅ Landed |
| 4 | Enrichment pipeline (F) — parallel fetchers, circuit breakers, `incident.enriched` event | ✅ Landed |
| 5 | Diagnosis agent (G) — versioned prompt, Anthropic call, schema + evidence gate, idempotent persistence | ✅ Landed |
| 6 | Memory / feedback loop (H) — embedding on resolve, similar-incident retrieval feedback | ⏳ Next |
| 7 | API surface (I) — REST + SSE/WS, `/readyz` dependency checks, OpenAPI publish | ⏳ Planned |
| 8 | Eval harness (K) — postmortem corpus, scoring, CI smoke + nightly full | ⏳ Planned |
| 9 | UI (L) — Next.js incident view | ⏳ Planned |
| 10 | Load + chaos (M) — Locust, Toxiproxy | ⏳ Planned |

Designs and per-phase plans live in [`plans/`](plans/).

## Quality bar — what the code is required to do

The non-negotiable invariants. Any change that weakens one needs an ADR.

1. Every external call has a timeout and a circuit breaker (`5 failures / 60s → open, 30s → half-open, 1 trial → closed`). Failures are logged and counted, never raised — fetchers return `FetcherResult{status: ok | degraded | failed}`.
2. Every LLM response is Pydantic-validated. Invalid → retry once → fail loud.
3. Every evidence citation is verified against the supplied context. Hallucinated → `hallucinated_evidence: true`, confidence capped at 0.4, `sentinel_hallucinated_evidence_rate` incremented.
4. Context items use stable, checkable IDs in the prompt (`[deploy:abc123]`, `[similar:incident-uuid]`); the validation gate depends on this format.
5. Confidence rubric is fixed (0.0–0.3 speculation · 0.4–0.6 plausible · 0.7–0.85 strong · 0.86–1.0 direct causal). Change → bump `prompt_version` and re-baseline evals.
6. Pydantic on every API boundary. No `dict[str, Any]` in request/response models. OpenAPI must be publishable.
7. **Suggest only, never execute.** `SuggestedAction.requires_human_approval` defaults to true. No code path executes remediations.
8. Every webhook is idempotent on payload hash. Every migration is reversible (`upgrade` and `downgrade`).
9. Every PR includes tests. CI green or it doesn't merge.

If a code path can't articulate its failure mode, it isn't done.

## Local development

### Requirements
- Python 3.12
- Docker + Docker Compose v2 (Postgres 16 + pgvector, Redis 7, Kafka 3.7)
- GNU Make

### Bootstrap

```bash
make bootstrap                 # create .venv, install runtime + dev deps, install pre-commit
cp .env.example .env           # fill in SENTINEL_ANTHROPIC_API_KEY and per-source webhook secrets
make compose-up                # boot Postgres, Redis, Kafka, app, worker (waits on healthchecks)
curl localhost:8000/healthz    # → {"status":"ok"}
make compose-down              # tear down (-v removes volumes)
```

### Common targets

```bash
make fmt                       # ruff format + autofix
make lint                      # ruff format --check + ruff check (CI parity)
make typecheck                 # mypy --strict
make test                      # unit tests
make test-integration          # integration tests (needs `make compose-up`)
make migrate                   # alembic upgrade head
make migrate-down              # alembic downgrade -1
make evals-smoke               # 5-case smoke set        (placeholder until Work Area K)
make evals                     # full corpus             (placeholder until Work Area K)
make openapi                   # export OpenAPI JSON     (placeholder until Work Area I)
```

Single test:

```bash
pytest tests/unit/diagnosis/test_validation.py::test_hallucinated_evidence_caps_confidence
```

### CI

Every PR runs: `lint → typecheck → unit → integration → evals-smoke`. Nightly runs the full eval corpus. README numbers (once eval phase lands) come from `evals/results/<latest>.md` — never hand-edited.

## Configuration

All settings are loaded by `sentinel.config.settings.Settings` via `pydantic-settings`. Source is environment variables prefixed `SENTINEL_`. See [`.env.example`](.env.example) for the full list — runtime mode, Postgres DSN, Redis URL, Kafka brokers + topic, Anthropic key + model, per-source webhook HMAC secrets, diagnosis token caps and timeout, OTel endpoint, LLM audit log path.

## ADRs

- [`0001-transactional-outbox.md`](docs/adr/0001-transactional-outbox.md) — Atomic Postgres + Kafka via an `outbox_events` table and a drainer.
- [`0002-enrichment-same-topic-roundtrip.md`](docs/adr/0002-enrichment-same-topic-roundtrip.md) — `incident.enriched` reuses the same `sentinel.incidents` topic.
- [`0003-single-llm-provider-no-breaker.md`](docs/adr/0003-single-llm-provider-no-breaker.md) — V1 is Anthropic-only; timeout + retry-once-then-fail substitutes for a per-LLM breaker.

## Non-goals for V1

Multi-tenancy. Hosted auth (local / reverse-proxy only). Automatic remediation execution. Real-time log search at scale. Multiple LLM providers (interface stays single-impl).

## License

[Apache 2.0](LICENSE).

## Support

If you find this project useful, you can support its development:

<a href="https://buymeacoffee.com/JumpTech"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" width="200"></a>
