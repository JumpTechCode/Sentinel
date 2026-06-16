# API surface (Work Area I) — design

**Date:** 2026-06-15
**Status:** Approved (design); plan + implementation to follow
**Scope:** Close the `sentinel-claude-code-prompt.md` §"API contracts" gap. The backend is feature-complete through Week 5; of the 15 spec'd routes, only 4 exist. Build the remaining 11 — incident CRUD/list/detail, re-diagnose, eval HTTP surface, `/metrics`, SSE stream, WS broadcast — plus extend `/readyz`. **Zero Anthropic spend** (cassette-replay only). Unblocks Work Area L (the Next.js UI), which is a pure consumer of these endpoints.

## Why

Spec §"API contracts" (lines 321–337) lists 15 routes. Implemented today: `POST /webhooks/{source}`, `POST /incidents/{id}/resolve`, `GET /healthz`, `GET /readyz` (aliveness-only) — 4. Missing: the entire read surface (`GET /incidents`, `GET /incidents/{id}`, `POST /incidents`), `POST /incidents/{id}/diagnose`, the `/evals/*` surface, `GET /metrics` (metrics are collected but never exposed), and the realtime endpoints (SSE `/stream`, WS `/ws/incidents`). The UI cannot be built against endpoints that don't exist, so this is the gating work area for Week 6.

## Non-negotiable constraint: no Anthropic credits

Only the diagnosis **LLM call** costs credits. Every new route either does no LLM work, or routes through the existing **cassette transport in replay mode** (`sentinel/evals/cassette.py`, wired in `app.py` lifespan under `eval_mode`). Consequence:

- `POST /incidents/{id}/diagnose` replays from a cassette when one exists; in cassette mode with no match it returns **409 `no_recording_available`** rather than making a live call.
- `POST /evals/runs` runs the corpus through the runner in **replay** mode (cassettes already committed from the eval-harness work).
- Live LLM is never on the demo path. `docker compose up` seeds incidents whose diagnoses are pre-recorded.

## Resolved design decisions (from brainstorming)

Three forks were decided with the user; each becomes an ADR.

1. **SSE `/stream` emits lifecycle events, not LLM tokens** → **ADR 0012**. Diagnosis runs out-of-band (Kafka consumer), so token-level streaming would force either a request-path LLM call (breaks the single-shot invariant) or token-level pub/sub (disproportionate). SSE emits coarse `state` events (`enriching` → `diagnosing` → `diagnosed`) plus a terminal `diagnosis` event carrying the final validated payload, then `done`. This is a **documented deviation from spec line 210** ("emit partial diagnosis over SSE as the model streams").
2. **Realtime fan-out = Redis pub/sub fed by a Kafka→Redis bridge** → **ADR 0011**. A bridge task consumes the `sentinel.incidents` topic (already fed by the outbox drainer) and republishes lifecycle events to Redis channels. SSE/WS handlers subscribe. Rationale: Redis is already in `app.state`; keeps Redis out of the per-domain consumers; survives multiple API workers (in-memory broadcast would not); gets a `/readyz` aliveness check like the other consumers. Alternatives rejected: in-process Kafka consumer + asyncio broadcast (single-process only), DB polling (laggy/chatty).
3. **Compute-heavy POSTs are wired + replay-backed + async** → **ADR 0013**. `/diagnose` and `/evals/runs` are fully implemented but execute through cassette replay → zero live spend. `POST /evals/runs` starts an `asyncio` background run and returns **202 + `run_id`**; `GET /evals/runs/{id}` reports `running` → `ok`/`failed`. Rejected: read-mostly/POST-deferred (leaves spec routes unbuilt), live-by-default (violates the credit constraint).

## Route inventory

| Route | PR | Notes |
|---|---|---|
| `GET /metrics` | 1 | `prometheus_client.generate_latest()` on the default registry; correct content-type; never blocks handlers |
| `GET /incidents` | 1 | list + filters (`status`, `service`, `severity`) + offset pagination; `IncidentListResponse{items,total,limit,offset}` |
| `GET /incidents/{id}` | 1 | `IncidentDetailResponse` with enrichment `context`; 404 on unknown. **`diagnoses` population moved to PR 2** — see note below |
| `POST /incidents` | 1 | manual create **through the fingerprint + idempotency path** (not a raw insert) |
| `GET /readyz` (extend) | 1 | add Postgres + Redis + Kafka probes (spec line 337; deferred here from the load-chaos work) |
| `POST /incidents/{id}/diagnose` | 2 | replay-backed; returns persisted `Diagnosis`; 404 unknown, 409 `no_recording_available`, 409 if already resolved |
| `GET /evals/runs` | 2 | `EvalRunRepository.list_recent(limit)` |
| `POST /evals/runs` | 2 | async background run (replay), 202 + `run_id` |
| `GET /evals/runs/{id}` | 2 | full results; 404 unknown |
| `GET /evals/baseline` | 2 | `get_latest_ok_run(trigger="baseline")`; 404 if none |
| `GET /incidents/{id}/stream` | 3 | SSE lifecycle events |
| `WS /ws/incidents` | 3 | broadcast lifecycle events |

## Module structure

New route modules under `sentinel/api/routes/`, mounted in `app.py:build_app()`, following the existing `request.app.state.<repo>` DI pattern (no FastAPI `Depends` graph — match `resolve.py`/`webhooks.py`):

- `incidents.py` — list / detail / create / diagnose
- `evals.py` — runs list/create/get, baseline
- `metrics.py` — Prometheus exposition
- `realtime.py` — SSE `/stream` + WS `/ws/incidents`
- `realtime_bridge.py` (or `sentinel/api/realtime/`) — Kafka→Redis bridge task + Redis subscriber helper

Lifespan additions to `app.state`: stash `incident_repo` and `eval_run_repo` (created today but not exposed), ensure `diagnosis_repo` is always present, and start/stop the Kafka→Redis bridge task + register its aliveness in the `consumer_alive` map used by `/readyz`.

## Schema & repository changes

Most Pydantic models already exist in `sentinel/schemas/api.py` (`IncidentListItem`, `IncidentDetailResponse`, `CreateIncidentRequest`). Additive work:

- **`IncidentRepository.list_incidents(...)`** — extend beyond `list_recent(limit)` to accept optional `status` / `service` / `severity` filters + `limit`/`offset`. Returns items + a total count for the paginated envelope. Offset pagination (cursor is YAGNI for the demo).
- **Populate `IncidentDetailResponse.diagnoses`** — **deferred to PR 2.** Planning surfaced that a fully-hallucinated `PersistedDiagnosis` has an *empty* evidence list (by design — `diagnosis/persisted.py` drops the constraint), but the wire `Diagnosis` requires `evidence: min_length=1`. So persisted diagnoses cannot be rendered through the strict `Diagnosis` model. PR 2 (which owns `/diagnose`) introduces a relaxed `DiagnosisView` that allows empty evidence and carries the `hallucinated_evidence`/`confidence`/`model` metadata the UI needs, and retypes `IncidentDetailResponse.diagnoses` to it. PR 1's detail endpoint returns `diagnoses: []`. Uses the existing `DiagnosisRepository.get_by_incident_id` (most-recent single record), not a new `list_by_incident`.
- **`POST /incidents`** computes the fingerprint and goes through `ingest()` so manual incidents dedup identically to webhook incidents (same 1h fingerprint window + payload-hash idempotency). It does **not** bypass via raw `create_from_alert`.
- **New Pydantic models** (no `dict[str, Any]` on any boundary — invariant 6): `IncidentListResponse`, `EvalRunListItem`, `EvalRunResponse`, `StartEvalRunRequest`/`StartEvalRunResponse`, `BaselineResponse`, and SSE/WS event envelopes (`IncidentStateEvent`, `IncidentDiagnosedEvent`).

## Error handling & invariants

Per the mantra — every route names its failure mode:

- **404** unknown incident / run / baseline. **409** `no_recording_available` (cassette mode, no match) and diagnose-on-resolved. **422** Pydantic validation. **503** from `/readyz` when a dependency probe fails.
- **SSE/WS**: bounded per-subscriber queues (drop + terminal `error` event on slow consumer, never unbounded memory growth); periodic heartbeat/ping; on Redis disconnect, emit terminal `error` and close with an explicit WS close code.
- **`/metrics`** is a read-only registry scrape; it must not block or contend with request handlers.
- **Preserved invariants**: Pydantic on every boundary (6); suggest-only — no route executes a remediation (7); the evidence-citation gate (3) lives inside `diagnose()` and is untouched; replay path keeps schema-validation + retry-once semantics (2).

## Testing

- **Unit**: each handler with fake repos — filter/pagination math, 404/409 branches, SSE event serialization, `POST /incidents` fingerprint path, `/metrics` exposition format.
- **Integration** (testcontainers PG + Redis + Kafka): list/detail/create round-trip; `/diagnose` replay against a committed cassette (+ the 409-no-cassette branch); SSE and WS receive events end-to-end after a webhook fires, exercising the Kafka→Redis bridge; `/evals/runs` async lifecycle (`202` → poll `running` → `ok`); `/metrics` scrape asserts known metric names (e.g. `sentinel_hallucinated_evidence_total`); `/readyz` flips to 503 when a dependency is down (composes with the existing toxiproxy chaos harness).
- **OpenAPI**: `make openapi` exports a clean, publishable spec; add a test asserting no `dict[str, Any]` leaks into the generated schema.

## CI placement

- Unit + integration for these routes run in the normal PR jobs.
- Realtime integration tests (SSE/WS/bridge) need Redis + Kafka — keep them in the existing integration tier, not the fast smoke job.
- No change to the evals-gate or nightly jobs.

## PR slicing (3 reviewable PRs)

1. **Read surface + foundation** — stash `incident_repo` in `app.state` (`eval_run_repo` → PR 2); `GET /incidents` (filters + pagination), `GET /incidents/{id}` (context only), `POST /incidents`, `GET /metrics`, extend `/readyz` with Postgres + Redis probes (Kafka readiness via consumer aliveness; explicit Kafka probe deferred), real `make openapi` export. No new infra. No ADR. Detailed plan: `plans/2026-06-15-api-surface-pr1-plan.md`.
2. **Diagnose + evals** — `POST /incidents/{id}/diagnose` (replay), `GET/POST /evals/runs`, `GET /evals/runs/{id}`, `GET /evals/baseline`. **ADR 0013** (replay-backed async POST execution).
3. **Realtime** — Kafka→Redis bridge, SSE `/incidents/{id}/stream`, WS `/ws/incidents`. **ADR 0011** (Redis pub/sub fan-out), **ADR 0012** (SSE lifecycle vs token streaming).

## Deliverables / file plan

- `sentinel/api/routes/incidents.py`, `evals.py`, `metrics.py`, `realtime.py`
- `sentinel/api/realtime/` (or `realtime_bridge.py`) — Kafka→Redis bridge + subscriber helper
- `sentinel/api/app.py` — mount routers; stash repos; start/stop bridge; register bridge aliveness
- `sentinel/api/routes/health.py` — extend `/readyz` with PG/Redis/Kafka probes
- `sentinel/persistence/repositories.py` — `list_incidents(...)` filter/pagination; `DiagnosisRepository.list_by_incident` if absent
- `sentinel/schemas/api.py` — new request/response + event envelope models
- `tests/unit/api/…`, `tests/integration/api/…` — per the testing section
- `docs/adr/0011-realtime-redis-pubsub.md`, `0012-sse-lifecycle-not-tokens.md`, `0013-replay-backed-post-execution.md`
- `README.md` — API surface section + status table update
- `Makefile` — replace the `make openapi` placeholder with a real OpenAPI export
- `Settings` / `.env.example` — only if a new knob is genuinely needed (e.g. bridge enable flag); prefer reusing existing flags

## YAGNI cuts

- **Auth** — out of scope for V1 (local / reverse-proxy only, per spec non-goals).
- **Cursor pagination** — offset is sufficient for the demo dataset.
- **Token-level SSE streaming** — lifecycle events instead (ADR 0012).
- **Live LLM on any route in the demo** — replay only.
- **prometheus-fastapi-instrumentator** — not needed; a one-line `generate_latest()` handler exposes the already-populated registry.

## Risks / watch-items

- **Bridge ordering / at-least-once**: the Kafka→Redis bridge inherits at-least-once delivery; SSE/WS consumers must tolerate duplicate lifecycle events (treat them as idempotent state assertions, not deltas).
- **Multi-worker fan-out**: Redis pub/sub is the reason this works across API workers; if the deployment ever runs a single worker, the bridge still works but the benefit is moot — note in the ADR.
- **`/diagnose` replay matching**: the cassette key includes prompt_version + model + case/shot; arbitrary demo incidents won't match. The 409 branch must be explicit and tested, and seeded demo incidents must have recorded cassettes.
- **`POST /evals/runs` background task lifecycle**: the run outlives the request; on app shutdown the task must be cancelled cleanly and the run row finalized as `failed`/`partial` rather than left dangling in `running`.
- **`/readyz` probe timeouts**: each dependency probe needs its own short timeout so a slow dependency can't hang the readiness check past its budget.
