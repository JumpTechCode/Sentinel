# Load + Chaos test suites — design

**Date:** 2026-06-15
**Status:** Approved (design); plan + implementation to follow
**Scope:** Close the two `sentinel-claude-code-prompt.md` §"Testing" gaps — **load** and **chaos** — with **zero Anthropic spend**. Corpus 10→30 is explicitly deferred (it is the credit-burner: each new postmortem requires live cassette recording + re-baseline).

## Why

The backend is feature-complete through Week 5 (ingestion, enrichment, diagnosis, memory/feedback, eval harness). Spec §"Testing" lists five test tiers; **unit** and **integration** exist, **eval** exists, but **load** (locust) and **chaos** (toxiproxy) do not. These two are the remaining credit-safe polish: load exercises the synchronous ingestion path, chaos verifies the circuit breakers and graceful degradation that are the project's headline resilience invariants.

## Non-negotiable constraint: no Anthropic credits

Only the diagnosis **LLM call** costs credits. Enrichment fetchers are in-process, repo-backed stubs (no external HTTP). So:

- **Load test** excludes the diagnosis path entirely by running the app with `diagnosis_consumer_enabled=False` + `memory_consumer_enabled=False` (both already in `Settings` — no new env var).
- **Chaos test** runs the diagnosis consumer with `_FakeAnthropicClient` (the established fake from `tests/integration/diagnosis/test_end_to_end.py`), so breakers are exercised end-to-end with zero API calls.

## Part A — Load test (locust)

**Target:** `POST /webhooks/{source}` ingestion path only.

**Mechanism:** boot the app (testcontainers Postgres + Redis + Kafka, or compose) with `diagnosis_consumer_enabled=False` and `memory_consumer_enabled=False`. The enrichment consumer may run in the background (no LLM) — realistic load.

**Scenario (`tests/load/locustfile.py`):** fire valid HMAC-signed `generic` webhooks with **unique payloads** (unique service + title → unique fingerprint → exactly one incident each). This makes the zero-drop assertion a clean 1:1 count and avoids fingerprint-dedup / idempotency collapse muddying the numbers.

**Assertions (post-run harness):**
1. `0` non-2xx responses (no 5xx, nothing dropped at the edge).
2. **Zero dropped events:** `count(incidents) == unique fingerprints sent` AND `count(outbox_events WHERE event='incident.opened') == count(incidents)`. Every accepted webhook → exactly one incident row + one outbox row.
3. p95 ingestion (202) latency under a generous bound.

**Spec deviation (documented):** the spec asserts *"p95 **diagnosis** latency < 10s."* We exclude diagnosis to stay creditless, so we assert **p95 ingestion latency** instead. README copy will say "ingestion throughput / latency" and will never report a diagnosis-latency number we did not measure.

**Form factor:**
- `tests/load/locustfile.py` — the locust scenario (HMAC signing helper reused from existing ingestion test fixtures).
- `tests/load/test_load_smoke.py` — `@pytest.mark.load`; boots the app against testcontainers, runs locust **headless at low scale** (~20 RPS × ~15s) and asserts invariants 1–3. CI-affordable but **not** in the PR smoke job.
- `tests/load/conftest.py` — app + container fixtures (follow `tests/integration/*/conftest.py`).
- `make load` — full **100 RPS × 5 min** on-demand run (matches spec scale); prints the real numbers.
- `make load-smoke` — the pytest smoke.

**p95 bound decision:** smoke assertion uses a generous bound (e.g. `< 1s` p95) to avoid CI flakiness; the *real* measured number is reported separately (README / run output), not gated.

## Part B — Chaos test (two layers)

### B1 — Infra chaos via toxiproxy (ingestion path)

**As built:** a **toxiproxy testcontainer** (on a shared network) fronts **Redis**;
Postgres and Kafka run direct so the app boots and background tasks run. The app
is driven via ASGITransport with consumers disabled. `toxiproxy-python` (dev dep)
drives the proxy via the REST API (`:8474`). Suite:
`tests/chaos/test_infra_chaos.py`, `@pytest.mark.chaos`. (The original plan
proxied PG/Redis/Kafka via a `docker-compose.chaos.yml`; the testcontainers
approach is self-contained and CI-friendlier — no separate compose file shipped.)

| Fault (toxic) | Expected behavior (assertion) | Built |
|---|---|---|
| Redis proxy disabled | webhook → `503 INFRA_UNAVAILABLE` (fails **closed** — `sentinel/ingestion/webhook.py:106`); **no** incident created | ✅ |
| Redis re-enabled | next webhook → `202` (clean recovery) | ✅ |
| Redis +latency toxic | webhook still `202`; incident persisted (no drop under latency) | ✅ |
| Postgres / Kafka via toxiproxy | **dropped** — Kafka advertises its own listener, so clients reconnect around the proxy; Kafka-outbox-recovery (ADR 0001) is already covered by `tests/integration/ingestion/test_drainer_resilience.py` | — |
| `/readyz` dependency flip | **dropped** — `/readyz` only checks consumer aliveness today; PG/Redis/Kafka probes are deferred to the API layer (Work Area I) | — |

See ADR 0010 for the toxiproxy-vs-fault-injection rationale and this scope.

### B2 — Enrichment breaker chaos via in-process fault injection (diagnosis path, fake LLM)

**Why not toxiproxy here:** enrichment fetchers are in-process repo-backed stubs — there is no external HTTP socket to intercept. Injecting a fault-fetcher is the faithful equivalent and directly drives the `CircuitBreaker` the spec wants verified. **This deviation is recorded in ADR 0010.**

Suite: `tests/chaos/test_breaker_chaos.py`. Inject a `FaultFetcher` (its `fetch()` raises, or sleeps past `timeout_s`) into `EnrichmentDeps.fetchers`, paired with a real breaker built via `sentinel/enrichment/metrics_wiring.py::make_breaker` (so the `sentinel_circuit_breaker_state` gauge is wired). Drive `assemble()` directly and/or the full enrichment→diagnosis consumer with `_FakeAnthropicClient`.

**Assertions:**
1. After `threshold` (5) failures within `window_s`, breaker → `open`; `breaker.state == "open"`; `sentinel_circuit_breaker_state{integration=<name>} == 1`.
2. Subsequent calls short-circuit (`CircuitOpenError` path) → `enrichment_failures_total{reason="circuit_open"}` increments.
3. `assemble()` degrades gracefully: faulted section `status="failed"`, the other sections `ok` — `assemble()` never raises (orchestrator contract).
4. Diagnosis still completes over the partial context (consumer writes a diagnosis row + `incident.diagnosed` outbox event), proving graceful degradation end-to-end.
5. Half-open recovery: after `cooldown_s`, a successful trial → `closed`, gauge back to `0`. Use an injected `time_fn` to avoid a real 30s wait.

## CI placement

- `tests/load/` and `tests/chaos/` are marked `load` / `chaos` and are **excluded from the PR smoke job** (extra services + runtime; would slow/flake PR CI). PR CI stays fast and green.
- Provide `make load` / `make load-smoke` / `make chaos`, README how-to, and an **optional manual-dispatch / nightly** CI job (workflow_dispatch). The breaker logic already has unit coverage; B2 is the integration-level proof.
- Register `load` and `chaos` markers in `pyproject.toml` `[tool.pytest.ini_options]` so they don't trip "unknown marker."

## Deliverables / file plan

- ✅ `tests/load/locustfile.py`, `tests/load/test_load_smoke.py`, `tests/load/conftest.py`, `tests/load/__init__.py`
- ✅ `tests/chaos/conftest.py`, `tests/chaos/test_infra_chaos.py`, `tests/chaos/test_breaker_chaos.py`, `tests/chaos/__init__.py`
- ✅ `docker-compose.load.yml` (consumers-off + pinned secret for `make load`). **No `docker-compose.chaos.yml`** — chaos is testcontainers-driven (self-contained).
- ✅ `pyproject.toml`: `toxiproxy-python` dev dep; `toxiproxy.*` mypy override; `load` + `chaos` markers
- ✅ `Makefile`: `load`, `load-smoke`, `chaos` targets (placeholders replaced)
- ✅ `docs/adr/0010-chaos-toxiproxy-vs-fault-injection.md`
- ✅ `README.md`: status table row + load/chaos targets + creditless note
- ⏳ Optional `.github/workflows` manual/nightly job — **not shipped** (deferred; suites run via `make`)
- ✅ No app `Settings` / `.env.example` changes (reused the consumer-enabled flags)

## YAGNI cuts

- Corpus 10→30 — deferred; it is the Anthropic credit-burner.
- 100 RPS × 5 min in PR CI — on-demand `make load` only.
- toxiproxy for enrichment fetchers — they are in-process stubs; fault injection instead (ADR 0010).
- A bespoke load dashboard — locust's own stats output + the post-run invariant harness suffice.

## Risks / watch-items

- **Load test app boot under testcontainers** must disable diagnosis+memory consumers or it will attempt Anthropic calls. The smoke fixture must assert these flags are off (loud guard) before driving traffic.
- **toxiproxy app rewiring:** the app reads `redis_url` / `kafka_brokers` / Postgres DSN from `Settings`; the chaos compose must point these at the proxy endpoints, not the real services.
- **Kafka-recovery timing** in B1: the outbox drainer interval governs how long until the unpublished event is republished; the test polls with a bounded timeout (mirror the diagnosis-e2e poll loop) rather than sleeping a fixed duration.
- **Determinism in B2:** use injected `time_fn` for the breaker so cooldown/half-open transitions are exercised without wall-clock waits.
