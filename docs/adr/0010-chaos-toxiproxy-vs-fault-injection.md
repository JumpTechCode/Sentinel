# 0010 — Chaos testing: toxiproxy for infra, fault-injection for breakers

**Status:** Accepted
**Date:** 2026-06-15

## Context

Spec §"Testing" calls for a chaos suite: *"runs against the live stack with
`toxiproxy` injecting latency and failures into Postgres/Redis/external
integrations. Verify circuit breakers behave, graceful degradation works."*

Two facts about the V1 codebase complicate a literal reading:

1. **The circuit breakers do not wrap network sockets.** They wrap the
   enrichment fetchers (`enrichment/orchestrator.py::_run` → `breaker.call(...)`),
   which in V1 are **in-process, repository-backed stubs** (`DeploysFetcher`
   queries `deploy_repo`, etc.). There is no outbound HTTP to a Sentry/Datadog
   API to intercept — those "external integrations" are not wired in V1
   (non-goal). So toxiproxy has nothing to sit in front of for the breakers.

2. **Kafka advertises its own listener address.** A client bootstraps against the
   broker and is handed back the broker's advertised host:port, then reconnects
   there directly — around any proxy. Fronting Kafka with toxiproxy requires
   rewriting advertised listeners and is brittle; it does not cleanly model "Kafka
   is down" for a transactional-outbox app.

## Decision

Split the chaos suite by what each tool actually models:

- **toxiproxy for infra-dependency outages** (`tests/chaos/test_infra_chaos.py`).
  A toxiproxy container fronts **Redis** (the ingestion idempotency store);
  Postgres and Kafka run direct. The suite disables the proxy and asserts the
  synchronous ingestion path **fails closed** (`503 INFRA_UNAVAILABLE`, no
  incident written), recovers when the proxy is re-enabled, and does not drop
  events under a latency toxic. This is the spec's "latency and failures into
  Redis → graceful degradation" case, verified against a live stack.

- **In-process fault injection for the breakers**
  (`tests/chaos/test_breaker_chaos.py`). A `_FaultFetcher` whose `fetch()` raises
  is wired into `EnrichmentDeps` with a real `CircuitBreaker`. The suite drives
  `assemble()` under sustained fault and asserts the breaker opens after
  threshold, short-circuits subsequent calls, keeps the other context sections
  healthy (`assemble` never raises), recovers after cooldown, and moves the
  `sentinel_circuit_breaker_state` gauge. This is the faithful equivalent of
  "toxiproxy on the integration" given the breakers guard in-process callables,
  and it is **creditless and Docker-free**.

- **Kafka outage is covered elsewhere, not via toxiproxy.** The transactional
  outbox (ADR 0001) means a Kafka outage cannot drop an `incident.opened` event:
  the row is committed with the incident and the drainer republishes on recovery.
  That property is already integration-tested in
  `tests/integration/ingestion/test_outbox_drainer.py` and
  `test_drainer_resilience.py`, so the chaos suite does not re-implement it with a
  brittle Kafka proxy.

## Consequences

- The chaos suite has two layers under one `chaos` pytest marker, run by
  `make chaos`. B2 (breaker) needs no Docker; B1 (toxiproxy) needs Docker.
- Both layers keep traffic off the Anthropic API: B1 runs the app with
  `diagnosis_consumer_enabled=false` + `memory_consumer_enabled=false`; B2 never
  calls an LLM. The chaos suite costs zero Anthropic credits.
- New dev dependency `toxiproxy-python` and a session-scoped testcontainers stack
  (Postgres + Redis + Kafka + toxiproxy on a shared network).
- The suite is excluded from PR CI (extra services + runtime); it runs via
  `make chaos` and an optional nightly/manual job. PR CI stays fast.
- **Coverage gap acknowledged:** `/readyz` does not yet probe Postgres/Redis/Kafka
  connectivity — it only checks consumer-task aliveness (see
  `api/routes/health.py`; dependency probes are deferred to the API layer / Work
  Area I). So B1 asserts the ingestion-path failure mode, not a readyz dependency
  flip. When the readyz probes land, a readyz-under-outage assertion should be
  added here.

## References

- Spec §"Testing" (load / chaos tiers)
- ADR 0001 (transactional outbox — why Kafka outage ≠ dropped events)
- `tests/chaos/test_infra_chaos.py`, `tests/chaos/test_breaker_chaos.py`
- `tests/chaos/conftest.py` (toxiproxy-fronted Redis stack)
- `plans/2026-06-15-load-chaos-test-suites-design.md`
