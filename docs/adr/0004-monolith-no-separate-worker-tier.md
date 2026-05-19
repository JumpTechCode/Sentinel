# ADR 0004 — API process owns Kafka consumers; no separate worker tier (V1)

## Status

Accepted, 2026-05-19. Revisit when (a) diagnosis throughput requires independently scaled workers, (b) CPU-bound background work is added that would starve the request path, or (c) workers gain resource needs distinct from the API (e.g., GPU for local embeddings).

## Context

Work Area A scaffolded a `sentinel-worker` console script and a `worker` service in `docker-compose.yml`, with the intent that long-running Kafka consumers would live in a separate process. The actual stub at `sentinel/workers/diagnosis_worker.py` only logged "idle (stub) — waiting for shutdown" and blocked on `stop.wait()`.

Phase 5 (Work Area G) wired both `EnrichmentConsumer` and `DiagnosisConsumer` into the API's FastAPI lifespan (`sentinel/api/app.py`). That made the integration test and `make compose-up` work end-to-end, but left two problems:

1. In any deployment that actually ran the `worker` container, the worker did nothing while the API container did all the consumption.
2. The split process topology was suggested in compose but never realized in code, creating ambient confusion about where things run.

Three options were considered (see issue #20):

- Move consumers to the worker process. Production-correct for an eventual scale-out, but splits the lifecycle, complicates the integration test, and adds a moving part (a second container) without a current workload that benefits from it.
- Remove the worker service and consolidate on a single API process that runs consumers in its lifespan. Honest about what the codebase actually is.
- Run consumers in both processes. Rejected: same group is partition-split (works but confusing); different groups double-process (broken).

## Decision

V1 has no separate worker tier. The API process is the only long-running process; it owns the FastAPI HTTP server plus the `EnrichmentConsumer` and `DiagnosisConsumer`, all managed by the FastAPI lifespan.

Concretely:

- The `worker` service is removed from `docker-compose.yml`.
- The `sentinel-worker` console script and `sentinel/workers/diagnosis_worker.py` stub are deleted.
- The `sentinel/workers/` package is removed; there is no longer a "workers" boundary in the codebase.
- README and module map reflect the monolithic process topology.

The seam is preserved: `EnrichmentConsumer` and `DiagnosisConsumer` are self-contained classes with explicit `start()`/`stop()`. Moving them to a separate entrypoint later is a small, localized change.

## Consequences

**Production behavior in V1**

- Horizontal scale = N API replicas sharing a Kafka consumer group. Kafka rebalances partitions across replicas; no double-processing.
- Partition count on `incident.opened` / `incident.enriched` caps consumer concurrency. If partition count is 1, only one replica consumes regardless of replica count. Operators deploying Sentinel must size partitions for expected throughput.
- Rolling deploys trigger consumer-group rebalances (standard Kafka behavior). In-flight messages are redelivered to the new owner; idempotency on `(incident_id, prompt_version)` (diagnosis) and outbox semantics (ingestion) keep this safe.
- The API request path and the diagnosis path share the same event loop. Both are I/O-bound (LLM calls dominate latency), so async scheduling handles contention. If we later add CPU-bound enrichment, this assumption breaks and we revisit.
- Graceful shutdown: lifespan must drain in-flight diagnoses on SIGTERM. Already implemented in the consumer's `stop()`.

**What this is not**

- Not a claim that Sentinel cannot scale. It can — by adding API replicas and partitions.
- Not a permanent rejection of a worker tier. The seam is preserved; this is a "not yet" decision.

**Triggers to revisit**

- Diagnosis volume sustained at >10× ingest QPS (different scaling shape).
- Background work added that is CPU-bound or has distinct resource needs.
- Independent autoscaling signals required (e.g., scale workers on Kafka lag, scale API on request rate).

When any of those land, introduce a `sentinel-consumer` entrypoint and a separate compose service; the consumer classes are already self-contained.
