# ADR 0001 — Transactional outbox for webhook → Kafka emission

**Status:** Accepted
**Date:** 2026-05-18

## Context

Webhook ingestion (`POST /webhooks/{source}`) must persist an incident in Postgres
and emit an `incident.opened` (or `incident.recurred`) event to Kafka so the
enrichment worker can pick it up. These are two writes to two systems — the
classic dual-write problem.

If we commit Postgres then emit to Kafka, an outage between the two loses the
event silently; the incident exists but never gets diagnosed. If we emit to Kafka
then commit Postgres, we can publish an event for an incident that fails to
persist. Either ordering is wrong.

## Decision

We adopt the **transactional outbox pattern**. The webhook handler writes the
incident row and an `outbox_events` row in a single Postgres transaction. A
background `OutboxDrainer` task polls `outbox_events` under `FOR UPDATE SKIP
LOCKED`, emits each row to Kafka, and marks it published — all within its own
transaction. The webhook request path never touches Kafka.

Properties:

- **At-least-once delivery** to Kafka. The Kafka producer is configured with
  `enable_idempotence=True` so consumers can dedupe on incident ID.
- **No event loss** during Kafka outages — rows remain `published_at IS NULL`
  until the drainer succeeds; backoff is `min(2^attempts, 300)` seconds.
- **Concurrent drainers** are safe under `SKIP LOCKED`.
- **Fast 202** — the webhook handler completes its work without waiting on
  Kafka, preserving the spec's "diagnosis runs out of band" contract.

## Alternatives considered

- **Emit-then-commit / commit-then-emit with a metric.** Simpler but loses
  events on Kafka outage. A counter tells you something went wrong but does not
  recover. Not acceptable for a production-grade portfolio repo whose audience
  asks about the dual-write problem within five seconds of reading the route
  handler.
- **Change Data Capture (Debezium etc.).** Powerful but introduces a heavy
  dependency. Overkill for a single-table outbox pattern at this project's
  scale.

## Consequences

- One new table (`outbox_events`) and one background task (`OutboxDrainer`)
  must be operated, monitored, and pruned.
- Webhook → Kafka latency gains ~250ms (drainer poll interval) under nominal
  conditions. Acceptable: enrichment has a 5s per-fetcher budget downstream.
- The pattern is reusable when diagnosis or memory later need to emit events
  — no need to redesign each time.
