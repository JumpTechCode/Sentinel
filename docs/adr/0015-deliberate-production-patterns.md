# ADR 0015 — Deliberately built to full production patterns (and when to collapse them)

## Status

Accepted, 2026-06-16. Revisit if Sentinel ever targets a real single-tenant deployment rather than serving as a portfolio demonstration of distributed-systems patterns.

## Context

Sentinel is a public portfolio repository held to a production engineering bar. A fair critique of the architecture — for the *workload it actually serves* — is that it is over-built: a single-tenant, single-shot diagnosis copilot at low incident volume does not strictly need a Kafka broker, a transactional outbox, and a separate consumer group. The same correctness could be had with a simpler topology.

That critique is correct on the merits and worth stating plainly rather than hiding. The reason the apparatus exists anyway is that the repository's purpose is to **demonstrate that these patterns can be designed and built correctly**, not to ship the minimal system that solves the immediate problem. Breadth-of-production-patterns-executed-correctly is the deliverable here, so the apparatus is a feature, not an accident.

This ADR records that trade-off explicitly so the choice reads as judgment rather than gold-plating, and so a future reader knows exactly what to remove if they ever repurpose this for a real low-volume deployment.

## Decision

V1 deliberately implements full production patterns even where a smaller system would suffice:

- **Transactional outbox + Kafka** for webhook → out-of-band diagnosis (ADR 0001, ADR 0002), rather than running diagnosis inline in the request path or polling a table.
- **Per-dependency circuit breakers + timeouts** on every fetcher (invariant 1), rather than bare `try/except`.
- **pgvector incident memory** with embed-on-resolve (ADR 0005), rather than keyword search over past incidents.
- **A statistically-rigorous eval harness** with paired-bootstrap regression gating, rather than spot-checking a few prompts.

These are kept because demonstrating them — correctly, with ADRs and tests — is the point.

## Consequences

**The honest simplification.** A real single-tenant deployment at low incident volume would collapse this topology:

- **Drop the Kafka broker; use the outbox table as the queue.** The `outbox_events` table already exists and is written in the same transaction as the incident (ADR 0001). A `SELECT ... FOR UPDATE SKIP LOCKED` poller over that table gives the same at-least-once, ordered-per-incident delivery without a broker to operate. The consumer classes (`EnrichmentConsumer`, `DiagnosisConsumer`) are already self-contained (ADR 0004), so the change is localized to the transport, not the processing logic.
- **Keep everything else.** Circuit breakers, the evidence-citation gate, idempotency, embed-on-resolve, and the eval harness are not workload-dependent — they are correctness and quality machinery that earns its keep at any scale.

The result would be a Postgres + Redis + FastAPI deployment with no broker: materially simpler to operate, with the same correctness guarantees. This is the deployment a real single-tenant user should run.

**What this ADR is not.** It is not a deprecation. V1 keeps the full topology on purpose. It is a written acknowledgement of the seam so that the "isn't this over-built?" question has a documented, considered answer instead of a defensive one.

## Triggers to revisit

- The repository is forked for an actual single-tenant deployment (collapse Kafka into the outbox-as-queue per above).
- Multi-tenancy or cross-service fan-out is introduced (the broker earns its place; this ADR no longer applies).
