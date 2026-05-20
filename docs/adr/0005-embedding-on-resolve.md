# 0005 — Embedding on resolve, not on open

**Status:** Accepted
**Date:** 2026-05-19

## Context

Sentinel's similar-incident retrieval depends on a `vector(1024)` embedding stored on `incidents.embedding`. Two natural moments to compute or update that vector:

1. **At open** — embed the alert title (`service + title`) so the row is searchable from the moment it lands.
2. **At resolve** — recompute from `title + root_cause + remediation` once the operator has captured what actually happened.

We do both. This ADR records *why*, and which alternatives we rejected.

## Decision

- **At open**, MemoryConsumer computes `embed(f"{service} {title}")` and writes it. This makes the row immediately retrievable — useful while it's still being diagnosed.
- **At resolve**, MemoryConsumer re-computes `embed(f"{title}\n{root_cause}\n{remediation}")` and overwrites the column. This is the "gets smarter from use" loop: the embedding now reflects the *actual cause*, not the surface symptom.

Both writes go through `IncidentRepository.set_embedding`, which is idempotent on `last_embedding_event_id`.

## Why

The opened-path embedding is the fallback while we wait for human truth. The resolved-path embedding is the actual learning signal. A query like *"db pool exhaustion"* against a corpus of resolved incidents matches *because* their embeddings encode the words *pool exhaustion* from root cause, not just whatever surface symptom the alert title said (often misleading: "high latency", "5xx surge").

The retrieval filter (`status IN ('resolved','closed') AND diagnosis_was_correct IS NOT FALSE`) means only post-resolve embeddings actually get retrieved. The opened-path embedding is essentially insurance against the case where we want to query the open corpus later.

## Alternatives rejected

- **Embed only on resolve.** Loses retrievability of in-flight incidents. We do not currently need that, but the cost of also embedding on open is one extra ~100ms compute per incident, paid asynchronously off the request path.
- **Re-embed on every status change.** Wasteful — only `resolved` carries new information.
- **Embed at diagnosis time** (e.g., embed the hypothesis). Diagnoses are model-generated; embedding them feeds the model's interpretation back into retrieval, amplifying its biases. We embed only operator-confirmed text.
- **Persist multiple embedding versions per incident.** Adds complexity for unclear benefit at V1. The single-row pattern is sufficient and aligned with the spec.

## Consequences

- A resolved incident takes one extra MemoryConsumer event to become discoverable in its post-resolve form.
- If the consumer is stalled, retrieval still works but with stale embeddings. The outbox + at-least-once guarantee means no permanent loss.
- ADR 0006 (local embeddings) makes the compute essentially free, so embedding on both events has negligible cost.
