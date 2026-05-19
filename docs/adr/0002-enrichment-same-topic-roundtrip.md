# ADR 0002 — Enrichment publishes `incident.enriched` to the same `sentinel.incidents` topic

**Status:** Accepted
**Date:** 2026-05-19
**Phase:** 4 — Enrichment (Work Area F)

## Context

The enrichment pipeline (Work Area F) consumes `incident.opened` /
`incident.recurred` events from the `sentinel.incidents` Kafka topic, assembles
`IncidentContext`, persists it, and emits an `incident.enriched` event via the
existing transactional outbox (see ADR 0001).

The question: does `incident.enriched` go to its own topic (e.g.
`sentinel.incidents.enriched`) or to the same `sentinel.incidents` topic?

## Decision

For V1, `incident.enriched` is published to **the same** `sentinel.incidents`
topic. The enrichment consumer filters by the `event` field at parse time so it
does not re-process its own output. Filtering happens on the raw JSON
(`payload.get("event")`) **before** full Pydantic validation, because the
`incident.enriched` envelope intentionally has a slimmer shape (no
`fingerprint` / `source`) than `IncidentEvent` requires.

## Consequences

- **Pro**: one topic to provision, monitor, and replay. No risk of topic-config
  drift between input and output streams. The outbox is already wired to a
  single topic; separating would require a routing layer.
- **Pro**: future diagnosis consumer (Work Area G) subscribes to a single topic
  with a distinct consumer group; the existing `event` filter pattern carries
  over.
- **Con**: the enrichment consumer sees its own messages on every poll.
  Mitigated by early-filter parsing: peek the `event` field BEFORE full
  Pydantic validation, so payloads with a different shape (e.g.
  `incident.enriched` has no `fingerprint`) don't pollute
  `sentinel_enrichment_invalid_events_total`. Without this guard, the consumer
  self-poisons its own poison-pill metric on every round-trip.
- **Con**: if a future event type is added with the same envelope shape as
  `incident.opened` but should NOT trigger enrichment, the consumer's filter
  list (`{"incident.opened", "incident.recurred"}`) needs explicit update.

## Alternatives considered

- **Separate topic `sentinel.incidents.enriched`**: cleaner separation but adds
  topic-config surface area (retention, partition count, replication). The
  outbox drainer would need a routing rule per outbox payload. Re-evaluate when
  Work Area G lands if the diagnosis consumer wants different retention or
  partitioning than ingestion does.
- **Validate envelopes first, then filter**: simpler control flow but every
  `incident.enriched` message increments
  `sentinel_enrichment_invalid_events_total`, masking real poison-pill
  incidents. The two-stage parse is strictly better.

## Related

- ADR 0001 — Transactional outbox for webhook → Kafka emission
- Plan: `plans/2026-05-19-enrichment-plan.md`
- Design: `plans/2026-05-19-enrichment-design.md`
- Code: `sentinel/enrichment/consumer.py`, `sentinel/schemas/enrichment_event.py`
