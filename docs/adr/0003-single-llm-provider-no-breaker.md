# ADR 0003 — Single LLM provider, no per-LLM circuit breaker (V1)

## Status

Accepted, 2026-05-19. Revisit when a second LLM provider is added.

## Context

The spec's runtime invariant 1 says "every external call has a timeout and a circuit breaker." Diagnosis is an external call: it talks to Anthropic. In V1, Anthropic is the only LLM provider — and is so by design (spec §non-goals).

A circuit breaker between Sentinel and Anthropic, in V1, would do one useful thing (fail fast during a sustained Anthropic outage) and one less-useful thing (delay recovery once Anthropic comes back, while we wait for cooldown). With one provider and no fallback, the breaker mostly converts "the LLM is down" into "diagnosis is down faster" — both are total failures.

## Decision

V1 ships diagnosis with:

- A hard 30s timeout per call (`asyncio.wait_for`).
- One short-backoff retry inside the client on HTTP 5xx/429.
- No per-LLM circuit breaker.
- Failures fall through to Kafka redelivery (the consumer does not commit on `LLMTimeout` / `LLMTransport`).

When a second LLM provider lands (none planned in V1), introduce a breaker at that point and wire fallback. Per-provider breakers will be one of the first things added when that happens.

## Consequences

- During an Anthropic outage, Kafka will retry diagnosis events. Eventually they hit the DLQ. Operators see `sentinel_diagnosis_failures_total{reason="timeout"|"transport"}` rising and have to mute the incident pipeline or wait.
- This is acceptable for V1 — Sentinel is suggest-only, and a diagnosis delay does not block remediation.
- The omission is intentional and tracked here so future reviewers don't have to re-derive the reasoning.
