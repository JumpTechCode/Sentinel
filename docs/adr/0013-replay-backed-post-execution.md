# 0013 — Replay-backed POST execution for /diagnose

**Status:** Accepted
**Date:** 2026-06-15

## Context

`POST /incidents/{id}/diagnose` re-runs the LLM diagnosis on demand. The project
holds a hard **zero-live-Anthropic-spend** constraint for local/dev/CI/demo, and
the single-shot diagnosis pipeline already records and replays LLM HTTP through
the cassette transport (`sentinel/evals/cassette.py`), keyed by
`(prompt_version, model_id, case_id, shot_index)`.

The diagnosis agent (`diagnose()`) and its dependencies were previously built
only inside the `if settings.diagnosis_consumer_enabled:` block of the app
lifespan — the HTTP route needs them regardless of whether the Kafka consumer
runs.

## Decision

`/diagnose` reuses the out-of-band `diagnose()` agent and `save_with_outbox()`
persistence rather than a separate code path. When the app runs in
eval/cassette mode the LLM call goes through the cassette transport in replay
mode; the route stamps a `CassetteContext` keyed on the **incident id**
(`shot_index=0`). A cassette miss fails closed with **409
`no_recording_available`** instead of making a live call. Unit tests inject a
fake LLM client; CI never spends credits. A live call happens only in a
deployment that is explicitly *not* in cassette mode and has a real API key.

The diagnosis agent deps (`AnthropicClient`, prompt bundle, audit logger) and
`diagnosis_repo` are built independent of `diagnosis_consumer_enabled` and
stashed on `app.state`, so the HTTP route works with the Kafka consumer
disabled (e.g. the creditless API test deployment).

The manual trigger has no Kafka envelope, so `save_with_outbox` is called with a
freshly synthesized `upstream_event_id` and an `incident.diagnosed` outbox
event mirroring the consumer.

## Consequences

- A demo `/diagnose` on a brand-new (non-corpus) incident needs a recorded
  cassette for that incident's id, or it returns 409. The compose demo uses
  seeded incidents with committed cassettes.
- Persistence is idempotent on `(incident_id, prompt_version, model)`; re-running
  with unchanged prompt+model returns `persisted: "duplicate"` and writes no
  second outbox event.
- The manual route does **not** call `mark_diagnosing` — a recompute must not
  regress the incident's lifecycle status.
- The suggest-only invariant is untouched: the route never executes actions.
- Building the agent deps unconditionally means the Anthropic API key
  (`anthropic_api_key`, a required `Settings` field) must be present at boot, as
  it already was — `Settings` validation enforces it regardless of this change.
