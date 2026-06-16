# 0014 — Defer POST /evals/runs (eval runs are CLI-triggered)

**Status:** Accepted
**Date:** 2026-06-15

## Context

The spec's API contract lists `POST /evals/runs`. PR 2b ships the eval **read**
surface (`GET /evals/runs`, `/evals/runs/{id}`, `/evals/baseline`); this ADR
records why the trigger endpoint is deferred rather than built alongside them.

The eval runner's canonical (shot 0) path fires a real webhook through the full
pipeline — `POST /webhooks/{source}` → Kafka → enrichment → diagnosis — creating
incident and diagnosis rows. A corpus run also depends on a clean Kafka topic:
the workflow runs `make evals-reset` (delete the topic + flush Redis + truncate
Postgres) before each run, because stale messages on the topic corrupt results.
The runner only functions in `eval_mode` (cassette replay + the in-process
ASGI client + the diagnosis consumer running).

## Decision

Ship the read endpoints and **defer** `POST /evals/runs`. Triggering a corpus
run stays a CLI operation (`make evals` / `sentinel.evals.cli`):

- An API-triggered run would write eval incidents into whatever database the
  app points at — polluting production incidents — and it cannot guarantee the
  `make evals-reset` precondition, so a stale Kafka topic could silently corrupt
  the run.
- It would duplicate the CLI, which already assembles the run deterministically
  (`_discover_run_metadata` → `start_run` → `run_corpus` → `finalize_run`).
- The eval dashboard's value (Work Area L) is *reading* run history, trends, and
  the current baseline — which the read endpoints fully serve. Runs are produced
  by CI (the nightly full-corpus job + the per-PR evals-gate) and the local CLI,
  then surfaced read-only.

## Consequences

- The dashboard reads `/evals/runs`, `/evals/runs/{id}`, `/evals/baseline`; it
  does not start runs.
- If an API trigger is ever genuinely needed, it must be `eval_mode`-gated, run
  against a dedicated eval database (not the app's primary DB), and own the
  topic/Redis reset itself — a materially larger change that warrants its own
  ADR superseding this one.
- The spec's `POST /evals/runs` line is intentionally unbuilt; this ADR is the
  record of that deviation.
