# 0009 — Multi-shot stability at the LLM-call level

**Status:** Accepted
**Date:** 2026-06-15

## Context

The eval harness reports a per-case **stability** number: the sample stddev of
each scorer across `--shots` repeated diagnoses of the same case. It exists to
answer "how much does the model's answer wobble run-to-run?" — a distinct
question from "how good is the answer?".

It was structurally broken (issue #49). The runner drove every shot through the
full production pipeline (`POST /webhooks/{source}` → Kafka → enrichment →
diagnosis). Production fingerprinting is `sha256(service ‖ normalized_title ‖
severity)` with no per-call entropy (ADR-level invariant), so all shots of one
case share a fingerprint. Within the 1h dedup window the second webhook updates
the existing incident rather than creating a new one, and the diagnoser's
idempotency — the `uq_diagnoses_incident_prompt_model` UNIQUE constraint on
`(incident_id, prompt_version, model_id)` — means shots 1+ never produce a fresh
LLM call. The runner got shot 0's diagnosis back N times. `mean_stability` was
`0.000` across every case and metric: the stat looked perfect because it was
measuring nothing.

Issue #49 laid out three options.

## Decision

**Option 3: measure stability at the LLM-call level, decoupled from
persistence.**

- **Shot 0** runs the full webhook → Kafka → enrich → diagnose pipeline and is
  persisted as the single canonical `EvalCaseResultRecord` for the case. The
  production fingerprint + idempotency contract is exercised here and only here.
- **Shots 1+** call `sentinel.diagnosis.agent.diagnose(...)` directly from the
  runner, against an `IncidentContext` reconstructed from the corpus seed (the
  same translation the scoring path already uses) and a throwaway fabricated
  `IncidentDetailResponse`. They are **not** persisted — there is no incident
  row to foreign-key against — and exist only as `MetricSet` entries feeding the
  per-case stddev.

Each shot's Anthropic response still resolves through the shared cassette
transport: the runner stamps the `(prompt_version, model_id, case_id,
shot_index)` key before every shot, so replay is deterministic and recording
captures one cassette per `(case, shot_index)`.

The mechanism lives entirely in the eval module
(`sentinel/evals/runner.py::_run_in_memory_shot`) plus a one-line lifespan
exposure (`app.state.diagnosis_deps`). No production code path changes.

## Why not the other options

**Option 1 — per-shot fingerprint divergence.** Push the per-shot `external_id`
(`<case_id>-shot-<i>`) into the fingerprint so each shot is a distinct
`(incident, fingerprint)` pair. Rejected: it forks the fingerprint invariant for
eval mode. Fingerprint = `sha256(service ‖ normalized_title ‖ severity)` with no
per-call entropy is load-bearing for production dedup; an "eval-mode" branch in
fingerprint logic is exactly the kind of test-only divergence that later hides a
real dedup bug.

**Option 2 — drop the UNIQUE constraint, key on `(incident_id, prompt_version,
model_id, shot_index)`.** Cleanest *schema* fit, but it bends the production
idempotency contract to serve an eval concern, and forces a question we don't
need to answer ("should a true repeat of the same `(incident, prompt, model)`
ever overwrite in prod?"). It also requires a reversible migration for no
production benefit. Rejected as scope the eval feature shouldn't drag in.

## The decoupling (the actual point)

Stability and persistence measure **different** things and should not be the
same mechanism:

- **Persistence** records what the *production* contract produces: one diagnosis
  per `(incident, prompt, model)`, idempotent on redelivery. The eval harness
  asserts that contract via shot 0.
- **Stability** measures *LLM-call non-determinism*: same prompt, repeated
  calls, how much does the scored output move. That's a property of the model,
  not of the database.

Conflating them (Options 1/2) was what produced a confidently-wrong `0.000`.
Option 3 keeps the production contract pristine and measures the model directly.

## Consequences

- `eval_case_results` stays one row per case (shot 0). Stability shots leave no
  shadow rows; `corpus_size × 1` is the persisted row count regardless of
  `--shots`.
- `--shots` defaults to 3 for `run` / `baseline` / `record`. Recording now
  writes one cassette per `(case, shot_index)`; the committed cassette set and
  the baseline both reflect a 3-shot run.
- `run_corpus` fails loud if `shots_per_case > 1` without `diagnosis_deps`
  wired, rather than silently single-shotting.
- An in-memory shot that hits hallucinated evidence still gets its confidence
  capped inside `diagnose()`; the capped score flows into the stddev like any
  other shot.
- **`pass_rate_strict` drops 0.90 → 0.80, and that is the honest number.**
  `_case_passes_strict` requires `category_match == 1.0`, and with multi-shot
  the per-case `category_match` is the mean across shots. Two cases
  (`github-2018-10-21-network`, `roblox-2021-10-28-consul`) had a shot 1+ pick a
  category outside `acceptable_categories`, so their mean falls below 1.0 and
  they no longer pass strict. This is a deliberate semantic strengthening: with
  `--shots 3` the default, "strict pass" now means *the model got the category
  right on all three independent shots*, not just on the one persisted
  diagnosis. The single-shot 0.90 measured one sample; the 0.80 measures
  consistency, and the `stability` column shows exactly which cases wobble. The
  paired-bootstrap regression gate covers the four scorer metrics, not this
  headline, so the move is documented here rather than caught by CI.
- The in-memory shots audit-log under a fabricated incident id (`uuid4()`) that
  has no row in `incidents`. The LLM audit log is an append-only cost/latency
  ledger, not a queryable join target, so these "orphan" ids are acceptable; if
  the audit log ever becomes join-able, the `eval_shot_diagnoses` table above is
  where shot-level identity would live.

## Deferred alternative

If "every shot's diagnosis is queryable" ever becomes a real ask (e.g. to chart
which shots disagreed), add an `eval_shot_diagnoses` table keyed on
`(run_id, case_id, shot_index)` with no FK to `incidents`. Not built now: the
stddev is the only consumer of shot-level data today, and it lives in memory for
the duration of the run.

## References

- Issue #49
- `plans/2026-05-21-evals-pr5-7-design.md` (PR 6 section)
- `sentinel/evals/runner.py` (`_run_in_memory_shot`, `_full_pipeline_shot`)
- `sentinel/api/app.py` (`app.state.diagnosis_deps` exposure)
- ADR 0007 (evidence-id match contract the scores rely on)
