# Eval harness (Work Area K) — design

**Status:** approved (brainstorm), implementation plan to follow.
**Date:** 2026-05-20.
**Scope:** Work Area K per `sentinel-claude-code-prompt.md` §Eval harness + Build phases (Week 5). Plan size: L. Depends on B/F/G/H (all landed).

Phase 5 of Sentinel. Builds the eval harness — the project's stated **differentiator**. Real ground-truth evals against curated public postmortems, with a paired-bootstrap regression gate on merges, mean-with-variance shot aggregation, cassette-replay for PR smoke + live calls nightly, and full per-shot persistence to Postgres so trends are queryable and runs are debuggable.

Core scope: 10-case corpus + runner + scoring + persistence + baseline-tracked regression gate + CI wiring + README auto-update.

Out of scope: corpus expansion to 30 cases (Week 6), Next.js dashboard for eval trends (Work Area I), claim-entailment / Ragas-style faithfulness scoring (deferred — current evidence-ID-resolution gate is stricter than industry default).

---

## 1. Module layout & responsibilities

```
sentinel/evals/
├── __init__.py
├── schema.py                # Pydantic models: CorpusCase, GroundTruth, RunMetadata,
│                            #   MetricSet, ShotResult, CaseResult, RunResult
├── corpus/
│   ├── __init__.py          # loader; validates all *.yaml against schema at import
│   ├── *.yaml               # one file per postmortem (10 to start)
│   └── README.md            # curation guidelines for the corpus
├── corpus_loader.py         # parse + validate YAMLs; fail-loud on schema drift
├── fetcher_override.py      # CorpusFetcher — returns context_seed verbatim;
│                            #   swapped in for real fetchers during eval runs
├── cassette.py              # VCR-style record/replay layer for Claude HTTP calls
│                            #   keyed by hash(prompt_version, model_id, case_id)
├── runner.py                # orchestrates: load → fire webhook → await diagnosis → score
├── scoring.py               # pure functions over (Diagnosis, GroundTruth, IncidentContext)
├── stats.py                 # bootstrap CI, paired-difference regression gate
├── report.py                # writes results/<run_id>.{json,md}; computes baseline diff
├── readme_patcher.py        # patches README between <!-- evals:start --> markers
├── baselines/
│   └── v1.json              # COMMITTED; the active baseline for the regression gate
├── results/                 # GITIGNORED; per-run JSON + MD artifacts
└── cli.py                   # `python -m sentinel.evals` entrypoint
                             # subcommands: run, record, baseline, report, readme

sentinel/persistence/
├── repositories/
│   └── eval_runs.py         # EvalRunsRepository — the ONLY caller of the eval tables
└── alembic/versions/
    └── XXXX_eval_runs.py    # migration: eval_runs + eval_case_results (lands FIRST,
                             #   separate PR before the runner)
```

**Boundary rules:**
- `scoring.py` and `stats.py` are pure functions — no I/O, fully unit-testable.
- `runner.py` is the only piece that knows about HTTP/compose/Kafka.
- `cassette.py` injects a custom `httpx.AsyncClient` into `anthropic.AsyncAnthropic(http_client=...)` (SDK ≥0.39 supports this) to capture/replay LLM HTTP calls. It does **not** mock the diagnosis agent. Ingestion + Kafka + enrichment + scoring all run for real even under cassette replay.
- `evals/` never touches the DB directly — goes through `EvalRunsRepository` and (for diagnosis polling) `DiagnosisRepository`.
- `evals/` reuses `sentinel.memory.embeddings.FastEmbedProvider` for hypothesis/action cosine — same embedding space as production → numbers comparable across systems.
- `fetcher_override.py` lives in `evals/` (not `enrichment/`) — it's a test-time concern; production fetchers stay clean.

**Fetcher-override mechanism (concrete):**
`EnrichmentDeps.fetchers` is currently constructed once in `sentinel/api/app.py::lifespan()` from `default_fetchers()` and held immutably for the consumer's lifetime — there is no FastAPI dependency-override hook on this path. The eval mode introduces a settings flag (`SENTINEL_EVAL_MODE=true`) and a settings field `eval_corpus_path: Path | None`. When set, `lifespan()` takes a branch:
```python
if settings.eval_mode:
    fetchers = (CorpusFetcher(settings.eval_corpus_path),)
else:
    fetchers = default_fetchers(...)
```
`CorpusFetcher` reads the active case from a per-request lookup (the alert's `service` + `title` keys the corpus case), bypassing breakers/timeouts since the data is local fixture, not external I/O. The eval runner sets `SENTINEL_EVAL_MODE=true` in the FastAPI process's environment for the duration of the run.

---

## 2. Corpus YAML schema

One file per case under `sentinel/evals/corpus/`. Validated by `schema.py` Pydantic models at module import (fail-loud — broken YAML breaks `make evals` immediately, not at run time).

```yaml
id: cloudflare-2022-06-21-bgp           # also the filename stem
corpus_version: 1                       # bump on any schema change
source_url: https://blog.cloudflare.com/cloudflare-outage-on-june-21-2022/
sources_consulted:
  - https://blog.cloudflare.com/cloudflare-outage-on-june-21-2022/
notes: |                                # curator's free-text rationale; not scored
  Picked because BGP/config + clear remediation in the writeup.

# Synthetic webhook payload — fed into POST /webhooks/{source}
alert:
  source: generic                       # generic|sentry|pagerduty|datadog
  service: edge-network
  severity: SEV1
  title: "Elevated 5xx errors across multiple POPs"
  timestamp: "2022-06-21T06:27:00Z"
  raw_payload: { ... }                  # opaque blob matching the source adapter's contract

# What enrichment "would have found." CorpusFetcher returns this verbatim.
context_seed:
  deploys:
    - id: deploy-abc123                 # IDs stable so EvidenceRef.id can cite them
      service: edge-network
      sha: abc123
      pr_title: "Update BGP route propagation policy"
      pr_diff_summary: "Modifies prefix-list filter ordering"
      deployed_at: "2022-06-21T06:25:00Z"
  related_alerts: []
  logs:
    - id: log-1
      timestamp: "2022-06-21T06:27:12Z"
      level: error
      service: edge-network
      message: "BGP session reset peer 1.2.3.4"
  similar_incidents: []                 # populated only when testing the memory loop
  runbooks: []

ground_truth:
  category: config                      # primary label
  acceptable_categories: [config, deploy]  # avoids penalising defensible alternative labels
  root_cause: "BGP misconfiguration during a planned network change caused route propagation failures."
  correct_actions:
    - "Revert the BGP configuration change"
    - "Roll back the deploy"
```

**Notes:**
- `acceptable_categories` lets the category metric give partial credit when multiple labels are defensible.
- Stable IDs on every `context_seed` item are the contract the evidence-quality metric depends on (same contract production uses).
- `fetcher_fixture_hash` (computed at runtime, persisted to `eval_runs.fetcher_fixture_hash`) is `sha256` of the canonical-JSON-serialized `context_seed` blocks loaded for the run — so a corpus edit shows up as a metadata diff, not a silent baseline drift.

---

## 3. Runner & end-to-end flow

Per case, per shot (N=3 by default, N=5 for weekly tagged baseline):

```
0. Setup (once per run, not per case)
   - Spin up FastAPI process with SENTINEL_EVAL_MODE=true and a dedicated DATABASE_URL
     pointing at a separate test database `sentinel_evals_test` (alembic head applied).
     Production-equivalent code path runs end-to-end, but in an isolated DB.
   - eval_runs + eval_case_results live in the SAME `sentinel_evals_test` database
     during a CI run; nightly/baseline runs against a dedicated long-lived DB
     (`sentinel_evals`) so trends accumulate.
   - The eval runner connects directly to the same DB for repository reads
     (no extra HTTP endpoint needed for diagnosis polling).

1. Reset state between cases (no cross-case contamination)
   - TRUNCATE incidents, incident_events, diagnoses, outbox, embeddings_queue
     in the test DB (CASCADE)
   - FLUSHDB on the dedup Redis namespace
   - Per-run Kafka consumer group: evals-<run_id> (so we don't fight other consumers
     and offsets don't persist across runs)

2. Load corpus case
   - CorpusFetcher's active-case registry is updated to point at this case
     (read by the fetcher when enrichment fires)

3. Fire the webhook
   - POST /webhooks/{case.alert.source} with case.alert.raw_payload + valid HMAC
   - HMAC signed via sentinel.integrations.base.compute_hmac_sha256 using the
     adapter's test secret (resolved via settings)
   - Assert 202 + incident_id returned in the response body

4. Await diagnosis
   - Poll DiagnosisRepository.get_by_incident_id(incident_id) every 250ms; timeout 60s
   - This method DOES NOT EXIST YET — added in PR 1 alongside the schema migration
   - On timeout: ShotResult.case_status = 'timeout', metrics all 0

5. Score (pure functions in scoring.py)
   - 4 metrics → MetricSet for this shot
   - Persist full ShotResult to eval_case_results (including raw_response JSONB)

6. Per-case aggregation across the 3 shots
   - metric mean across shots → per-case MetricSet
   - metric stddev across shots → per-case stability vector
   - flag stability_high cases (stddev > 0.15) for corpus review

7. Per-run aggregation across cases
   - mean of per-case means → run-level MetricSet
   - bootstrap CI on per-case scores → run-level CI for the gate
```

**Failure modes the runner names explicitly:**
- Webhook 5xx → `ShotResult.case_status = 'ingest_failed'`, metrics 0.
- Diagnosis timeout (60s) → `'timeout'`, metrics 0.
- Diagnosis validation gate rejects all retries → `'schema_failed'`, metrics 0 except `evidence_quality = null` (not 0, since there's nothing to evaluate).
- Claude rate-limit → exponential backoff inside the shot; **fail the run loudly** after 3 retries (do not silently downscore). The PR/run is marked `status='failed'` — the gate does not run.
- Cassette miss in replay mode → fail the run loudly with a clear "regenerate cassettes" message.
- Network error → `'rate_limited'` status (catch-all for transient transport failures); the shot is retried up to 2x, then dropped. If only 2 valid shots remain, take the per-case mean of those 2 — report flags the case as `reduced_shots`.

**Cassette mode:**
- `cassette.py` wraps `anthropic.AsyncAnthropic.messages.create` via the SDK's HTTP transport hook.
- Record mode: live calls flow through; responses are written to `evals/cassettes/<prompt_version>/<model_id>/<case_id>__shot<i>.json`.
- Replay mode: matched on hash. Miss → loud error. No fallback to live.
- Invalidation: change `prompt_version` (in `diagnosis/prompts/v1.md` header) or `model_id` → cassettes for that combo become stale; `make evals` in replay mode fails until re-recorded.
- Cassette regeneration is a deliberate PR (`make evals-record` locally → commit the new cassettes → PR).

---

## 4. Scoring

Four metrics per case, all in `[0, 1]`. Pure functions over `(Diagnosis, GroundTruth, IncidentContext)`.

```python
# scoring.py

def score_category(d: Diagnosis, gt: GroundTruth) -> float:
    return 1.0 if d.likely_category in gt.acceptable_categories else 0.0

def score_hypothesis(d: Diagnosis, gt: GroundTruth, embed: EmbeddingProvider) -> float:
    # cosine sim of hypothesis vs root_cause embeddings — continuous, NOT thresholded
    # (a drift from 0.82 → 0.71 is meaningful; binary pass/fail hides it)
    sim = cosine(embed(d.hypothesis), embed(gt.root_cause))
    return max(0.0, sim)   # clamp; cosine can be slightly negative

def score_action_coverage(d: Diagnosis, gt: GroundTruth, embed: EmbeddingProvider) -> float:
    # for each correct_action: max cosine sim against any suggested_action.description
    # final = mean across correct_actions
    if not gt.correct_actions:
        return 1.0
    action_embeds = [embed(a.description) for a in d.suggested_actions]
    if not action_embeds:
        return 0.0
    return mean(
        max(cosine(embed(ca), ae) for ae in action_embeds)
        for ca in gt.correct_actions
    )

def score_evidence_quality(d: Diagnosis, ctx: IncidentContext) -> float:
    # fraction of EvidenceRef.id values that resolve to a context item
    # (the strict ID-resolution gate; the existing production hallucination metric
    # handles the "cited but unsupported" case via confidence capping)
    valid_ids = collect_stable_ids(ctx)   # deploy:*, similar:*, log:*, etc.
    if not d.evidence:
        return 0.0
    resolved = sum(1 for e in d.evidence if e.id in valid_ids)
    return resolved / len(d.evidence)
```

**Aggregation:**
- **Per-case** = mean across shots (not median — masks tail behavior). Per-case stddev reported separately as stability signal.
- **Per-run** = mean across cases. Cases with `status != ok` score 0 in the mean (drags the number down, which is correct).

**Headline derived numbers** (in the run report, surfaced to README):
- `pass_rate_strict` — fraction of cases where all 4 metrics ≥ thresholds (category=1.0, hypothesis≥0.7, action≥0.6, evidence≥0.8).
- `mean_confidence_when_correct` — calibration sanity check.
- `hallucinated_evidence_rate` — count of cases where any `EvidenceRef.id` didn't resolve.
- `mean_stability` — mean of per-case shot-stddevs across all metrics; tracks corpus quality over time.

---

## 5. Regression gate — paired bootstrap, not flat threshold

Naive `>5% drop` gates produce constant false positives at n=10 cases × 4 metrics due to LLM noise. Replace with paired-bootstrap (per Cameron Wolfe's stats-for-LLM-evals analysis; matches Inspect AI's bootstrap_std primitive):

```python
# stats.py

def regression_for_metric(
    current_per_case: list[float],
    baseline_per_case: list[float],
    n_resamples: int = 10_000,
    practical_floor: float = 0.05,
) -> RegressionResult:
    """
    Paired difference d_i = current[i] - baseline[i].
    Regression iff CI_95 excludes 0 AND mean(d_i) < -practical_floor.

    Both conditions required:
      - CI gate catches statistical signal
      - 5% floor prevents 'statistically significant tiny drops' from blocking merges
    """
    d = [c - b for c, b in zip(current_per_case, baseline_per_case)]
    mean_d = mean(d)
    ci_lo, ci_hi = bootstrap_ci(d, n_resamples, alpha=0.05)
    is_regression = (ci_hi < 0) and (mean_d < -practical_floor)
    return RegressionResult(mean_d=mean_d, ci=(ci_lo, ci_hi), is_regression=is_regression)
```

**Special cases:**
- `hallucinated_evidence_rate` is **inverted** (lower is better) — gate fires if `mean(d) > +practical_floor` AND `ci_lo > 0`.
- Corpus-size mismatch (current run size ≠ baseline size) → gate **skipped** with a loud banner ("baseline is stale, regenerate"). CI passes but report flags it.
- Corpus-version mismatch → same: skipped + banner. Forces an intentional baseline refresh.
- Baseline absent (first run) → gate skipped; current run becomes seed for the first baseline (committed in a follow-up PR).

**Baseline file** — committed JSON tied to schema version, not "latest on main":

```json
// evals/baselines/v1.json
{
  "git_sha": "891228c",
  "git_tag": "evals-baseline-v1",
  "captured_at": "2026-05-20T...",
  "corpus_version": 1,
  "corpus_size": 10,
  "shots_per_case": 5,                // weekly baselines run at N=5 for tighter CI
  "model_id": "claude-sonnet-4-5-20250929",
  "prompt_version": "v1",
  "embedding_model_id": "BAAI/bge-small-en-v1.5",
  "metrics": {
    "category_match": 0.90,
    "hypothesis_cosine": 0.78,
    "action_coverage": 0.71,
    "evidence_quality": 0.94,
    "pass_rate_strict": 0.70,
    "hallucinated_evidence_rate": 0.05,
    "mean_stability": 0.08
  },
  "per_case": [
    {"case_id": "cloudflare-2022-06-21-bgp", "metrics": { ... }},
    ...
  ]
}
```

`per_case` is required (not just aggregates) — the paired bootstrap needs the per-case scores from the baseline.

**Baseline update flow** (manual, deliberate):
```
make evals-baseline    # runs full corpus at N=5, writes evals/baselines/v1.json
# you stage, review, commit, PR with a justification (prompt change, model bump, corpus refresh)
```

No auto-promotion. Every baseline change is a reviewable diff with explicit rationale in the PR description.

---

## 6. Persistence — Postgres tables for trends

Two tables added via alembic migration. Existing tables live in the default (public) schema — eval tables follow the same convention; no custom schema introduced (matches `sentinel/migrations/versions/0001_initial.py` and subsequent migrations).

The eval runner uses a **separate DATABASE_URL** (`sentinel_evals_test` for CI smoke; `sentinel_evals` for nightly/baseline) so eval-generated `incidents`/`diagnoses`/`outbox` rows are isolated from any other DB. `eval_runs` + `eval_case_results` live in the *same* DB as the eval-generated incidents — they accumulate across runs because the runner only truncates the incident-side tables between cases, not the eval tables.

The FK from `eval_case_results` to `incidents` is intentionally **absent**: between cases the runner truncates `incidents`, which would cascade-delete every prior `eval_case_results` row if a FK existed. `incident_id` is stored as a bare UUID for forensic correlation only.

```sql
-- One row per eval run
CREATE TABLE eval_runs (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ,
    status                TEXT NOT NULL CHECK (status IN ('running','ok','failed','partial')),
    trigger               TEXT NOT NULL CHECK (trigger IN ('local','ci-smoke','ci-nightly','baseline','manual')),

    -- Reproducibility metadata (canonical set per industry convention)
    git_sha               TEXT NOT NULL,
    prompt_version        TEXT NOT NULL,
    model_id              TEXT NOT NULL,           -- e.g. claude-sonnet-4-5-20250929
    embedding_model_id    TEXT NOT NULL,           -- cosine scorer silently depends on this
    corpus_version        INT  NOT NULL,
    corpus_size           INT  NOT NULL,
    shots_per_case        INT  NOT NULL,
    fetcher_fixture_hash  TEXT NOT NULL,           -- sha256 of context_seed blocks loaded

    -- Aggregated results
    metrics               JSONB NOT NULL DEFAULT '{}'::jsonb,  -- MetricSet (means)
    metrics_stability     JSONB NOT NULL DEFAULT '{}'::jsonb,  -- stddev across shots
    regression_baseline_sha TEXT,                   -- git_sha of baseline compared against
    regression_passed     BOOLEAN,                  -- null while running
    regression_detail     JSONB,                    -- per-metric CI + verdict

    extra                 JSONB NOT NULL DEFAULT '{}'::jsonb   -- MLflow-style escape hatch
);
CREATE INDEX idx_eval_runs_started_at ON eval_runs (started_at DESC);
CREATE INDEX idx_eval_runs_status     ON eval_runs (status);

-- One row per (run, case, shot) — 10 cases × 3 shots = 30 rows per smoke run
CREATE TABLE eval_case_results (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES eval_runs(id) ON DELETE CASCADE,
    case_id         TEXT NOT NULL,                  -- e.g. "cloudflare-2022-06-21-bgp"
    shot_index      INT  NOT NULL,                  -- 0..shots_per_case-1
    case_status     TEXT NOT NULL CHECK (case_status IN
                        ('ok','timeout','ingest_failed','schema_failed','rate_limited')),
    metrics         JSONB NOT NULL DEFAULT '{}'::jsonb,   -- per-shot MetricSet

    -- Per Inspect AI's headline lesson: without raw outputs you cannot debug
    -- regressions or build new scorers retroactively.
    raw_response    JSONB,                          -- full Claude response (null if pre-LLM failure)
    diagnosis       JSONB,                          -- parsed, schema-validated Diagnosis
    incident_id     UUID,                           -- forensic ref; intentionally NO FK
    token_usage     JSONB,                          -- input/output/cache tokens + cost
    latency_ms      INT,
    error_detail    TEXT,                           -- populated on non-ok statuses

    UNIQUE (run_id, case_id, shot_index)
);
CREATE INDEX idx_eval_case_results_run_id  ON eval_case_results (run_id);
CREATE INDEX idx_eval_case_results_case_id ON eval_case_results (case_id, run_id);
```

**Repository**: `sentinel/persistence/repositories/eval_runs.py` — only caller of these tables. Methods: `start_run(metadata)`, `persist_shot(run_id, shot_result)`, `finalize_run(run_id, status, metrics, regression_result)`, `get_run(run_id)`, `get_latest_ok_run()`, `list_recent(limit)`.

**Companion change in PR 1**: add `DiagnosisRepository.get_by_incident_id(incident_id: UUID) -> Diagnosis | None` to the Protocol in `sentinel/persistence/repositories.py` plus its concrete implementation. The runner's diagnosis-await loop depends on this method; today the repository only exposes `save_with_outbox()` (verified — see `repositories.py:948-956`). This is a one-method addition, naturally co-located with the migration PR since both are persistence-layer changes.

**JSON artifacts still written** (`evals/results/<run_id>.{json,md}`, gitignored) for:
- CI artifact upload / PR comment attachment without requiring DB access from the workflow runner
- Easy local diffing without SQL

The DB row's `metrics` JSONB and the JSON file's metrics are written in the same code path — guaranteed identical.

**README updates**: `make readme-numbers` reads from the DB (latest `status='ok'` row tagged `trigger='baseline'`), patches the README between `<!-- evals:start -->` and `<!-- evals:end -->` markers. Numbers never hand-edited.

---

## 7. CI wiring

Three workflows, three cadences:

| Workflow | Trigger | Calls | Cost/run | Gate |
|---|---|---|---|---|
| `ci.yml` → `evals-smoke` | every PR + push to main | **cassette replay** (5 cases × 3 shots) | $0 | passes baseline gate or fails PR |
| `nightly-evals.yml` | 06:00 UTC daily + manual | **live** (10 cases × 3 shots) | ~$0.45 | reports drift; no merge gate (informational) |
| `weekly-baseline.yml` (new) | Sunday 06:00 UTC + manual | **live** (10 cases × 5 shots) | ~$1.50 | opens a PR proposing a new baseline if non-regression |

**PR smoke specifics:**
- No Anthropic API key needed in PR CI (cassettes serve responses).
- Compose stack required (full webhook path runs). Reuses the `test-integration` job's compose setup pattern.
- Cassette miss → loud failure with "run `make evals-record` and commit the cassettes" guidance.
- The 5-case smoke subset is a deterministic slice: first 5 cases by sorted `id`. Documented in `evals/corpus/README.md`.

**Nightly specifics:**
- Live API calls; secret already in workflow.
- Uploads `evals/results/<run_id>.json` + `<run_id>.md` as artifacts.
- Posts the markdown report as a workflow summary.
- On regression detection: opens (or updates) a tracked GitHub issue `evals-drift: <metric>` so drift doesn't silently accumulate.

**Weekly baseline specifics:**
- N=5 shots for tighter bootstrap CI.
- If `regression_passed = true`, opens a PR updating `evals/baselines/v1.json` with the new numbers. Human still approves the merge — no auto-promotion.

---

## 8. Implementation sequencing

**PR 1 — Schema + repository plumbing** (`feat: eval_runs + eval_case_results migration + diagnosis read path`):
- Alembic upgrade + downgrade for `eval_runs` + `eval_case_results`
- `EvalRunsRepository` with all methods stubbed + unit tests
- `DiagnosisRepository.get_by_incident_id()` added to Protocol + concrete impl + unit test (needed by runner; cleaner here than buried in PR 3)
- No runner, no scoring, no CI changes
- Risk surface: schema + one repository method. Easy to revert.

**PR 2 — Scoring + stats** (`feat: eval scoring + bootstrap regression gate`):
- `scoring.py` + `stats.py` as pure functions
- Comprehensive unit tests (synthetic Diagnosis/GroundTruth pairs)
- No runner, no CI changes yet

**PR 3 — Corpus + runner** (`feat: eval runner + 10-case corpus`):
- `corpus_loader.py`, `fetcher_override.py`, `runner.py`, `cassette.py`, `report.py`, `cli.py`
- All 10 corpus YAMLs (drafted by Claude from public postmortems, reviewed by operator)
- `make evals`, `make evals-smoke`, `make evals-record`, `make evals-baseline`, `make readme-numbers` wired up
- Cassettes recorded + committed
- README updated with the first real numbers

**PR 4 — CI integration** (`ci: wire eval smoke + nightly + weekly baseline`):
- `ci.yml`: smoke job runs cassettes against compose stack
- `nightly-evals.yml`: live calls, posts report, drift issue management
- `weekly-baseline.yml`: new workflow for proposing baseline updates

Each PR has its own brainstorm → plan → review → commit cycle per the existing project convention. PR 1 ships first (schema risk isolated); subsequent PRs depend on its merge.

---

## 9. ADR

A single ADR lands with PR 3: `docs/adr/0007-eval-harness-design.md`. Captures:
- Why end-to-end webhook (not direct agent call): exercises production plumbing
- Why mean + variance (not median): tail-sensitivity per Anthropic model-card practice
- Why cassettes for PR + live for nightly: cost/determinism split per industry convention
- Why paired-bootstrap (not flat threshold): noise floor with n=10 makes flat gates unworkable
- Why per-shot persistence with raw outputs: Inspect AI's headline lesson — debugging needs raw data
- Why no eval-judge LLM (no Ragas/TruLens): existing evidence-ID gate is stricter than industry default; defer the semantic faithfulness check until justified

---

## 10. Open questions deferred to implementation

These don't block the design but want a quick answer when writing the plan:

1. **Cassette commit size** — 30 cassettes × ~10KB raw response ≈ 300KB. Acceptable in-tree. Re-evaluate at corpus=30 (~1MB) — if it bloats, move to Git-LFS.
2. **Embedding model bump policy** — bumping `embedding_model_id` invalidates baseline. Document in `docs/adr/0007` that this requires a baseline refresh PR, same as a prompt change.
3. **Temperature/seed for the diagnosis agent** — verified the current `AnthropicClient.messages.stream()` call at `sentinel/diagnosis/llm_client.py:106-112` does **not** pass `temperature`, so it inherits the SDK default (model-dependent, typically 1.0 for Sonnet 4-5). Evals deliberately do **not** override this — the harness's job is to measure production behavior, not a counterfactual deterministic variant. The N=3 mean+stddev design already handles the resulting variance; if stability metrics turn out unworkable in practice, the right fix is a production-side decision to pin temperature (which would require an ADR + baseline refresh), not an eval-side override.

## 11. Code-vs-spec verification gaps closed in this design

The spec was reviewed against the actual codebase (verification report 2026-05-20). Adjustments folded in above:

- **§1 fetcher override**: clarified that `EnrichmentDeps.fetchers` is immutable post-`lifespan()` construction; eval mode uses an explicit settings-flag branch in `lifespan()` rather than a FastAPI DI override (which the current wiring doesn't support).
- **§3 diagnosis polling**: `DiagnosisRepository` currently only has `save_with_outbox()` — added `get_by_incident_id()` to PR 1's scope explicitly. No new HTTP endpoint needed; the runner shares the DB connection.
- **§3 per-run schema dropped**: original spec proposed `sentinel_evals_<run_id>` schema-per-run, but existing alembic migrations all target the default (public) schema and don't support schema parameterisation. Replaced with **separate database** (`sentinel_evals_test` / `sentinel_evals`) — same alembic, same migrations, isolated data.
- **§6 schema naming**: corrected references from "production schema (sentinel)" to default (public) schema, matching existing migrations.
- **§7 cassette transport**: clarified mechanism — inject custom `httpx.AsyncClient` via `anthropic.AsyncAnthropic(http_client=...)` (SDK ≥0.39 supports this; pyproject pins `anthropic>=0.39,<0.40`).
- **§3 HMAC signing**: confirmed `sentinel.integrations.base.compute_hmac_sha256` is the correct helper for the runner to sign synthetic payloads against the per-adapter test secret.

Confirmed unchanged: Diagnosis schema fields (§4 scoring), IncidentContext stable IDs (§4 evidence-quality), webhook response shape (§3), prompt versioning via `PromptBundle.version` (§7 cassette key), EmbeddingProvider Protocol (§1 reuse).
