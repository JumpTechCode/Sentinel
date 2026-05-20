# Eval Harness PR 3b — Runner + Report + CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Land the orchestration layer — settings + lifespan branch for eval mode, `runner.py` (load corpus → fire webhook → poll diagnosis → score → persist per shot), `report.py` (JSON + Markdown), `cli.py` (`python -m sentinel.evals` entrypoint), Makefile targets, README markers. Cassette-replay mode only — record mode lands in PR 3c when we record real cassettes against drafted YAMLs.

**Architecture (single-process):** The runner CLI builds the FastAPI app in-process via `build_app()`, drives requests via `httpx.AsyncClient(transport=ASGITransport(app))`, and polls `PostgresDiagnosisRepository.get_by_incident_id` directly on the shared DB connection. The shared `ActiveCaseRegistry` is a module singleton so the corpus fetchers (wired into `EnrichmentDeps` by lifespan when `eval_mode=true`) see the same active case the runner just set. Cassette wraps the AnthropicClient via a custom `httpx.AsyncClient(transport=CassetteTransport(...))` injected at `AnthropicClient(...)` construction in lifespan.

**Operational caveat:** the runner shares the compose data tier (Postgres + Kafka + Redis) but starts its own FastAPI app in-process. The compose `app` container must be stopped first (or use a different consumer group) to avoid Kafka partition contention. Documented in the CLI's `run` subcommand help.

**Tech Stack:** Python 3.12, FastAPI, httpx (ASGITransport), Pydantic v2, asyncio.

**Spec reference:** `plans/2026-05-20-eval-harness-design.md` §1, §3, §6, §7. PR 3a (#44) shipped the harness pieces; this PR wires them together; PR 3c lands the 10 corpus YAMLs and recorded cassettes; PR 4 ships CI integration.

**Dependency:** stacks on PR 3a's branch (`feat/eval-harness-pr3a-harness`). Uses CorpusCase, ActiveCaseRegistry, corpus_fetchers, CassetteTransport, CassetteContext.

---

## Out of scope (deferred)

- 10 hand-drafted corpus YAMLs + recorded cassettes → **PR 3c**
- CI workflows (smoke / nightly / weekly baseline) + cassette commit workflow → **PR 4**
- `make evals-record` (live API mode) — the helper lands here but with a "not implemented; use 3c" stub that prints how to record manually until 3c
- README content update (the markers + `make readme-numbers` target land here; the actual numbers come from PR 3c's first real run)

---

## Architectural decisions baked into this plan

1. **Single-process runner.** Builds FastAPI in-process, drives via `ASGITransport`. Pros: shared ActiveCaseRegistry, no consumer-group contention worry. Cons: compose `app` container must be stopped first (documented).
2. **ActiveCaseRegistry moves to its own module** (`sentinel/evals/registry.py`) so both `fetcher_override` and the runner import the same singleton without a circular dep.
3. **Lifespan reads `settings.eval_mode`.** When true: swaps `default_fetchers()` for `corpus_fetchers(registry)`; wraps `AnthropicClient` with a `CassetteTransport`-backed `httpx.AsyncClient` when `settings.eval_cassette_dir` is set.
4. **Runner polling cadence: 250ms; timeout: 60s.** Matches design §3.
5. **Per-shot persistence via `PostgresEvalRunRepository.persist_shot`** (from PR 1). The runner needs Postgres access independent of the in-process FastAPI app.
6. **Report writes both JSON and Markdown.** JSON is machine-readable (CI artifacts); Markdown is for README + GitHub PR comments.
7. **No baseline regression gate wiring in this PR** — the gate functions exist (PR 2's `regression_for_metric`), but wiring them to a real baseline file lands when PR 3c first produces a baseline. PR 3b's runner returns the metrics; the gate is a separate CLI subcommand `compare-to-baseline` that lands as a stub.

---

## File Structure

**New:**
- Create: `sentinel/evals/registry.py` (ActiveCaseRegistry singleton)
- Create: `sentinel/evals/runner.py`
- Create: `sentinel/evals/report.py`
- Create: `sentinel/evals/cli.py`
- Create: `sentinel/evals/__main__.py` (so `python -m sentinel.evals` works)
- Create: `tests/integration/evals/__init__.py` + `tests/integration/evals/conftest.py`
- Create: `tests/integration/evals/test_runner_e2e.py` (one integration test against a fixture YAML + hand-written cassette)
- Create: `tests/unit/evals/test_report.py` (JSON + MD shape tests)
- Create: `tests/unit/evals/test_cli.py` (argparse + dispatch tests)
- Create: `tests/integration/evals/fixtures/cassettes/` directory (one hand-written cassette for the e2e test)

**Modified:**
- Modify: `sentinel/evals/fetcher_override.py` (remove ActiveCaseRegistry — re-export from registry for backwards compat)
- Modify: `sentinel/evals/__init__.py` (export new types from registry/runner/report/cli)
- Modify: `sentinel/config/settings.py` (add 3 fields: `eval_mode`, `eval_corpus_dir`, `eval_cassette_dir`)
- Modify: `sentinel/api/app.py` (lifespan branch on `settings.eval_mode`)
- Modify: `sentinel/diagnosis/llm_client.py` (accept `http_client: httpx.AsyncClient | None = None` to pass through to AsyncAnthropic)
- Modify: `Makefile` (wire `evals`, `evals-smoke`, `evals-record`, `evals-baseline`, `readme-numbers`)
- Modify: `README.md` (add `<!-- evals:start -->` / `<!-- evals:end -->` markers — content stays empty until PR 3c)

---

## Task 0: Branch + verify PR 3a tip

- [ ] Run `git checkout feat/eval-harness-pr3a-harness && git checkout -b feat/eval-harness-pr3b-runner && git log --oneline -3` — verify on the new branch with PR 3a tip as parent.

---

## Task 1: Extract `ActiveCaseRegistry` into its own module

**Files:**
- Create: `sentinel/evals/registry.py`
- Modify: `sentinel/evals/fetcher_override.py` (re-export from registry)
- Modify: `sentinel/evals/__init__.py` (still exports `ActiveCaseRegistry`)
- Modify: `tests/unit/evals/test_fetcher_override.py` (imports stay valid via re-export)

**Why:** the runner needs to import `ActiveCaseRegistry` without dragging in the 6 fetcher classes. Putting it in its own module avoids a circular import once `runner.py` lands.

### Steps

1. Create `sentinel/evals/registry.py` with the `ActiveCaseRegistry` class verbatim from `fetcher_override.py` (no changes to behavior). Add a module-level singleton instance: `REGISTRY = ActiveCaseRegistry()`. Both `fetcher_override` and `runner` import `REGISTRY` from here.
2. In `fetcher_override.py`: remove the `class ActiveCaseRegistry` block; replace with `from sentinel.evals.registry import REGISTRY, ActiveCaseRegistry`. The `corpus_fetchers(registry: ActiveCaseRegistry)` signature is unchanged; defaults to `REGISTRY` if not supplied.
3. Update `sentinel/evals/__init__.py`: still exports `ActiveCaseRegistry`; now also exports `REGISTRY`.
4. Run the existing fetcher_override tests — they should still pass without modification (the import path through `sentinel.evals` works either way).
5. Commit: `refactor(evals): extract ActiveCaseRegistry into registry.py + module singleton`.

**Test gate:** `pytest tests/unit/evals/test_fetcher_override.py` — all 8 tests still pass.

---

## Task 2: Settings + AnthropicClient `http_client` passthrough

**Files:**
- Modify: `sentinel/config/settings.py`
- Modify: `sentinel/diagnosis/llm_client.py`
- Create: `tests/unit/config/test_settings_eval_fields.py`

### Settings additions (`sentinel/config/settings.py`)

Add three fields to the existing `Settings` class:

```python
eval_mode: bool = False
eval_corpus_dir: Path | None = None  # required when eval_mode=True; checked in lifespan
eval_cassette_dir: Path | None = None  # if set, AnthropicClient uses cassette transport
```

Add an `@model_validator(mode="after")` that raises if `eval_mode=True` and `eval_corpus_dir is None` (fail-loud at startup, not 3 min into a run).

### AnthropicClient (`sentinel/diagnosis/llm_client.py`)

Today: `AsyncAnthropic(api_key=api_key.get_secret_value())`. Add a new optional param to `AnthropicClient.__init__`:

```python
http_client: httpx.AsyncClient | None = None,
```

If supplied, pass through: `AsyncAnthropic(api_key=..., http_client=http_client)`. The Anthropic SDK ≥0.39 supports this — verified earlier in the design pass.

### Tests

`tests/unit/config/test_settings_eval_fields.py` (4 tests):
- `test_default_eval_mode_is_false` — Settings() instantiates with eval_mode=False
- `test_eval_mode_true_without_corpus_dir_raises` — validation error
- `test_eval_mode_true_with_corpus_dir_succeeds`
- `test_eval_cassette_dir_independent_of_corpus_dir` — can set one without the other

Add an `AnthropicClient.http_client` passthrough test to the existing `tests/unit/diagnosis/test_llm_client.py` (or wherever the LLM client unit tests live; grep for it):
- `test_http_client_passthrough` — construct with a fake httpx.AsyncClient, verify it's used (assert via a mock on AsyncAnthropic).

**Commit:** `feat(config,diagnosis): eval-mode settings + AnthropicClient http_client passthrough`.

---

## Task 3: Lifespan branch for eval mode

**Files:**
- Modify: `sentinel/api/app.py` (lifespan function)

### Behavior

When `settings.eval_mode` is True:
1. **Swap fetchers**: instead of `fetchers = default_fetchers()`, use `fetchers = corpus_fetchers(REGISTRY)` (imported from `sentinel.evals`).
2. **Wire cassette into AnthropicClient** (only if `settings.eval_cassette_dir` is set):
   ```python
   from sentinel.evals.cassette import CassetteTransport
   import httpx
   transport = CassetteTransport(mode="replay", cassette_dir=settings.eval_cassette_dir)
   http_client = httpx.AsyncClient(transport=transport)
   llm_client = AnthropicClient(..., http_client=http_client)
   app.state.cassette_transport = transport  # so runner can call set_context() before each request
   ```
3. Everything else stays the same — Kafka consumers, drainer, memory consumer, etc. all start as usual.

### Notes for the implementer

- The cassette transport must be reachable from the runner so the runner can call `set_context()` before each diagnosis. The cleanest path is via `app.state.cassette_transport` — the runner reads `app.state` directly since it built the app.
- The fetcher_swap is unconditional on `eval_mode` (no separate flag) — eval mode without corpus fetchers makes no sense.
- The cassette wiring is gated on `eval_cassette_dir` being set so that PR 3b can also support a future "live eval" mode (e.g., recording) without changing lifespan.
- If `eval_mode` is True but `settings.diagnosis_consumer_enabled` is False, log a warning and skip cassette wiring — there's no AnthropicClient to wrap.

### Tests

Don't add lifespan tests in this task — the next task's integration test exercises the full path. mypy strict + manual sanity (boot the app with `SENTINEL_EVAL_MODE=true SENTINEL_EVAL_CORPUS_DIR=tests/unit/evals/fixtures` and verify it starts without error) is enough.

**Commit:** `feat(api): lifespan branch for eval_mode — corpus fetchers + cassette-wrapped AnthropicClient`.

---

## Task 4: `runner.py` — single-case + multi-shot orchestration

**Files:**
- Create: `sentinel/evals/runner.py`
- Create: `tests/unit/evals/test_runner_unit.py` (pure-function unit tests; integration test lands in Task 7)

### Responsibilities

`runner.py` exposes one public coroutine:

```python
async def run_corpus(
    *,
    cases: list[CorpusCase],
    shots_per_case: int,
    runner_deps: RunnerDeps,
) -> RunResult:
    ...
```

Where `RunnerDeps` is a frozen dataclass carrying:
- `app: FastAPI` (already built and lifespan-started by the CLI)
- `client: httpx.AsyncClient` (already wrapping the app via ASGITransport)
- `diagnosis_repo: DiagnosisRepository` (for polling)
- `eval_run_repo: EvalRunRepository` (for persisting shots + finalizing)
- `embed: EmbeddingProvider` (for scoring)
- `cassette_transport: CassetteTransport | None` (set per-request when present)
- `run_id: UUID` (already created via `eval_run_repo.start_run(...)` before the runner is invoked)
- `prompt_version: str`, `model_id: str` (for cassette key derivation)
- `truncate_between_cases: Callable[[], Awaitable[None]]` (the CLI supplies a closure that TRUNCATEs incidents/diagnoses/outbox)

### Per-case-per-shot flow (from design §3)

```python
for case in cases:
    for shot_index in range(shots_per_case):
        await runner_deps.truncate_between_cases()
        REGISTRY.set(case)
        if runner_deps.cassette_transport is not None:
            runner_deps.cassette_transport.set_context(CassetteContext(
                prompt_version=runner_deps.prompt_version,
                model_id=runner_deps.model_id,
                case_id=case.id,
                shot_index=shot_index,
            ))
        incident_id, case_status, raw_diagnosis = await _fire_and_poll(case, runner_deps)
        if case_status == "ok":
            metrics = await _score(raw_diagnosis, case, runner_deps.embed)
        else:
            metrics = MetricSet(category_match=0.0, hypothesis_cosine=0.0,
                                action_coverage=0.0, evidence_quality=None)
        shot = EvalCaseResultRecord(
            run_id=runner_deps.run_id,
            case_id=case.id,
            shot_index=shot_index,
            case_status=case_status,
            metrics={"category_match": metrics.category_match, ...},
            raw_response=...,  # serialized from the diagnosis row
            diagnosis=...,
            incident_id=incident_id,
            incident_fingerprint=...,  # extracted from incident row
            incident_title=case.alert.title,
            incident_severity=case.alert.severity,
            token_usage=...,
            latency_ms=...,
            error_detail=None if case_status == "ok" else str(...),
        )
        await runner_deps.eval_run_repo.persist_shot(shot)

# After the loop:
return RunResult(
    run_id=runner_deps.run_id,
    per_case_metrics={...},  # case_id -> MetricSet
    aggregate_metrics={...},  # MetricSet means across cases
    stability={...},  # per-case per-metric stddev across shots
)
```

### `_fire_and_poll(case, runner_deps)` helper

1. Build the HMAC signature for the synthetic payload using the per-source test secret resolved from settings.
2. `await runner_deps.client.post("/webhooks/{case.alert.source}", json=case.alert.raw_payload, headers={...})`
3. Assert 202; extract `incident_id` from the response body.
4. Poll `runner_deps.diagnosis_repo.get_by_incident_id(incident_id)` every 250ms, up to 60s.
5. Failure modes (design §3):
   - Webhook 5xx → return `(None, "ingest_failed", None)`
   - Diagnosis timeout → return `(incident_id, "timeout", None)`
   - Diagnosis present → return `(incident_id, "ok", diagnosis_row)`
6. Cassette miss raises `CassetteMiss` (from PR 3a) — let it propagate; the CLI catches it and marks the whole run failed.

### `_score(diagnosis, case, embed)` helper

Calls the 4 scoring functions from PR 2 (`score_category`, `score_hypothesis`, `score_action_coverage`, `score_evidence_quality`). Needs the IncidentContext for evidence_quality — fetch via `incident_repo.get_enrichment_context(incident_id)` (verify this method exists; if not, use `incident_repo.get(incident_id)` and pull from there).

### Tests for Task 4 (`test_runner_unit.py`)

Tests focus on the orchestration helpers — not the end-to-end (that's Task 7):

1. `test_failure_mode_ingest_failed` — webhook returns 500 → case_status = "ingest_failed", metrics 0
2. `test_failure_mode_timeout` — diagnosis poll times out → case_status = "timeout", metrics 0
3. `test_per_case_stability_computation` — given fake per-shot metrics, compute mean + stddev correctly
4. `test_run_result_aggregation` — given fake per-case metrics, compute run-level mean correctly

Use a `FakeClient` + `FakeDiagnosisRepo` + `FakeEvalRunRepo` + the `FakeEmbedder` already in PR 2 tests.

**Commit:** `feat(evals): runner.py — single-case + multi-shot orchestration`.

---

## Task 5: `report.py` — JSON + Markdown writer

**Files:**
- Create: `sentinel/evals/report.py`
- Create: `tests/unit/evals/test_report.py`

### Responsibilities

```python
def write_report(
    *,
    run_result: RunResult,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Returns (json_path, md_path). Both files keyed by run_id."""
```

**JSON shape** (per design §6 — the eval_runs DB `metrics` JSONB column gets the same blob):

```json
{
  "run_id": "...",
  "metrics": {
    "category_match": 0.9,
    "hypothesis_cosine": 0.78,
    "action_coverage": 0.71,
    "evidence_quality": 0.94,
    "pass_rate_strict": 0.7,
    "mean_confidence_when_correct": 0.82,
    "hallucinated_evidence_rate": 0.05,
    "mean_stability": 0.08
  },
  "per_case": [
    {"case_id": "cloudflare-bgp", "metrics": {...}, "stability": {...}}
  ]
}
```

**Markdown shape** — operator-readable summary:

```markdown
# Sentinel Eval Results — <run_id>

**Status:** ok | failed | partial
**Cases:** N
**Shots/case:** N

## Aggregate Metrics

| Metric | Value |
|---|---|
| category_match | 0.90 |
| ...

## Per-case

| Case | category | hypothesis | actions | evidence | stability |
|---|---|---|---|---|---|
| cloudflare-bgp | 1.0 | 0.85 | 0.72 | 1.0 | 0.04 |
...

## Headline numbers

- pass_rate_strict: 70%
- mean_confidence_when_correct: 0.82
- hallucinated_evidence_rate: 5%
```

### Tests (6 tests)

1. `test_writes_both_files`
2. `test_json_round_trips_run_result`
3. `test_markdown_includes_aggregate_table`
4. `test_markdown_includes_per_case_table`
5. `test_handles_zero_cases_gracefully` — no crash; empty tables, headline numbers all 0/null
6. `test_headline_numbers_computed_correctly` — given fake per-case data, verify pass_rate_strict and the others

**Commit:** `feat(evals): report.py — JSON + Markdown writer`.

---

## Task 6: `cli.py` + `__main__.py` + Makefile + README markers

**Files:**
- Create: `sentinel/evals/cli.py`
- Create: `sentinel/evals/__main__.py` (one line: `from sentinel.evals.cli import main; main()`)
- Modify: `Makefile`
- Modify: `README.md`
- Create: `tests/unit/evals/test_cli.py`

### CLI subcommands (argparse)

```
python -m sentinel.evals run [--corpus DIR] [--shots N] [--cassette-dir DIR]
python -m sentinel.evals record [--corpus DIR] [--shots N] [--cassette-dir DIR]
   # Stub in PR 3b — prints "use manual record flow until PR 3c"; exit 1
python -m sentinel.evals baseline [--corpus DIR] [--shots 5]
   # Stub — same as record, defers to PR 3c
python -m sentinel.evals readme [--latest-run-id UUID]
   # Patches README.md between <!-- evals:start --> / <!-- evals:end --> markers
   # from the EvalRunRepository row matching the run_id (defaults to latest ok run)
python -m sentinel.evals compare-to-baseline --run-id UUID --baseline FILE
   # Stub — wired in PR 3c when first baseline exists
```

`run` is the only fully-implemented subcommand in PR 3b. It:
1. Loads settings; verifies `eval_mode=True`, `eval_corpus_dir` set.
2. Builds the FastAPI app via `build_app()`; enters its lifespan via the standard FastAPI test pattern (`async with LifespanManager(app)`).
3. Constructs an `httpx.AsyncClient(transport=ASGITransport(app=app))`.
4. Constructs `RunnerDeps` from app.state (cassette_transport, diagnosis_repo, etc.) + the corpus loaded from `eval_corpus_dir`.
5. Calls `await eval_run_repo.start_run(...)` to mint a run_id.
6. Calls `await run_corpus(cases=..., shots_per_case=args.shots, runner_deps=...)`.
7. Calls `await eval_run_repo.finalize_run(...)` with the aggregated metrics.
8. Calls `write_report(...)` to emit `evals/results/<run_id>.{json,md}`.
9. Exits 0 on success, 1 on failure (CassetteMiss, runtime error).

### Makefile additions

```make
evals:
	.venv/bin/python -m sentinel.evals run --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes

evals-smoke:
	.venv/bin/python -m sentinel.evals run --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes --smoke

evals-record:
	.venv/bin/python -m sentinel.evals record --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes

evals-baseline:
	.venv/bin/python -m sentinel.evals baseline --corpus sentinel/evals/corpus --shots 5

readme-numbers:
	.venv/bin/python -m sentinel.evals readme
```

(Replace the existing placeholder `evals` / `evals-smoke` targets that print "not implemented yet".)

The `--smoke` flag on `run` selects the first 5 cases by sorted id (design §7). Implement in `cli.py`.

### README markers

Append to `README.md` (the implementer should find a sensible location near the top, probably right after the project blurb):

```markdown
## Eval results

<!-- evals:start -->
*Pending PR 3c — first real corpus run lands the numbers here.*
<!-- evals:end -->
```

`make readme-numbers` (Task 6) replaces the content between the markers from the latest `eval_runs` row's metrics JSONB.

### Tests (`test_cli.py`, 4 tests)

1. `test_run_requires_eval_mode_true` — fails with clear error if SENTINEL_EVAL_MODE not set
2. `test_record_subcommand_is_a_stub`
3. `test_baseline_subcommand_is_a_stub`
4. `test_readme_patches_between_markers` — uses tmp_path README + fake EvalRunRecord

**Commit:** `feat(evals): cli.py + Makefile targets + README markers`.

---

## Task 7: Integration test — end-to-end run with cassette + fixture YAML

**Files:**
- Create: `tests/integration/evals/__init__.py`
- Create: `tests/integration/evals/conftest.py`
- Create: `tests/integration/evals/test_runner_e2e.py`
- Create: `tests/integration/evals/fixtures/corpus/sample-case.yaml`
- Create: `tests/integration/evals/fixtures/cassettes/<key>.json`

The integration test is the proof-of-life for the whole runner. One full end-to-end:

1. Load a single fixture corpus case (lift from PR 3a's `cloudflare-bgp.yaml` fixture; rename + adapt as needed).
2. Pre-write a cassette file with a known-good diagnosis response. The cassette key is derived from `(prompt_version="v1", model_id="...", case_id="...", shot_index=0)` — compute it once and use it as the filename.
3. Set `SENTINEL_EVAL_MODE=true`, `SENTINEL_EVAL_CORPUS_DIR=fixtures/corpus`, `SENTINEL_EVAL_CASSETTE_DIR=fixtures/cassettes`.
4. Invoke the runner via the CLI's `run` subcommand (or call `await run_corpus(...)` directly — your call).
5. Assert:
   - `eval_runs` table has one row with `status='ok'`
   - `eval_case_results` table has 1 row (1 case × 1 shot)
   - The persisted metrics are non-zero (specifically: `category_match=1.0` if the cassette diagnosis matches the ground_truth)
   - A `evals/results/<run_id>.json` file was written
   - A `evals/results/<run_id>.md` file was written

This test requires the full compose stack (Postgres + Kafka + Redis). The compose `app` container should be stopped before running this test (or run with a different consumer group via env).

**The hand-written cassette is the tricky part.** The actual Anthropic API response shape is opaque — find an existing recorded response in the repo (e.g., from diagnosis-agent integration tests at `tests/integration/diagnosis/`) and adapt. If none exists, hand-craft a minimal `messages.stream` response that the AnthropicClient + DiagnosisConsumer can parse cleanly. Spend up to 30 min on this; if it's blocking, mark the test as `@pytest.mark.skip` with a clear TODO and move on — PR 3c can land it when cassettes get recorded properly.

**Commit:** `test(evals): integration test — end-to-end run with cassette + fixture YAML`.

---

## Task 8: Sweep + review + PR

- [ ] `make lint && make typecheck && make test && make test-integration` — all green
- [ ] Code review via `superpowers:requesting-code-review`. Specific concerns:
  - Single-process architecture viable? Anything that makes us regret it before PR 3c lands?
  - Lifespan branch on `eval_mode` — any code paths that mistakenly assume eval mode === default mode?
  - Cassette wiring correct? `app.state.cassette_transport` reachable from runner without leaks?
  - Failure modes (ingest_failed / timeout / schema_failed) — all paths produce a valid `EvalCaseResultRecord`?
  - The hand-written cassette in the integration test — fragile? Better alternatives?
- [ ] Address feedback
- [ ] Push (`git push -u origin feat/eval-harness-pr3b-runner`) + open PR with base = `feat/eval-harness-pr3a-harness`

PR title: `feat(evals): runner + report + CLI (PR 3b/4 of Work Area K)`

PR body notes:
- Stacked on PR #44 (PR 3a — harness pieces)
- Single-process architecture: runner builds FastAPI in-process via ASGITransport; ActiveCaseRegistry is a module singleton
- Compose `app` container must be stopped before running (consumer-group contention)
- Cassette replay only — record mode lands in PR 3c
- Tests: 1 integration test + 4 unit tests (runner) + 6 unit tests (report) + 4 unit tests (cli) + 4 unit tests (settings)

---

## Notes for the implementer subagents

The plan describes intent + key signatures rather than verbatim code (PR 3b has more orchestration judgement than 3a). Implementers should:

- Follow the file structure and the signatures, but feel free to refactor for clarity
- Match the project conventions (frozen dataclasses, `from __future__ import annotations`, `Mapped[Any]` on JSONB, etc.)
- If a translation between the design and the actual code requires a judgement call, make it, document it in the commit message, and flag it in the task report
- Don't try to verbatim-paste — for PR 3b, the plan trades verbatim discipline for orchestration flexibility
- The verbatim-paste discipline of PR 3a worked because the modules were small and self-contained. PR 3b touches lifespan, settings, the CLI — many integration points where the right code depends on what's already there
