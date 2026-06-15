# Evals harness — PRs 5/6/7 design

Date: 2026-05-21
Scope: close out the three deferred issues from the eval-harness sequence (#48, #49, #50). Each is its own PR; merged in order so per-PR `evals-gate` validates against the baseline cleanly.

| PR | Issue | Title |
|----|-------|-------|
| 5  | #48   | Wire `PostgresEvalRunRepository` lifecycle (`start_run` / `persist_shot` / `finalize_run`) |
| 6  | #49   | Multi-shot stability via in-memory shot 1+ (Option 3) |
| 7  | #50   | `test_runner_e2e` against real FastAPI app + Postgres |

## Background

The eval-harness sequence (PRs 1, 3a/b/c, 3.5, 4a, 4b) shipped a working harness with a 10-case corpus, recorded cassettes, baseline + regression gate in CI, and a nightly live-API workflow. Three follow-ups were deferred:

- The CLI persists eval results to JSON+MD files via an in-memory `_StubShotPersister`. The Postgres-backed `EvalRunRepository` protocol and `PostgresEvalRunRepository` concrete exist (Work Area K PR 1, #42) but the run-lifecycle (`start_run` / `finalize_run`) was never wired into the CLI.
- The CLI defaults `--shots 1` because shots 2+ silently collapse onto shot 0's diagnosis via fingerprint dedup → `uq_diagnoses_incident_prompt_model`. Per-shot stability is structurally 0.
- `tests/integration/evals/test_runner_e2e.py` is a `@pytest.mark.skip` scaffold; PR 3.5 added unit-level regression tests for each surfaced bug but no integration test pins them in one shot.

## Non-negotiables that constrain all three PRs

- The diagnosis production contract (`uq_diagnoses_incident_prompt_model` as idempotency, fingerprint = `sha256(service || normalized_title || severity)`) does not fork for eval-mode. Both invariants stay.
- Per-PR `evals-gate` (CI) must continue to pass against the existing baseline — no regression to `category_match`, `hypothesis_cosine`, `action_coverage`, `evidence_quality` greater than 5%.
- No new external dependencies; everything stays in the existing module tree.
- ADRs in `docs/adr/` are mandatory for the two non-obvious decisions (trigger inference legacy-naming, stability-at-LLM-call-level).

---

## PR 5 — Issue #48 — `PostgresEvalRunRepository` lifecycle

### Why it's its own PR

The CLI today writes a row of value (JSON+MD report) via file persistence. Switching to Postgres-backed run history is a product feature (queryable run history, baseline comparison via `get_latest_ok_run`, regression gating reading from DB instead of file scan) — not cleanup. It also introduces failure modes (DB unavailable at run start) that need explicit handling.

### What changes

`sentinel/evals/cli.py`:
- Add `_discover_run_metadata(args, settings) -> dict[str, Any]` that returns the `start_run` kwargs (see table below).
- Replace the stub instantiation at lines ~580-581 with:
  ```python
  run_repo = PostgresEvalRunRepository(session_factory)
  start_kwargs = _discover_run_metadata(args, settings)
  run_id = await run_repo.start_run(**start_kwargs)
  ```
- Wrap `run_corpus(...)` in `try/finally`:
  - Success: `await run_repo.finalize_run(run_id, status="ok", metrics=..., metrics_stability=..., regression_baseline_sha=None, regression_passed=None, regression_detail=None)`.
  - Failure: `await run_repo.finalize_run(run_id, status="failed", ..., regression_detail={"error": repr(exc)})`.
  - `KeyboardInterrupt`: `status="failed"`, `regression_detail={"error": "interrupted"}`, then re-raise.
- Add `--no-persist` flag (default off). When set, retains `_StubShotPersister` for offline replay/dev iteration; no `start_run`/`finalize_run`.
- `_cmd_baseline` already delegates to `_cmd_run`; ensure the trigger inference correctly tags the run as `baseline` rather than the underlying `run` subcommand. Approach: inspect `args.subcommand` directly — `argparse` already sets it via `sub = parser.add_subparsers(dest="subcommand")` at `cli.py:88`, so `args.subcommand` survives the `_cmd_baseline → _cmd_run` call without mutation.

`sentinel/evals/runner.py`:
- No structural change. `EvalShotPersister` Protocol is already satisfied by `PostgresEvalRunRepository.persist_shot` structurally.

`tests/integration/evals/test_run_lifecycle.py` (new):
- Full lifecycle: `start_run → persist_shot × N → finalize_run → get_run`.
- Failure path: `start_run → persist_shot → finalize_run(status="failed")` → `get_run` returns failed row.
- Reuses existing Postgres integration fixture.

`docs/adr/0008-eval-run-trigger-inference.md` (new):
- Trigger inference table (below).
- Legacy `ci-smoke` value retained even though PR 4a made the PR gate full-corpus — renaming requires a Postgres CHECK constraint migration + Protocol Literal change, out of scope for this PR. The CI job is *named* `evals-gate`; the *persisted trigger value* is `ci-smoke` for historical continuity.
- `--allow-dirty` behavior + fail-loud default on dirty tree.

`Makefile`:
- No change to `make evals` invocation; the CLI handles the wiring transparently.

### Trigger inference table

`_discover_run_metadata` derives the `trigger` field from `args.subcommand` + env. The table is:

| `args.subcommand` | `CI` env | `GITHUB_WORKFLOW` env | → trigger |
|---|---|---|---|
| `baseline` | * | * | `baseline` |
| `run` | unset/false | * | `local` |
| `run` | true | `nightly-evals` | `ci-nightly` |
| `run` | true | `ci` (PR gate job lives in `ci.yml`) | `ci-smoke` (legacy name for the PR-gate trigger) |
| `run` | true | any other value | `manual` (covers `workflow_dispatch`) |

Other `start_run` kwargs:
- `git_sha`: `subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True).stdout.decode().strip()`. If the working tree is dirty (`git status --porcelain` non-empty), fail loud unless `--allow-dirty` is passed. CI tree is always clean; local dirty runs are surface-area for "I ran evals against uncommitted code" confusion.
- `corpus_version`: `sha256` of the concatenation of sorted-by-name `sentinel/evals/corpus/*.yaml` bytes, hex-encoded.
- `fetcher_fixture_hash`: `sha256` of each case's `context_seed` block serialized as canonical JSON (sort_keys=True), in case-id order, hex-encoded.
- `model`: `settings.anthropic_model`.
- `prompt_version`: `settings.diagnosis_prompt_version`.
- `embedding_model_id`: `settings.embedding_model_name`.
- `corpus_size`: `len(cases)` after `--smoke` filtering.
- `shots_per_case`: `args.shots`.
- `extra`: `{"cassette_mode": "replay"|"record", "allow_dirty": bool}`.

### Failure modes

| Failure | What's logged / what happens |
|---|---|
| `git rev-parse HEAD` fails (not a repo, no commits) | Fail loud, exit 1 before any DB write. |
| Working tree dirty + no `--allow-dirty` | Fail loud, exit 1 before any DB write. |
| `start_run` raises (DB down) | Exception propagates; CLI exits non-zero before the harness boots. |
| `run_corpus` raises | `finalize_run(status="failed", regression_detail={"error": repr(exc)})` in `finally`, then re-raise. |
| `finalize_run` raises post-run | Log + exit non-zero, but the eval results already in `eval_case_results` are intact (FK to `eval_runs.id` still resolves; row is `status="running"` until a future fix-up). |

### Acceptance

- `make evals` writes one row to `eval_runs` and N rows to `eval_case_results`.
- `make evals --no-persist` falls back to the stub persister; no DB writes.
- `_discover_run_metadata` unit tests cover every row of the trigger table.
- Integration test passes against the existing compose stack.
- ADR 0008 committed in the same PR.

---

## PR 6 — Issue #49 — multi-shot stability via in-memory shot 1+

### Mechanism (Option 3 from the issue)

Shot 0 runs through the full webhook → Kafka → enricher → diagnoser → Postgres pipeline (unchanged). Shots 1+ call `sentinel.diagnosis.agent.diagnose(...)` directly from the runner against an in-memory `IncidentContext` reconstructed from the corpus seed, and a fabricated `IncidentDetailResponse` matching shot 0's incident shape. Shots 1+ are NOT persisted to either `diagnoses` or `eval_case_results` (no FK to a real incident row) — they exist only as `MetricSet` entries in the per-case shot list and contribute to the stability stddev.

### Why this preserves invariants

- The production fingerprint contract is untouched: shots 1+ never hit ingestion.
- The diagnosis UNIQUE constraint is untouched: shots 1+ never call `save_with_outbox`.
- Cassette replay still keys on `(prompt_version, model_id, case_id, shot_index)`: the runner sets `shot_index` on the cassette context before each shot, the in-memory `diagnose()` call goes through the same `AnthropicClient` (which is wrapped by the cassette transport).

### What changes

`sentinel/evals/runner.py`:
- `RunnerDeps` gains `diagnosis_deps: DiagnosisDeps | None = None`. When `None`, multi-shot is rejected at `run_corpus` entry (`shots_per_case > 1 raises ValueError`).
- New helper `_run_in_memory_shot(case, deps, shot_index) -> MetricSet`:
  1. Set cassette context for `shot_index`.
  2. Reconstruct `IncidentContext` via `_seed_to_incident_context(case)` (already exists for scoring).
  3. Fabricate `IncidentDetailResponse` from `case.alert` + the reconstructed context. The fabricated incident's UUID is fresh (`uuid4()`); the audit logger inside `diagnose()` records `incident_id=<fabricated>`, which is acceptable because no DB row is keyed to it.
  4. `persisted = await diagnose(fabricated_incident, ctx, deps.diagnosis_deps)`.
  5. Score and return `MetricSet`.
- The shot loop becomes:
  ```python
  for shot_index in range(shots_per_case):
      if shot_index == 0:
          # Existing full-pipeline path, persists shot 0.
          metrics = await _full_pipeline_shot(case, deps, shot_index=0)
      else:
          # Direct LLM call against in-memory context.
          metrics = await _run_in_memory_shot(case, deps, shot_index=shot_index)
      per_case_shots[case.id].append(metrics)
  ```
  Note: shot 0 still goes through `persist_shot` to record the canonical row; shots 1+ don't.
- The runner's `eval_case_results` rows therefore stay one-per-case (shot_index=0), but `per_case_shots[case_id]` carries N `MetricSet` entries → stability stddev is meaningful.

`sentinel/evals/cli.py`:
- Reach for `app.state.diagnosis_deps` after the lifespan starts and pass it into `RunnerDeps`.
- Default `--shots` flag: `1 → 3`.

`sentinel/api/app.py`:
- Expose `app.state.diagnosis_deps` from the lifespan. As of today `diagnosis_deps` is constructed as a local at `app.py:315` and never assigned to `app.state`; only `app.state.cassette_transport` is exposed (`app.py:346`). Adding `app.state.diagnosis_deps = diagnosis_deps` in the lifespan is a mandatory part of this PR — without it the runner can't reach the cassette-wrapped LLM.

`sentinel/evals/report.py`:
- No change. Already conditionally renders the stability column when `shots_per_case > 1`.

`README.md`:
- Remove the "stability disabled" footnote.
- Update headline numbers from the next baseline-run that uses `--shots 3`. (The baseline file's `shots_per_case` field becomes `3`; `compare-to-baseline` already gates on shots-per-case mismatch.)

`evals/baselines/main.json`:
- Re-baseline with `--shots 3` once the mechanism is verified. This baseline update is part of PR 6.

`docs/adr/0009-stability-at-llm-call-level.md` (new):
- Why Option 3 over Options 1 & 2 from the issue.
- The decoupling: stability measures LLM-call non-determinism; persistence measures the production idempotency contract. They are intentionally not the same thing.
- Deferred alternative: `eval_shot_diagnoses` table if/when "all shot outputs queryable" becomes a real ask.

### Failure modes

| Failure | What's logged / what happens |
|---|---|
| `--shots > 1` + `diagnosis_deps is None` | `ValueError` at `run_corpus` entry, before any case runs. |
| Shot 1+ LLM call fails (cassette miss, validation error) | The exception propagates per existing `diagnose()` semantics. The case's `MetricSet` list is incomplete → `_per_case_stats` operates on what landed; one missing shot is acceptable signal. |
| Shot 1+ produces hallucinated-evidence diagnosis | Confidence cap applies inside `diagnose()`; the in-memory shot's `MetricSet` reflects the capped score; stability stddev for that case grows. |

### Acceptance

- `--shots 3` produces a non-zero `mean_stability` across the corpus.
- `eval_case_results` row count stays at one per case (no schema change, no shadow rows).
- Recorded cassettes exist for `(case_id, shot_index)` ∈ corpus × {0, 1, 2}. (Cassette recording is a manual step run against the live API; the recorded set is committed.)
- ADR 0009 committed in the same PR.
- README headline numbers refreshed from a `--shots 3` baseline.

---

## PR 7 — Issue #50 — `test_runner_e2e`

### Wiring

`tests/integration/evals/test_runner_e2e.py` (replaces the skipped scaffold):

- Marks: `@pytest.mark.integration`.
- Fixtures: `tests/integration/evals/conftest.py` exists today but is a placeholder (it documents that real fixtures land in PR 3c). PR 7 must either populate it with real fixtures (`postgres_dsn`, `kafka_broker`, Redis) or reuse parent-level fixtures from `tests/integration/conftest.py` — confirm which during implementation. Whichever path is chosen, the placeholder content must be replaced, not extended.
- App: `from sentinel.api.app import build_app; app = build_app()`, then `httpx.ASGITransport(app)`.
- Cassette: `CassetteTransport` in `replay` mode against `tests/integration/evals/fixtures/cassettes/` (one or two pre-recorded responses).
- Corpus: tiny one- or two-case fixture corpus under `tests/integration/evals/fixtures/corpus/` (curated for this test, not the production corpus).
- Real `PostgresDiagnosisRepository` + `PostgresEvalRunRepository`.

### Assertions (each pins one regression from PR 3.5)

Kafka-event assertions are made against the **outbox table** (`OutboxEventModel`), not via a Kafka consumer. The outbox is the authoritative staged-event record; the publisher to Kafka is a separate concern with its own tests. This avoids dragging Kafka consumer wiring into pytest.

1. `incident.opened` outbox row has `payload.fingerprint` and `payload.source` populated. (Bug 1: enricher payload regression.)
2. `incident.enriched` outbox row has `payload.fingerprint` and `payload.source` populated. (Bug 2: enriched-event envelope regression.)
3. Diagnosis row has non-empty `root_cause`/`hypothesis` — cassette body decoded exactly once. (Bug 3: content-encoding double-decode.)
4. Two-shot run produces two distinct `external_id`s in the incident rows; no per-shot collision. (Bug 4: shot-collision regression.)
5. All evidence refs in the persisted diagnosis resolve via the lenient kind-bucketed matcher (both `[deploy:abc]` and bare `abc` forms accepted). (Bug 5: evidence-id format regression.)
6. `eval_runs` has one row in `status="ok"`; `eval_case_results` count matches `corpus_size × shots_per_case=1` (per PR 6, the in-memory shot 1+ path doesn't persist; we run this test with `shots=1` to keep the persistence assertion clean).

### Why `shots=1` here

PR 6's stability mechanism deliberately doesn't persist shots 1+. Running the e2e test with `shots=1` makes the persistence assertions deterministic; multi-shot semantics are tested separately at the unit level. If we ever want an e2e multi-shot test, that's a follow-up issue and a separate fixture.

### Acceptance

- Test passes against the compose stack (`make test-integration`).
- Each of the six assertions independently fails if its underlying regression is re-introduced (verified by deliberate revert during PR review).
- Test runs in CI `integration` job, not `evals-gate` (the latter is a corpus-replay run, not pytest).
- No new dependencies introduced.

---

## Sequencing rationale (recap)

- **#48 first** — #50 depends on it (real persister needed for `eval_runs` assertion).
- **#49 second** — independent of #48 in code, but easier to land after #48's lifecycle is stable so the baseline re-record + commit happens against the new run-history wiring.
- **#50 last** — pins everything up to this point.

Each PR's CI `evals-gate` will validate against the existing baseline in `evals/baselines/main.json`. PR 6 will refresh that baseline as part of its diff; PRs 5 and 7 must not change headline numbers.

## Out of scope (deferred follow-ups)

- `eval_shot_diagnoses` table for queryable shot-level diagnoses.
- Renaming the `ci-smoke` Literal value to `ci-pr` (requires Postgres CHECK migration; not load-bearing).
- Weekly baseline-update PR-bot.
- e2e multi-shot stability assertion (intentional follow-up if needed).
