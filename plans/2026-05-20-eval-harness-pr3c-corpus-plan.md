# Eval Harness PR 3c — Corpus + Cassettes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Ship the 10-case curated postmortem corpus + 30 recorded cassettes that turn the eval harness from "runnable in theory" into "runnable with real numbers." Implement the `record` CLI subcommand (currently a stub in PR 3b), record cassettes against the live Anthropic API, run the first eval, patch the README between the `<!-- evals:start -->` markers with real numbers.

**Architecture:** Pure additive work — no schema changes, no settings changes. Three categories:
1. **Corpus drafting** (research-heavy): 10 YAML files under `sentinel/evals/corpus/` extracted from real public postmortems, citation included.
2. **`record` subcommand**: identical orchestration to `run` but with cassette mode set to "record" instead of "replay" — captures HTTP exchanges to `<cassette_dir>/<key>.json`. Requires `ANTHROPIC_API_KEY` set.
3. **First-run + README patch**: run `make evals-smoke` against the recorded cassettes to validate replay end-to-end, then `make readme-numbers` to patch the eval-results section of the README.

**Tech Stack:** No new deps. Uses everything from PRs 3a + 3b.

**Spec reference:** `plans/2026-05-20-eval-harness-design.md` §1, §2, §3, §6, §7. PR 3a (#44) + 3b (#45) ship the harness; this PR ships the data + first-run.

**Dependency:** stacks on `feat/eval-harness-pr3b-runner` (PR #45).

---

## Out of scope (deferred)

- **Reconciliation with PR 1's persistence types** (drop local `EvalCaseResultRecord` duplicate, wire `PostgresEvalRunRepository.persist_shot`, replace `_StubShotPersister`) — small follow-up PR after #42 + this PR both merge to main.
- **CI workflows** (smoke / nightly / weekly baseline) — PR 4.

---

## Candidate corpus (10 incidents, diverse by category)

Drafted from well-known public postmortems with full writeups. Categories per the design's `category` enum (deploy / config / dependency / capacity / data / external).

| # | id (filename stem) | Date | Source | Primary category | Acceptable |
|---|---|---|---|---|---|
| 1 | cloudflare-2022-06-21-bgp | 2022-06-21 | Cloudflare blog | config | config, deploy |
| 2 | cloudflare-2019-07-02-regex | 2019-07-02 | Cloudflare blog | deploy | deploy, config |
| 3 | github-2018-10-21-network | 2018-10-21 | GitHub blog | dependency | dependency, data |
| 4 | aws-2021-12-07-useast1 | 2021-12-07 | AWS post-event | capacity | capacity, dependency |
| 5 | stripe-2019-07-10-db-failover | 2019-07-10 | Stripe status writeup | dependency | dependency, capacity |
| 6 | gitlab-2017-01-31-db-deletion | 2017-01-31 | GitLab blog | data | data |
| 7 | roblox-2021-10-28-consul | 2021-10-28 | Roblox blog | capacity | capacity, dependency |
| 8 | slack-2021-01-04-dns | 2021-01-04 | Slack blog | external | external, dependency |
| 9 | atlassian-2022-04-04-deletion | 2022-04-04 | Atlassian PIR | data | data, deploy |
| 10 | fastly-2021-06-08-config | 2021-06-08 | Fastly blog | config | config, deploy |

Distribution: **deploy×1, config×2, dependency×2, capacity×2, data×2, external×1**. Reasonable coverage across categories without over-indexing on one type.

---

## File Structure

**New (under `sentinel/evals/corpus/`):**
- `cloudflare-2022-06-21-bgp.yaml`
- `cloudflare-2019-07-02-regex.yaml`
- `github-2018-10-21-network.yaml`
- `aws-2021-12-07-useast1.yaml`
- `stripe-2019-07-10-db-failover.yaml`
- `gitlab-2017-01-31-db-deletion.yaml`
- `roblox-2021-10-28-consul.yaml`
- `slack-2021-01-04-dns.yaml`
- `atlassian-2022-04-04-deletion.yaml`
- `fastly-2021-06-08-config.yaml`
- `README.md` (curator guide: how to add a new case, the 5-case smoke subset semantics, sourcing discipline)

**New (under `sentinel/evals/cassettes/`):**
- 30 recorded cassette JSON files (10 cases × 3 shots), named `<key>.json` per `compute_cassette_key`.
- `.gitkeep` so the directory exists pre-recording.

**Modified:**
- `sentinel/evals/cli.py` — implement `_cmd_record` (currently a stub that exits 1)
- `tests/unit/evals/test_cli.py` — replace the `test_record_subcommand_is_a_stub` test with a real test of the record path
- `README.md` — patched by `make readme-numbers` between the `<!-- evals:start -->` markers

---

## Task 0: Verify branch + open the record subcommand stub

- [ ] Confirm on `feat/eval-harness-pr3c-corpus` stacked on `feat/eval-harness-pr3b-runner`
- [ ] Find the stub: `grep -n "_cmd_record" sentinel/evals/cli.py` — it currently prints "use manual record flow until PR 3c lands" and exits 1

---

## Task 1: Implement the `record` subcommand

**Files:**
- Modify: `sentinel/evals/cli.py`
- Modify: `tests/unit/evals/test_cli.py`

### Behavior

The record subcommand is mostly identical to `run` except:
- `CassetteTransport(mode="record", ...)` instead of `mode="replay"`
- Cassette dir is REQUIRED (no `--live` fallback — recording requires the cassette dir target)
- Requires `ANTHROPIC_API_KEY` env var (the inner transport hits the live API)
- Default shots = 1 (recording multiple shots is wasteful; the runner's per-shot variance is for replay-time measurement, not for recording. Recording N shots produces N cassettes per case for the runner to replay later.)
- Wait — actually NO: the cassette key includes shot_index, so we need to record one cassette per (case, shot). If we want N shots at replay time, we need N cassettes at record time. So default shots = N from the replay default = 3. Keep at 3.
- Output report still written but with a "[RECORDING RUN]" prefix in the markdown

### Implementation

Factor the shared orchestration in `_run_async` into a helper. The record subcommand calls the same helper with different `mode` and cassette-dir-required guards. Roughly:

```python
def _cmd_record(args: argparse.Namespace) -> int:
    try:
        settings = load_settings()
    except Exception as exc:
        ...

    if not settings.eval_mode:
        return _fail("SENTINEL_EVAL_MODE must be true")
    if not settings.diagnosis_consumer_enabled:
        return _fail("SENTINEL_DIAGNOSIS_CONSUMER_ENABLED must be true")

    corpus_dir = args.corpus or settings.eval_corpus_dir
    if corpus_dir is None:
        return _fail("--corpus or SENTINEL_EVAL_CORPUS_DIR required")

    cassette_dir = args.cassette_dir or settings.eval_cassette_dir
    if cassette_dir is None:
        return _fail("--cassette-dir or SENTINEL_EVAL_CASSETTE_DIR required for record")

    if not os.environ.get("ANTHROPIC_API_KEY") and not os.environ.get("SENTINEL_ANTHROPIC_API_KEY"):
        return _fail("ANTHROPIC_API_KEY required for record mode (live API calls)")

    try:
        return asyncio.run(_record_async(args, settings, corpus_dir, cassette_dir))
    except RuntimeError as exc:
        return _fail(str(exc))
```

`_record_async` is similar to `_run_async` but builds the CassetteTransport with `mode="record"` and passes it through the same lifespan + runner machinery. The transport's record-mode check (added in PR 3a's #44 review) validates the API key at construction.

### Wait — there's a wiring issue to fix here

Currently the lifespan in `sentinel/api/app.py` hardcodes `mode="replay"` when building the CassetteTransport (PR 3b Task 3). For record mode, we need either:
1. **Settings-driven mode**: add `eval_cassette_mode: Literal["record", "replay"] = "replay"` and read it in lifespan. The record subcommand sets `SENTINEL_EVAL_CASSETTE_MODE=record` before building the app.
2. **App.state override**: the record subcommand sets `app.state.cassette_mode_override = "record"` before lifespan runs — which doesn't work because state isn't readable during construction.

Go with option 1 — add `eval_cassette_mode` to Settings. The CLI passes the right value as an env var before building the app. Update the lifespan branch in `app.py` to read it.

### Tests

Replace the existing `test_record_subcommand_is_a_stub` test with:
- `test_record_requires_eval_mode` — sets eval_mode=False, expect SystemExit(1)
- `test_record_requires_cassette_dir` — no cassette dir set, expect exit 1
- `test_record_requires_api_key` — no API key in env, expect exit 1

(Don't add a "happy path" test — recording requires the full stack + live API. That's exercised in Task 3.)

### Commit

`feat(evals): implement record subcommand (live Anthropic → cassettes)`

---

## Task 2: Draft the 10 corpus YAMLs

**Files (all new):**
- `sentinel/evals/corpus/<id>.yaml` × 10
- `sentinel/evals/corpus/README.md`

For each of the 10 candidates in the table above:

1. WebFetch the source URL
2. WebFetch any sources_consulted URLs (chase one-hop links if needed for technical depth)
3. Extract structured data:
   - **alert** — synthesize the symptom that triggered the page (e.g., for the Cloudflare BGP incident: "Elevated 5xx errors across multiple POPs"). Use the actual production service name ("edge-network") and severity (SEV1).
   - **context_seed** — what enrichment would have surfaced if Sentinel had been watching:
     - `deploys` — 1-2 deploys around the incident time, with PR titles + summaries extracted from the writeup
     - `recent_logs` — 2-4 log lines pulled or paraphrased from the writeup
     - `similar_incidents` — usually empty (the first occurrence of a kind); add 1 if the writeup cites a prior recurrence
     - `runbooks` — usually empty; add 1 if the writeup cites an existing runbook
     - `related_alerts` / `active_alerts` — usually empty for clarity
   - **ground_truth** — extracted from the writeup's stated root cause:
     - `category` — primary (single)
     - `acceptable_categories` — primary + adjacent (e.g., config + deploy when the config change shipped via deploy)
     - `root_cause` — 1-sentence summary in the writeup's own framing
     - `correct_actions` — 1-3 actions the writeup says fixed or would have fixed the incident
4. Validate via `python -c "from sentinel.evals.corpus_loader import load_case; from pathlib import Path; load_case(Path('sentinel/evals/corpus/X.yaml'))"` — fail-loud on schema mismatches.

### Corpus README

Write `sentinel/evals/corpus/README.md` documenting:
- How to add a new case (template, sourcing discipline, ID conventions)
- The 5-case smoke subset = first 5 cases by sorted id (per design §7)
- Citation requirement: every case has a `source_url` to a public postmortem; `notes` carries the curator's rationale

### Commit

One per case is too noisy. Bundle as:
- `feat(evals): corpus — 10 postmortem cases drafted` (10 YAMLs + README in one commit)

---

## Task 3: Record cassettes for all 10 cases × 3 shots

**Files (all new):**
- `sentinel/evals/cassettes/<key>.json` × 30
- `sentinel/evals/cassettes/.gitkeep` (so the dir exists pre-recording)

### Steps

1. Ensure the compose `app` container is stopped (`docker compose stop app`) to avoid consumer-group contention.
2. Set required env vars: `SENTINEL_EVAL_MODE=true SENTINEL_EVAL_CORPUS_DIR=sentinel/evals/corpus SENTINEL_EVAL_CASSETTE_DIR=sentinel/evals/cassettes SENTINEL_EVAL_CASSETTE_MODE=record SENTINEL_DIAGNOSIS_CONSUMER_ENABLED=true SENTINEL_ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY`.
3. `make evals-record` — runs the corpus end-to-end against the live API. ~30 calls, ~$0.45.
4. Validate: `ls sentinel/evals/cassettes/*.json | wc -l` should print 30.
5. **Verify replay works**: stop record mode, switch to replay mode:
   ```bash
   unset SENTINEL_EVAL_CASSETTE_MODE  # default replay
   make evals-smoke
   ```
   Smoke runs against the first 5 cases × 3 shots = 15 cassette replays. Should complete in <30s with no live API calls. Eval results JSON should land at `evals/results/<run_id>.json`.

### Failure modes during recording

Probable issues that may surface (PR 3b's e2e test was skip-marked, so this is the first end-to-end run):
- **HMAC mismatch**: synthetic webhook signature wrong. Fix in `runner.py::_fire_and_poll` — match what `webhook_handler.py` expects per source.
- **Cassette serialization breakage**: httpx headers byte-vs-str round-trip. Adjust the cassette JSON shape until replay matches record.
- **Diagnosis polling timeout**: the cassette captures the API response but the diagnosis consumer doesn't write to the DB in time. Check Kafka topic / consumer group / DB connection.
- **Eval-mode lifespan branch**: `cassette_transport` is None even when set. Re-trace the env-var path through Settings → lifespan.

Each failure surfaces as a CassetteMiss or a 60s timeout. Triage one at a time; commit the fix; re-record only the affected cases.

### Commit

- `feat(evals): record 30 cassettes against live Anthropic API (10 cases × 3 shots)`

Include in the commit message: model name, prompt version, recording date, total token cost.

---

## Task 4: First eval run + README patch

**Files:**
- Modify: `README.md` (between the `<!-- evals:start -->` markers)
- Possibly: `evals/results/<run_id>.{json,md}` are gitignored — confirm they don't need to be committed (per design §6, results are not version-controlled; the patched README carries the numbers).

### Steps

1. With cassettes recorded, run the full corpus in replay mode:
   ```bash
   make evals
   ```
   This generates an aggregate `RunResult` and a report. Capture the run_id.
2. `make readme-numbers` — patches the README's eval section from the latest run.
3. Manually review the patched README — is the markdown clean? Numbers reasonable? Headline metrics make sense?
4. If the numbers look off (e.g., evidence_quality < 0.5 across all cases), it's probably an ID-format mismatch between the YAML's seed IDs and what the diagnosis prompt actually cites. Inspect a per-case row, fix the YAML, re-record only the affected cases, re-run.

### Commit

- `docs(evals): first README eval numbers from the 10-case corpus`

---

## Task 5: Sweep + code review + PR

- [ ] `make lint && make typecheck && make test && make test-integration` — all green. (PR 3c adds no production code paths outside the record subcommand; existing tests should remain unaffected.)
- [ ] Code review via `superpowers:requesting-code-review`. Specific concerns:
  - Corpus YAMLs: are the citations real and accurate? Any obvious mis-categorisations? Is the `acceptable_categories` set defensible per case?
  - Record subcommand: does the wiring re-use enough of `_run_async` (no copy-paste)? Are the guards (eval_mode + cassette_dir + API key) all enforced?
  - First eval numbers: do they look plausible? Any signs of systemic mis-scoring (e.g., evidence_quality always 0 → ID format bug)?
  - Cassette commit size: is the in-tree size reasonable (~300KB expected)? Worth git-lfs?
- [ ] Address feedback
- [ ] Push (`git push -u origin feat/eval-harness-pr3c-corpus`) + open PR with base = `feat/eval-harness-pr3b-runner`

PR title: `feat(evals): 10-case corpus + recorded cassettes + first README numbers (PR 3c/4 of Work Area K)`

PR body notes:
- Stacked on PR #45 (PR 3b)
- 10 hand-drafted YAMLs from public postmortems (Cloudflare, GitHub, AWS, Stripe, GitLab, Roblox, Slack, Atlassian, Fastly)
- 30 cassettes recorded against the live Anthropic API
- First real eval numbers in the README
- **Reconciliation deferred** to a post-merge follow-up PR: the runner still uses the local `EvalCaseResultRecord` duplicate + `_StubShotPersister`; that gets cleaned up once PR #42 (PR 1) + this PR both merge.

---

## Notes for the implementer subagents

- Tasks 2 (corpus drafting) is research-heavy and best done in the parent context (controller) — WebFetch + careful extraction needs judgement. Don't delegate this to a subagent.
- Task 3 (recording) requires live API access and is best done interactively in the controller (you'll watch for errors and triage). Don't delegate.
- Tasks 1, 4, 5 are mechanical and can be dispatched.
- Total PR size estimate: ~800 LOC (10 YAMLs ~50 lines each, record subcommand + tests ~150, README + corpus README ~50, 30 cassettes ~10KB each as JSON).
