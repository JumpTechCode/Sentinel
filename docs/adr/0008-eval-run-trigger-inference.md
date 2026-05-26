# 0008 — Eval-run trigger inference

**Status:** Accepted
**Date:** 2026-05-21

## Context

PR 5 (issue #48) wires `PostgresEvalRunRepository` into the eval CLI. The
`start_run` call requires a `trigger` field whose values are constrained
by a Postgres CHECK constraint (`ck_eval_runs_trigger_valid`) to one of
`local | ci-smoke | ci-nightly | baseline | manual`.

The CLI must derive `trigger` from `args.subcommand` + the environment.

## Decision

`_discover_run_metadata` derives the trigger from this table:

| `args.subcommand` | `CI` env    | `GITHUB_WORKFLOW` env | trigger      |
|-------------------|-------------|-----------------------|--------------|
| `baseline`        | *           | *                     | `baseline`   |
| `run`             | unset/false | *                     | `local`      |
| `run`             | true        | `nightly-evals`       | `ci-nightly` |
| `run`             | true        | `ci`                  | `ci-smoke`   |
| `run`             | true        | other                 | `manual`     |

`baseline` wins over CI signals: a baseline run inside CI is still a baseline
operation, not a regression-gate trigger.

`manual` covers `workflow_dispatch` and any future named workflow that hasn't
been mapped yet. Surfacing as `manual` rather than failing prevents an
unexpected workflow name from breaking eval runs.

## Why `ci-smoke` for the PR-gate trigger

PR 4a renamed the CI job from `evals-smoke` to `evals-gate` and switched it
from a 5-case smoke set to the full 10-case corpus. The persisted **value**
stays as `ci-smoke` because:

1. The Postgres CHECK constraint is part of migration 0006. Adding `ci-pr` (or
   renaming `ci-smoke` → `ci-pr`) requires a new migration with both
   `upgrade` and `downgrade`, plus a Protocol Literal change in
   `sentinel/persistence/repositories.py`. That's not load-bearing for this
   PR — the value is opaque to consumers (the regression gate filters on
   `regression_passed`, not `trigger`).
2. The trigger column is already prefixed with `ci-` to disambiguate from
   `local` and `baseline` runs; the suffix is incidental.

The CI job's *name* (`evals-gate`) is the operator-visible label; the
persisted *value* (`ci-smoke`) is internal. The two need not match. If/when
a downstream consumer needs to filter on "PR gate runs vs other CI runs,"
adding `ci-pr` becomes load-bearing and we migrate then.

## Why fail-loud on dirty tree

A dirty working tree at run time means the run was scored against
uncommitted code. Two months later when someone asks "why don't these
numbers reproduce," there's no `git_sha` that recovers the exact state —
the SHA recorded is the HEAD commit, not the dirty diff.

`--allow-dirty` exists for the local dev case ("I'm iterating on a prompt
change"), but its absence in CI ensures nightly runs and baseline updates
are anchored to a committable commit.

## Consequences

- `eval_runs.trigger` is queryable but coarse-grained; `extra->>'cassette_mode'`
  carries the orthogonal replay-vs-live dimension.
- `make readme-numbers` (when it lands) can read
  `get_latest_ok_run(trigger='baseline')` for headline numbers, falling
  through to `get_latest_ok_run()` if no baseline exists yet.
- A future migration can add `ci-pr` (or rename `ci-smoke`) cleanly: both
  upgrade and downgrade paths can rewrite the column in place because the
  CHECK constraint is the only consumer.

## References

- Issue #48
- `plans/2026-05-21-evals-pr5-7-design.md` (PR 5 section)
- `migrations/versions/0006_eval_runs_and_case_results.py` (CHECK constraint)
- `sentinel/persistence/repositories.py` (`PostgresEvalRunRepository` Protocol)
- `sentinel/evals/cli.py` (`_discover_run_metadata`, `_infer_trigger`)
