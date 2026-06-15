# PR 5 / Issue #48 — `PostgresEvalRunRepository` lifecycle implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the in-memory `_StubShotPersister` in `sentinel/evals/cli.py` with a real `PostgresEvalRunRepository` wired through the full `start_run → persist_shot → finalize_run` lifecycle, so each eval invocation produces a queryable `eval_runs` row.

**Architecture:** A new helper `_discover_run_metadata` collects all `start_run` kwargs (git_sha, corpus_version, fetcher_fixture_hash, trigger) from the CLI args + environment. `_run_async` opens the run before `run_corpus`, then closes it in `finally` with either an `ok` or `failed` status. A `--no-persist` flag retains the stub for offline dev iteration. The CI job name `evals-gate` maps to the legacy persisted trigger value `ci-smoke` (renaming would require a Postgres CHECK migration, deferred).

**Tech Stack:** Python 3.12, FastAPI, argparse, asyncpg/SQLAlchemy 2.x, alembic, pytest + pytest-asyncio, Anthropic SDK (touched indirectly via the existing cassette transport).

**Source-of-truth design:** `plans/2026-05-21-evals-pr5-7-design.md` (PR 5 section).

---

## File map

| Path | Action | Responsibility |
|---|---|---|
| `sentinel/evals/cli.py` | Modify | Add `--allow-dirty` + `--no-persist` flags; add `_discover_run_metadata` helper; replace stub instantiation in `_run_async`; wrap `run_corpus` in try/finally for `finalize_run`. |
| `tests/unit/evals/test_cli.py` | Modify | Extend with unit tests for `_discover_run_metadata` (trigger inference table, git_sha, corpus_version, fetcher_fixture_hash, dirty-tree handling). |
| `tests/integration/evals/test_run_lifecycle.py` | Create | End-to-end lifecycle test: `start_run → persist_shot × N → finalize_run → get_run` against real Postgres, both ok and failed paths. |
| `docs/adr/0008-eval-run-trigger-inference.md` | Create | ADR: trigger inference table, `ci-smoke` legacy-name rationale, dirty-tree failure-loud default. |

No schema changes. No migration. The `eval_runs.trigger` CHECK constraint already permits the five Literal values (verified at `migrations/versions/0006_eval_runs_and_case_results.py:40,107-109`).

---

## Conventions

- Per user memory `feedback-commits-by-user.md`: default is **stage and stop**. The "Commit" steps below stage and stop; the user (or you on user delegation) creates the actual git commit, with the user's identity + the AI co-author trailer.
- Per user memory `sentinel-review-before-commit.md`: **before any commit, dispatch a subagent code review via `superpowers:requesting-code-review`**. The review step is explicit in Task 6.
- Per CLAUDE.md: every new external behavior gets a test in the same PR. Every new flag/env var is added to `Settings` + `.env.example` + docker-compose where relevant (this PR adds none — the new flags are CLI-only).

---

## Task 1: Add `--allow-dirty` and `--no-persist` CLI flags

**Files:**
- Modify: `sentinel/evals/cli.py:83-200` (around `_build_parser`)
- Test: `tests/unit/evals/test_cli.py` (extend)

`--allow-dirty` is a top-level flag (applies to `run` and `baseline`). `--no-persist` is on `run` and `baseline` only (record + compare-to-baseline don't open eval_runs rows).

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/evals/test_cli.py`:

```python
def test_parser_accepts_allow_dirty() -> None:
    from sentinel.evals.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["run", "--allow-dirty"])
    assert args.allow_dirty is True


def test_parser_allow_dirty_default_false() -> None:
    from sentinel.evals.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["run"])
    assert args.allow_dirty is False


def test_parser_accepts_no_persist_on_run() -> None:
    from sentinel.evals.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["run", "--no-persist"])
    assert args.no_persist is True


def test_parser_no_persist_default_false() -> None:
    from sentinel.evals.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["run"])
    assert args.no_persist is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/unit/evals/test_cli.py::test_parser_accepts_allow_dirty tests/unit/evals/test_cli.py::test_parser_accepts_no_persist_on_run -v
```

Expected: FAIL — `argparse.ArgumentError` or `AttributeError`.

- [ ] **Step 3: Add the flags to `_build_parser`**

Inside `_build_parser` at the top (before subparser registration), add the `--allow-dirty` flag at parent-parser level:

```python
parser.add_argument(
    "--allow-dirty",
    action="store_true",
    help=(
        "permit eval runs against a dirty working tree (default: refuse). "
        "applies only to subcommands that open an eval_runs row (run, baseline)."
    ),
)
```

Inside the `run_p` subparser block (after the existing `--shots` arg):

```python
run_p.add_argument(
    "--no-persist",
    action="store_true",
    help=(
        "skip PostgresEvalRunRepository wiring; use the in-memory stub persister "
        "(no eval_runs / eval_case_results rows). default: persist to DB."
    ),
)
```

Inside the `baseline_p` subparser block (after its own `--shots` arg):

```python
baseline_p.add_argument(
    "--no-persist",
    action="store_true",
    help=(
        "skip PostgresEvalRunRepository wiring; use the in-memory stub persister "
        "(no eval_runs / eval_case_results rows). default: persist to DB."
    ),
)
```

If the existing baseline subparser variable isn't named `baseline_p`, use the actual variable name (grep `cli.py` for `add_parser("baseline"` and use the returned variable).

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/evals/test_cli.py -v -k "allow_dirty or no_persist"
```

Expected: PASS.

- [ ] **Step 5: Stage the changes**

```bash
git add sentinel/evals/cli.py tests/unit/evals/test_cli.py
git status
```

Stop here. Do not commit. (Commit batched at Task 6 after code review.)

---

## Task 2: Implement `_discover_run_metadata` with TDD

**Files:**
- Modify: `sentinel/evals/cli.py` (add helper above `_run_async`)
- Test: `tests/unit/evals/test_cli.py` (extend)

Helper signature:

```python
def _discover_run_metadata(
    args: argparse.Namespace,
    settings: Settings,
    corpus_dir: Path,
    cases: list[CorpusCase],
) -> dict[str, Any]:
    ...
```

Returns the full kwargs dict consumable by `PostgresEvalRunRepository.start_run(**kwargs)`. Raises `SystemExit(1)` after printing to stderr if the working tree is dirty and `--allow-dirty` is not set.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/evals/test_cli.py`:

```python
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _fake_settings() -> Any:
    """Minimal duck-typed Settings stand-in for _discover_run_metadata."""
    return SimpleNamespace(
        anthropic_model="claude-sonnet-4-5",
        diagnosis_prompt_version="v1",
        embedding_model_name="BAAI/bge-small-en-v1.5",
    )


def _patch_git(monkeypatch: pytest.MonkeyPatch, *, sha: str, dirty: bool) -> None:
    """Patch subprocess.run so _discover_run_metadata sees deterministic git state."""
    import subprocess

    def fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        if cmd[:3] == ["git", "rev-parse", "HEAD"]:
            return subprocess.CompletedProcess(cmd, 0, stdout=(sha + "\n").encode(), stderr=b"")
        if cmd[:3] == ["git", "status", "--porcelain"]:
            stdout = b" M sentinel/evals/cli.py\n" if dirty else b""
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr=b"")
        raise AssertionError(f"unexpected subprocess.run call: {cmd!r}")

    monkeypatch.setattr(subprocess, "run", fake_run)


def _write_corpus_fixture(tmp_path: Path) -> tuple[Path, list[Any]]:
    """Write a 1-case corpus YAML to tmp_path and return (corpus_dir, cases)."""
    from sentinel.evals.corpus_loader import load_corpus_dir

    yaml_text = (
        "id: cf-2019-cpu\n"
        "alert:\n"
        "  source: generic\n"
        "  service: api\n"
        "  severity: critical\n"
        "  title: 'CPU spike'\n"
        "  raw_payload: {}\n"
        "context_seed:\n"
        "  deploys: []\n"
        "  related_alerts: []\n"
        "  similar_incidents: []\n"
        "  runbooks: []\n"
        "  recent_logs: []\n"
        "  active_alerts: []\n"
        "ground_truth:\n"
        "  category: deploy\n"
        "  acceptable_categories: [deploy]\n"
        "  root_cause: 'bad deploy'\n"
        "  correct_actions: ['rollback']\n"
    )
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    (corpus_dir / "cf-2019-cpu.yaml").write_text(yaml_text)
    return corpus_dir, load_corpus_dir(corpus_dir)


def test_discover_metadata_trigger_local(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_WORKFLOW", raising=False)
    _patch_git(monkeypatch, sha="abc123" * 6 + "abcd", dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw["trigger"] == "local"
    assert kw["git_sha"] == "abc123" * 6 + "abcd"
    assert kw["status"] == "running"


def test_discover_metadata_trigger_baseline(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "nightly-evals")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="baseline", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw["trigger"] == "baseline"  # baseline subcommand wins over CI signals


def test_discover_metadata_trigger_ci_nightly(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "nightly-evals")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=True)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw["trigger"] == "ci-nightly"


def test_discover_metadata_trigger_ci_smoke(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "ci")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw["trigger"] == "ci-smoke"  # legacy name preserved


def test_discover_metadata_trigger_manual_workflow_dispatch(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "manual-replay")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw["trigger"] == "manual"


def test_discover_metadata_dirty_tree_refused(monkeypatch, tmp_path, capsys):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=True)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    with pytest.raises(SystemExit) as exc:
        _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "dirty" in err.lower()


def test_discover_metadata_dirty_tree_allowed(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=True)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=True, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw["extra"]["allow_dirty"] is True


def test_discover_metadata_corpus_version_deterministic(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw1 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)
    kw2 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw1["corpus_version"] == kw2["corpus_version"]
    assert len(kw1["corpus_version"]) == 64  # sha256 hex


def test_discover_metadata_fetcher_fixture_hash_deterministic(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw1 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)
    kw2 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw1["fetcher_fixture_hash"] == kw2["fetcher_fixture_hash"]
    assert len(kw1["fetcher_fixture_hash"]) == 64


def test_discover_metadata_includes_settings_fields(monkeypatch, tmp_path):
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)

    assert kw["model"] == "claude-sonnet-4-5"
    assert kw["prompt_version"] == "v1"
    assert kw["embedding_model_id"] == "BAAI/bge-small-en-v1.5"
    assert kw["corpus_size"] == 1
    assert "shots_per_case" in kw
```

Note: the tests use `getattr(args, "shots", 1)` style indirection via `SimpleNamespace`; the helper itself should read `args.shots` directly (set by the subparser).

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/unit/evals/test_cli.py -v -k "discover_metadata"
```

Expected: FAIL — `ImportError: cannot import name '_discover_run_metadata'`.

- [ ] **Step 3: Implement `_discover_run_metadata` in `sentinel/evals/cli.py`**

Add these imports at the top of `cli.py` (alphabetized into the existing import block):

```python
import hashlib
import os
import subprocess
```

Also ensure `from typing import Any` is already imported (it is — verify).

Add the helper above `_run_async`:

```python
def _discover_run_metadata(
    args: argparse.Namespace,
    settings: Settings,
    corpus_dir: Path,
    cases: list[CorpusCase],
) -> dict[str, Any]:
    """Collect every kwarg PostgresEvalRunRepository.start_run requires.

    Reads CLI args, env, git state, and the loaded corpus. Raises SystemExit(1)
    on a dirty working tree unless --allow-dirty is set; this is intentional —
    eval runs against uncommitted code are a frequent source of "why don't the
    numbers reproduce" confusion. See docs/adr/0008-eval-run-trigger-inference.md.
    """
    git_sha = _git_rev_parse_head()
    if _git_tree_is_dirty() and not args.allow_dirty:
        print(
            "error: working tree is dirty; commit or pass --allow-dirty",
            file=sys.stderr,
        )
        raise SystemExit(1)

    trigger = _infer_trigger(args)
    corpus_version = _hash_corpus_files(corpus_dir)
    fetcher_fixture_hash = _hash_fetcher_fixtures(cases)
    cassette_mode = "live" if getattr(args, "live", False) else "replay"
    shots = getattr(args, "shots", 1)

    return {
        "status": "running",
        "trigger": trigger,
        "git_sha": git_sha,
        "model": settings.anthropic_model,
        "prompt_version": settings.diagnosis_prompt_version,
        "embedding_model_id": settings.embedding_model_name,
        "corpus_version": corpus_version,
        "corpus_size": len(cases),
        "shots_per_case": shots,
        "fetcher_fixture_hash": fetcher_fixture_hash,
        "extra": {
            "cassette_mode": cassette_mode,
            "allow_dirty": args.allow_dirty,
            "subcommand": args.subcommand,
        },
    }


def _git_rev_parse_head() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
    )
    return result.stdout.decode().strip()


def _git_tree_is_dirty() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        check=True,
        capture_output=True,
    )
    return bool(result.stdout.strip())


def _infer_trigger(
    args: argparse.Namespace,
) -> Literal["local", "ci-smoke", "ci-nightly", "baseline", "manual"]:
    if args.subcommand == "baseline":
        return "baseline"
    ci = os.environ.get("CI", "").lower() in ("true", "1", "yes")
    if not ci:
        return "local"
    workflow = os.environ.get("GITHUB_WORKFLOW", "")
    if workflow == "nightly-evals":
        return "ci-nightly"
    if workflow == "ci":
        # Legacy persisted-value name. The job is renamed `evals-gate` and runs
        # the full corpus (PR 4a), but the persisted trigger value stays as
        # `ci-smoke` to avoid a Postgres CHECK migration. See ADR 0008.
        return "ci-smoke"
    return "manual"


def _hash_corpus_files(corpus_dir: Path) -> str:
    """sha256 of the concatenation of sorted-by-name corpus YAML bytes."""
    h = hashlib.sha256()
    for path in sorted(corpus_dir.glob("*.yaml")):
        h.update(path.read_bytes())
    return h.hexdigest()


def _hash_fetcher_fixtures(cases: list[CorpusCase]) -> str:
    """sha256 of each case's context_seed serialized as canonical JSON,
    concatenated in case-id order. Stable across runs because pydantic's
    model_dump(mode='json') is deterministic given the same input."""
    h = hashlib.sha256()
    for case in sorted(cases, key=lambda c: c.id):
        seed_json = json.dumps(
            case.context_seed.model_dump(mode="json"), sort_keys=True
        ).encode()
        h.update(seed_json)
    return h.hexdigest()
```

Also add `from typing import Literal` if not already imported.

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/evals/test_cli.py -v -k "discover_metadata"
```

Expected: PASS (10 tests).

- [ ] **Step 5: Run typecheck**

```bash
make typecheck
```

Expected: clean (no new errors). If `mypy --strict` complains about `Settings` (imported at top of `cli.py`), it's already there — no change needed.

- [ ] **Step 6: Stage the changes**

```bash
git add sentinel/evals/cli.py tests/unit/evals/test_cli.py
git status
```

---

## Task 3: Wire `PostgresEvalRunRepository` + lifecycle into `_run_async`

**Files:**
- Modify: `sentinel/evals/cli.py:540-617` (the `_run_async` body around the stub replacement point)

This is the load-bearing wiring change. The existing stub instantiation is at lines 580-581:

```python
run_id = uuid.uuid4()
shot_persister = _StubShotPersister()
```

Replace with a conditional on `args.no_persist`. The `run_corpus` call already happens at lines 598-603; wrap it in try/finally for `finalize_run`.

- [ ] **Step 1: Add the import**

Top of `cli.py`, in the existing `from sentinel.persistence.repositories import ...` group:

```python
from sentinel.persistence.repositories import (
    PostgresDiagnosisRepository,
    PostgresEvalRunRepository,
)
```

If there's no such group yet, search for the existing `PostgresDiagnosisRepository` import (around line 547 of `cli.py`) and add `PostgresEvalRunRepository` to the same import.

- [ ] **Step 2: Replace stub instantiation and add lifecycle wrapping**

Find this block (around lines 580-617):

```python
            # Run id is generated locally; the JSON+MD reports under
            # --output-dir are the canonical record of the run today. Switching
            # to PostgresEvalRunRepository requires building out the
            # ``start_run`` / ``finalize_run`` lifecycle (...).
            run_id = uuid.uuid4()
            shot_persister = _StubShotPersister()

            print(f"eval run starting: run_id={run_id} cases={len(cases)} shots={args.shots}")

            deps = RunnerDeps(
                client=client,
                diagnosis_repo=diagnosis_repo,
                eval_run_repo=shot_persister,
                ...
            )

            try:
                result = await run_corpus(
                    cases=cases,
                    shots_per_case=args.shots,
                    runner_deps=deps,
                )
            finally:
                await engine.dispose()
```

Replace with:

```python
            run_repo: EvalShotPersister
            run_id: UUID
            real_run_repo: PostgresEvalRunRepository | None = None
            if args.no_persist:
                run_id = uuid.uuid4()
                run_repo = _StubShotPersister()
                print(
                    f"eval run starting: run_id={run_id} cases={len(cases)} "
                    f"shots={args.shots} [--no-persist: in-memory only]"
                )
            else:
                real_run_repo = PostgresEvalRunRepository(session_factory)
                start_kwargs = _discover_run_metadata(args, settings, corpus_dir, cases)
                run_id = await real_run_repo.start_run(**start_kwargs)
                run_repo = real_run_repo
                print(
                    f"eval run starting: run_id={run_id} cases={len(cases)} "
                    f"shots={args.shots} trigger={start_kwargs['trigger']}"
                )

            deps = RunnerDeps(
                client=client,
                diagnosis_repo=diagnosis_repo,
                eval_run_repo=run_repo,
                embed=embed,
                cassette_transport=cassette_transport,
                run_id=run_id,
                prompt_version=settings.diagnosis_prompt_version,
                model_id=settings.anthropic_model,
                truncate_between_cases=_truncate,
                webhook_secret=_ensure_secret(webhook_secret),
            )

            result = None
            run_error: BaseException | None = None
            try:
                result = await run_corpus(
                    cases=cases,
                    shots_per_case=args.shots,
                    runner_deps=deps,
                )
            except BaseException as exc:  # finalize even on KeyboardInterrupt
                run_error = exc
                raise
            finally:
                if real_run_repo is not None:
                    await _finalize_run(real_run_repo, run_id, result, run_error)
                await engine.dispose()
```

Note: `corpus_dir` is the directory used by `load_corpus_dir(corpus_dir)` at the top of `_run_async`. It's already a local at line 475 area. If it's not bound by the helper's name, look for the `corpus_dir` parameter to `_run_async` (it's a positional arg per the function signature at line 463-467).

Also note: the existing code reads `corpus_dir` from the function signature; the spec assumes it's available. Verify.

- [ ] **Step 3: Add the `_finalize_run` helper**

Add to `cli.py` near `_discover_run_metadata`:

```python
async def _finalize_run(
    repo: PostgresEvalRunRepository,
    run_id: UUID,
    result: RunResult | None,
    error: BaseException | None,
) -> None:
    """Always-finalize wrapper for the run row. Called from `finally` so a
    cassette miss, KeyboardInterrupt, or DB error during the run still leaves
    a terminal-status row instead of a perpetually-`running` orphan.
    """
    if error is not None or result is None:
        status: Literal["ok", "failed", "partial"] = "failed"
        metrics: dict[str, float] = {}
        metrics_stability: dict[str, float] = {}
        regression_detail: dict[str, Any] | None = {"error": repr(error) if error else "no result"}
    else:
        status = "ok"
        agg = result.aggregate_metrics
        metrics = {
            "category_match": agg.category_match,
            "hypothesis_cosine": agg.hypothesis_cosine,
            "action_coverage": agg.action_coverage,
        }
        if agg.evidence_quality is not None:
            metrics["evidence_quality"] = agg.evidence_quality
        # Run-level stability = mean of per-case stddevs per metric. Skipping
        # for now; PR 6 adds meaningful stability (--shots > 1).
        metrics_stability = {}
        regression_detail = None

    try:
        await repo.finalize_run(
            run_id,
            status=status,
            metrics=metrics,
            metrics_stability=metrics_stability,
            regression_baseline_sha=None,
            regression_passed=None,
            regression_detail=regression_detail,
        )
    except Exception:
        # finalize_run failure is logged but does not mask the original error.
        # The run row stays in `running` status; an operator can fix it up via
        # SQL if it matters. We choose this over re-raising because the
        # original `error` (if any) is far more diagnostic.
        log.exception("finalize_run failed for run_id=%s", run_id)
```

Add `RunResult` and `EvalShotPersister` to existing imports from `sentinel.evals.runner`:

```python
from sentinel.evals.runner import EvalShotPersister, RunResult, RunnerDeps, run_corpus
```

(Adjust the existing import line; don't duplicate.)

- [ ] **Step 4: Run unit tests**

```bash
pytest tests/unit/evals/ -v
```

Expected: all pass (including the new metadata tests and any prior tests touching `_run_async` indirectly). If any prior test stubs `_StubShotPersister` directly, that's still supported via `--no-persist`.

- [ ] **Step 5: Run typecheck**

```bash
make typecheck
```

Expected: clean.

- [ ] **Step 6: Stage the changes**

```bash
git add sentinel/evals/cli.py
git status
```

---

## Task 4: Integration test for the full lifecycle

**Files:**
- Create: `tests/integration/evals/test_run_lifecycle.py`

This test asserts the success path AND the failure path produce correct `eval_runs` rows. It uses the existing Postgres fixture; check `tests/integration/conftest.py` for `postgres_dsn` / `session_factory` fixture names. If `tests/integration/evals/conftest.py` is still placeholder-only, this test pulls from the parent conftest.

- [ ] **Step 1: Create the test file**

```python
# tests/integration/evals/test_run_lifecycle.py
"""Integration test for PostgresEvalRunRepository lifecycle.

Tests `start_run → persist_shot × N → finalize_run → get_run` against a real
Postgres. Does not boot the FastAPI app or run cassettes — that's the e2e
test (PR 7, #50). This is a narrow lifecycle test for PR 5.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from sentinel.persistence.repositories import (
    EvalCaseResultRecord,
    PostgresEvalRunRepository,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_lifecycle_success(session_factory) -> None:
    """start_run → 2 persist_shot → finalize_run(ok) → get_run returns ok."""
    repo = PostgresEvalRunRepository(session_factory)

    run_id: UUID = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="0" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra={"cassette_mode": "replay", "allow_dirty": False, "subcommand": "run"},
    )
    assert isinstance(run_id, UUID)

    for shot_index in range(2):
        await repo.persist_shot(
            EvalCaseResultRecord(
                run_id=run_id,
                case_id="cf-2019-cpu",
                shot_index=shot_index,
                case_status="ok",
                metrics={
                    "category_match": 1.0,
                    "hypothesis_cosine": 0.85,
                    "action_coverage": 0.75,
                    "evidence_quality": 0.9,
                },
                raw_response=None,
                diagnosis=None,
                incident_id=uuid4(),
                incident_fingerprint="f" * 64,
                incident_title="CPU spike",
                incident_severity="critical",
                token_usage={"input": 1000, "output": 200},
                latency_ms=1200,
                error_detail=None,
            )
        )

    await repo.finalize_run(
        run_id,
        status="ok",
        metrics={
            "category_match": 1.0,
            "hypothesis_cosine": 0.85,
            "action_coverage": 0.75,
            "evidence_quality": 0.9,
        },
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail=None,
    )

    record = await repo.get_run(run_id)
    assert record is not None
    assert record.status == "ok"
    assert record.completed_at is not None


@pytest.mark.asyncio
async def test_lifecycle_failure(session_factory) -> None:
    """start_run → finalize_run(failed) → get_run returns failed with detail."""
    repo = PostgresEvalRunRepository(session_factory)

    run_id = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="0" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra=None,
    )

    await repo.finalize_run(
        run_id,
        status="failed",
        metrics={},
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail={"error": "CassetteMiss(...)"},
    )

    record = await repo.get_run(run_id)
    assert record is not None
    assert record.status == "failed"
    assert record.regression_detail == {"error": "CassetteMiss(...)"}


@pytest.mark.asyncio
async def test_get_latest_ok_run_filters_failed(session_factory) -> None:
    """A failed run is not returned by get_latest_ok_run."""
    repo = PostgresEvalRunRepository(session_factory)

    # Create a failed run.
    failed_id = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="1" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra=None,
    )
    await repo.finalize_run(
        failed_id,
        status="failed",
        metrics={},
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail={"error": "boom"},
    )

    # Create an ok run.
    ok_id = await repo.start_run(
        status="running",
        trigger="local",
        git_sha="2" * 40,
        model="claude-sonnet-4-5",
        prompt_version="v1",
        embedding_model_id="BAAI/bge-small-en-v1.5",
        corpus_version="a" * 64,
        corpus_size=1,
        shots_per_case=1,
        fetcher_fixture_hash="b" * 64,
        extra=None,
    )
    await repo.finalize_run(
        ok_id,
        status="ok",
        metrics={"category_match": 1.0, "hypothesis_cosine": 0.9, "action_coverage": 0.8},
        metrics_stability={},
        regression_baseline_sha=None,
        regression_passed=None,
        regression_detail=None,
    )

    latest = await repo.get_latest_ok_run()
    assert latest is not None
    assert latest.id == ok_id
```

If the `session_factory` fixture name is different (e.g., `db_session_factory` or `async_session_factory`), update the parameter name to match. Check `tests/integration/conftest.py`:

```bash
grep -n "session_factory\|async_session\|@pytest.fixture" tests/integration/conftest.py | head -20
```

- [ ] **Step 2: Run the integration test**

Ensure compose is up:

```bash
make compose-up
make migrate
```

Then:

```bash
pytest tests/integration/evals/test_run_lifecycle.py -v -m integration
```

Expected: 3 PASS.

If `session_factory` fixture is missing, you'll see a fixture-not-found error — adjust the fixture name per Step 1's grep.

- [ ] **Step 3: Stage the changes**

```bash
git add tests/integration/evals/test_run_lifecycle.py
git status
```

---

## Task 5: ADR 0008 — eval-run trigger inference

**Files:**
- Create: `docs/adr/0008-eval-run-trigger-inference.md`

- [ ] **Step 1: Write the ADR**

```markdown
# ADR 0008 — Eval-run trigger inference

Status: Accepted
Date: 2026-05-21

## Context

PR 5 (issue #48) wires `PostgresEvalRunRepository` into the eval CLI. The
`start_run` call requires a `trigger` field whose values are constrained
by a Postgres CHECK constraint (`ck_eval_runs_trigger_valid`) to one of
`local | ci-smoke | ci-nightly | baseline | manual`.

The CLI must derive `trigger` from `args.subcommand` + the environment.

## Decision

`_discover_run_metadata` derives the trigger from this table:

| `args.subcommand` | `CI` env  | `GITHUB_WORKFLOW` env | trigger     |
|-------------------|-----------|-----------------------|-------------|
| `baseline`        | *         | *                     | `baseline`  |
| `run`             | unset/false | *                   | `local`     |
| `run`             | true      | `nightly-evals`       | `ci-nightly`|
| `run`             | true      | `ci`                  | `ci-smoke`  |
| `run`             | true      | other                 | `manual`    |

`baseline` wins over CI signals: a baseline ran inside CI is still a baseline
operation, not a regression-gate trigger.

`manual` covers `workflow_dispatch` and any future named workflow that hasn't
been mapped yet. Surfacing as `manual` rather than failing prevents an
unexpected workflow name from breaking eval runs.

## Why `ci-smoke` for the PR-gate trigger

PR 4a renamed the CI job from `evals-smoke` to `evals-gate` and switched it from
a 5-case smoke to the full 10-case corpus. The persisted **value** stays as
`ci-smoke` because:

1. The Postgres CHECK constraint is part of migration 0006. Adding `ci-pr` (or
   renaming `ci-smoke` → `ci-pr`) requires a new migration with both
   `upgrade` and `downgrade`, plus a Protocol Literal change in
   `sentinel/persistence/repositories.py`. That's not load-bearing for this
   PR — the value is opaque to consumers (the regression gate filters on
   `regression_passed`, not `trigger`).
2. The trigger column is already prefixed with `ci-` to disambiguate from
   `local` and `baseline` runs; the suffix is incidental.

The CI job's *name* is the operator-visible label; the persisted *value* is
internal. If/when a downstream consumer needs to filter on "PR gate runs vs
other CI runs," adding `ci-pr` becomes load-bearing and we migrate then.

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
- `make readme-numbers` (when it lands) reads `get_latest_ok_run(trigger='baseline')`
  for the headline numbers, falling through to `get_latest_ok_run()` if no
  baseline exists yet.
- A future migration can add `ci-pr` (or rename `ci-smoke`) cleanly: both
  upgrade and downgrade paths can rewrite the column in place because the
  CHECK constraint is the only consumer.

## References

- Issue #48
- `plans/2026-05-21-evals-pr5-7-design.md` (PR 5 section)
- `migrations/versions/0006_eval_runs_and_case_results.py` (CHECK constraint)
- `sentinel/persistence/repositories.py:1060-1076` (Protocol)
```

- [ ] **Step 2: Stage the ADR**

```bash
git add docs/adr/0008-eval-run-trigger-inference.md
git status
```

---

## Task 6: Manual verification, code review, commit

**Files:** none

- [ ] **Step 1: Manual smoke test**

```bash
make compose-up
make migrate
make evals 2>&1 | tail -10
```

Expected output ends with something like:

```
eval run starting: run_id=<uuid> cases=10 shots=1 trigger=local
...
eval run complete: run_id=<uuid>
  json: evals/results/<uuid>.json
  md:   evals/results/<uuid>.md
```

Then verify the DB row:

```bash
docker compose exec -T postgres psql -U sentinel -d sentinel -c \
  "select id, status, trigger, git_sha, corpus_size from eval_runs order by started_at desc limit 1;"
```

Expected: one row, `status=ok`, `trigger=local`, your current commit SHA, `corpus_size=10`.

- [ ] **Step 2: Verify `--no-persist` fallback**

```bash
.venv/bin/python -m sentinel.evals run --no-persist 2>&1 | head -3
```

Expected first line contains `[--no-persist: in-memory only]`.

Then verify no new `eval_runs` row was added:

```bash
docker compose exec -T postgres psql -U sentinel -d sentinel -c \
  "select count(*) from eval_runs;"
```

Expected: same count as Step 1.

- [ ] **Step 3: Run full test suite**

```bash
make lint
make typecheck
make test
make test-integration
```

Expected: all green.

- [ ] **Step 4: Subagent code review (mandatory per project memory)**

Dispatch a code review subagent via the `superpowers:requesting-code-review` skill against the staged changes. Provide the spec link and the change diff. Address any blocking findings before commit.

```bash
git diff --staged | wc -l  # sanity check size
```

- [ ] **Step 5: Stage and stop (per user memory `feedback-commits-by-user.md`)**

```bash
git status
```

Confirm the staged set contains:
- `sentinel/evals/cli.py`
- `tests/unit/evals/test_cli.py`
- `tests/integration/evals/test_run_lifecycle.py`
- `docs/adr/0008-eval-run-trigger-inference.md`

Do not commit. Wait for user to delegate the commit. When delegated, use the user's git identity and add the Claude Code AI co-author trailer:

```bash
git commit -m "$(cat <<'EOF'
feat(evals): wire PostgresEvalRunRepository lifecycle (#48)

Replace _StubShotPersister with a real start_run/persist_shot/finalize_run
flow. Adds --allow-dirty and --no-persist flags, ADR 0008 captures
trigger-inference table and the ci-smoke legacy-naming decision.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 6: Push and open PR (on user delegation)**

```bash
git push -u origin <branch-name>
gh pr create --title "feat(evals): wire PostgresEvalRunRepository lifecycle (#48)" --body "$(cat <<'EOF'
## Summary
- Replace `_StubShotPersister` with `PostgresEvalRunRepository` + full `start_run`/`finalize_run` lifecycle
- Add `_discover_run_metadata` helper (trigger inference, git_sha, corpus/fixture hashes)
- Add `--allow-dirty` and `--no-persist` CLI flags
- ADR 0008 captures trigger inference table + `ci-smoke` legacy-naming rationale

Closes #48.

## Test plan
- [ ] `make lint && make typecheck && make test && make test-integration` green
- [ ] `make evals` writes a row to `eval_runs` and 10 rows to `eval_case_results`
- [ ] `make evals` with `--no-persist` writes nothing to DB
- [ ] CI `evals-gate` job passes against existing baseline

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review checklist (executor: please confirm before claiming done)

- [ ] Spec PR 5 section's "What changes" matches Tasks 1–3 file-by-file.
- [ ] Spec PR 5 section's "Failure modes" table maps to: dirty-tree (Task 2), `start_run` raises (propagated in Task 3), `run_corpus` raises (try/finally in Task 3), `finalize_run` raises (Task 3 helper catches + logs).
- [ ] Spec PR 5 section's "Acceptance" criteria → Task 6 Step 1 (`make evals` row), Task 6 Step 2 (`--no-persist`), Task 2 (unit tests), Task 4 (integration test), Task 5 (ADR).
- [ ] No placeholder text ("TBD", "implement later") in any task.
- [ ] All test code shown inline, not described.
- [ ] All command lines have expected output stated.
- [ ] Trigger inference matches the spec table exactly (5 rows).
