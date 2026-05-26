"""Unit tests for ``sentinel.evals.cli``.

Coverage:

1. ``test_run_requires_eval_mode_true`` — ``run`` with ``SENTINEL_EVAL_MODE``
   unset exits 1 with a clear stderr message.
2. ``record`` guards (three tests; happy path is exercised in Task 3 against
   the live API, not here):
   * ``test_record_requires_eval_mode`` — eval_mode=False → exit 1.
   * ``test_record_requires_cassette_dir`` — no cassette dir → exit 1.
   * ``test_record_requires_api_key`` — no ANTHROPIC_API_KEY → exit 1.
3. ``test_baseline_subcommand_is_a_stub``, ``test_compare_to_baseline_is_a_stub``
   — both still PR-3c-deferred stubs.
4. ``test_readme_patches_between_markers`` — patches a temp README between the
   ``<!-- evals:start --> .. <!-- evals:end -->`` markers using a fake result
   MD file written into the fallback ``--results-dir``.

The tests deliberately avoid touching the FastAPI app or Postgres — those are
covered by the integration test in Task 7. argparse's ``parse_args`` accepts
a ``list[str]``; the CLI's ``main()`` calls ``sys.exit(...)`` so each test
asserts on the ``SystemExit.code``.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sentinel.evals import cli

# --- Helpers --------------------------------------------------------------- #


def _run_main(argv: list[str]) -> int:
    """Invoke cli.main(argv); return the SystemExit code as int."""
    with pytest.raises(SystemExit) as exc_info:
        cli.main(argv)
    code = exc_info.value.code
    # SystemExit.code may be int | None | str — narrow for the assertions.
    assert isinstance(code, int), f"expected int exit code, got {code!r}"
    return code


# --- Tests ----------------------------------------------------------------- #


def test_run_requires_eval_mode_true(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """`run` fails fast with a clear stderr message when SENTINEL_EVAL_MODE is
    not set (or set to false)."""
    # Provide the bare-minimum settings the BaseSettings root validator needs
    # so load_settings() doesn't fail for unrelated reasons; eval_mode stays
    # at its default (False).
    monkeypatch.delenv("SENTINEL_EVAL_MODE", raising=False)
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://localhost/test")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "localhost:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("SENTINEL_ENV", "test")

    code = _run_main(["run", "--corpus", str(tmp_path)])
    assert code == 1

    captured = capsys.readouterr()
    assert "SENTINEL_EVAL_MODE" in captured.err
    assert "eval_mode" in captured.err.lower() or "true" in captured.err.lower()


def _set_baseline_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    eval_mode: bool,
    eval_corpus_dir: Path | None,
    api_key_present: bool,
    cassette_dir: Path | None = None,
) -> None:
    """Common env setup for record guard tests — Settings needs these to
    validate. The API key here is the one used by the *Anthropic client* (a
    SecretStr required by Settings); the separate ANTHROPIC_API_KEY env var
    is what the record subcommand checks for live-API access."""
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://localhost/test")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "localhost:9092")
    # Settings requires SENTINEL_ANTHROPIC_API_KEY; the record subcommand
    # checks ANTHROPIC_API_KEY (or SENTINEL_ANTHROPIC_API_KEY) separately
    # as a "live API access available?" gate. Set the Settings field
    # unconditionally so load_settings() doesn't fail for unrelated reasons.
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "test-settings-key")
    monkeypatch.setenv("SENTINEL_ENV", "test")
    monkeypatch.setenv("SENTINEL_EVAL_MODE", "true" if eval_mode else "false")
    if eval_corpus_dir is not None:
        monkeypatch.setenv("SENTINEL_EVAL_CORPUS_DIR", str(eval_corpus_dir))
    else:
        monkeypatch.delenv("SENTINEL_EVAL_CORPUS_DIR", raising=False)
    if cassette_dir is not None:
        monkeypatch.setenv("SENTINEL_EVAL_CASSETTE_DIR", str(cassette_dir))
    else:
        monkeypatch.delenv("SENTINEL_EVAL_CASSETTE_DIR", raising=False)
    # Ensure no stale cassette mode env from a prior test in the same process.
    monkeypatch.delenv("SENTINEL_EVAL_CASSETTE_MODE", raising=False)
    if api_key_present:
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-live-test")
    else:
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        # SENTINEL_ANTHROPIC_API_KEY is also accepted by the record guard,
        # so we'd have to clear it to test the "missing" branch — but the
        # Settings load needs it. Tests that exercise the api-key guard
        # are responsible for clearing this in-test after _set_baseline_env.


def test_record_requires_eval_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """`record` exits 1 with a clear stderr message when eval_mode is off."""
    _set_baseline_env(
        monkeypatch,
        eval_mode=False,
        eval_corpus_dir=tmp_path,
        api_key_present=True,
        cassette_dir=tmp_path,
    )

    code = _run_main(["record"])
    assert code == 1

    captured = capsys.readouterr()
    assert "SENTINEL_EVAL_MODE" in captured.err


def test_record_requires_cassette_dir(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """`record` exits 1 when neither --cassette-dir nor SENTINEL_EVAL_CASSETTE_DIR is set."""
    _set_baseline_env(
        monkeypatch,
        eval_mode=True,
        eval_corpus_dir=tmp_path,
        api_key_present=True,
        cassette_dir=None,
    )

    code = _run_main(["record"])
    assert code == 1

    captured = capsys.readouterr()
    assert "cassette" in captured.err.lower()


def test_record_requires_api_key(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    """`record` exits 1 when neither ANTHROPIC_API_KEY nor SENTINEL_ANTHROPIC_API_KEY is set.

    The record api-key guard accepts either env var (SENTINEL_ANTHROPIC_API_KEY
    is the Settings field; ANTHROPIC_API_KEY is the conventional name). Both
    must be absent for the guard to fire. This guard runs *before*
    load_settings(), so the Settings-required SENTINEL_ANTHROPIC_API_KEY
    being missing doesn't bypass the message — the record-specific error
    surfaces first.
    """
    _set_baseline_env(
        monkeypatch,
        eval_mode=True,
        eval_corpus_dir=tmp_path,
        api_key_present=False,
        cassette_dir=tmp_path,
    )
    # Clear the Settings-side env var too so the "neither is set" branch
    # actually fires. The record guard runs before load_settings(), so
    # there's no chicken-and-egg with the required Settings field here.
    monkeypatch.delenv("SENTINEL_ANTHROPIC_API_KEY", raising=False)

    code = _run_main(["record"])
    assert code == 1

    captured = capsys.readouterr()
    # The record-specific message includes "ANTHROPIC_API_KEY" and "record mode".
    assert "ANTHROPIC_API_KEY" in captured.err
    assert "record mode" in captured.err.lower()


def test_parser_record_defaults_no_persist_true() -> None:
    """record subcommand must default no_persist=True (record never opens eval_runs rows)."""
    parser = cli._build_parser()
    args = parser.parse_args(["record", "--corpus", "corpus"])
    assert args.no_persist is True


# --- compare-to-baseline (paired-bootstrap regression gate) ----------------- #


def _run_blob(per_case_metrics: dict[str, dict[str, float | None]]) -> dict[str, object]:
    """Build the minimum shape `_compute_regression_verdict` reads from a run
    JSON blob — `per_case` list of `{case_id, metrics}`. The full report blob
    is richer (aggregate_metrics, headline, stability) but the compare path
    only touches `per_case`.
    """
    return {
        "per_case": [{"case_id": cid, "metrics": m} for cid, m in per_case_metrics.items()],
    }


def test_compare_to_baseline_returns_zero_when_no_regression(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Identical baseline + run → no regression, exit 0, markdown shows ✅."""
    import json as _json

    baseline = _run_blob(
        {
            "case-a": {
                "category_match": 1.0,
                "hypothesis_cosine": 0.8,
                "action_coverage": 0.7,
                "evidence_quality": 1.0,
            },
            "case-b": {
                "category_match": 1.0,
                "hypothesis_cosine": 0.85,
                "action_coverage": 0.75,
                "evidence_quality": 1.0,
            },
        }
    )
    baseline_path = tmp_path / "main.json"
    baseline_path.write_text(_json.dumps(baseline))

    run_path = tmp_path / "run.json"
    run_path.write_text(_json.dumps(baseline))  # identical

    code = _run_main(
        [
            "compare-to-baseline",
            "--baseline",
            str(baseline_path),
            "--run-json",
            str(run_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 0, captured.err
    assert "no regression" in captured.out
    assert "✅" in captured.out


def test_compare_to_baseline_detects_regression_and_exits_nonzero(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Large negative diff across every case for one metric → regression
    detected, exit 1, markdown lists the regressed metric."""
    import json as _json

    # Baseline: 1.0 on every metric for 10 cases
    baseline = _run_blob(
        {
            f"case-{i}": {
                "category_match": 1.0,
                "hypothesis_cosine": 0.85,
                "action_coverage": 0.75,
                "evidence_quality": 1.0,
            }
            for i in range(10)
        }
    )
    # Run: action_coverage drops by 0.30 on every case — clear regression.
    run = _run_blob(
        {
            f"case-{i}": {
                "category_match": 1.0,
                "hypothesis_cosine": 0.85,
                "action_coverage": 0.45,
                "evidence_quality": 1.0,
            }
            for i in range(10)
        }
    )

    baseline_path = tmp_path / "main.json"
    baseline_path.write_text(_json.dumps(baseline))
    run_path = tmp_path / "run.json"
    run_path.write_text(_json.dumps(run))

    code = _run_main(
        [
            "compare-to-baseline",
            "--baseline",
            str(baseline_path),
            "--run-json",
            str(run_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1, captured.out
    assert "REGRESSION" in captured.out
    assert "action_coverage" in captured.out


def test_compare_to_baseline_writes_markdown_to_output_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When `--output` is given, the markdown report is written to that path
    AND printed to stdout (so CI can both surface and archive it)."""
    import json as _json

    blob = _run_blob({"case-a": {"category_match": 1.0}})
    baseline_path = tmp_path / "main.json"
    baseline_path.write_text(_json.dumps(blob))
    run_path = tmp_path / "run.json"
    run_path.write_text(_json.dumps(blob))
    output_path = tmp_path / "report.md"

    code = _run_main(
        [
            "compare-to-baseline",
            "--baseline",
            str(baseline_path),
            "--run-json",
            str(run_path),
            "--output",
            str(output_path),
        ]
    )
    assert code == 0
    assert output_path.exists()
    assert "regression gate" in output_path.read_text().lower()
    captured = capsys.readouterr()
    assert "regression gate" in captured.out.lower()


def test_compare_to_baseline_errors_when_baseline_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Missing baseline → exit 1, error message names the missing path."""
    code = _run_main(
        [
            "compare-to-baseline",
            "--baseline",
            str(tmp_path / "does-not-exist.json"),
            "--run-json",
            str(tmp_path / "run.json"),
        ]
    )
    assert code == 1
    captured = capsys.readouterr()
    assert "baseline not found" in captured.err
    assert "does-not-exist.json" in captured.err


def test_compare_to_baseline_warns_loudly_on_unpaired_cases(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cases in the baseline but missing from the run must be surfaced — silent
    drop is how a PR that deleted/renamed a corpus case could skate past the
    gate. The markdown report lists them under a ⚠️ bullet."""
    import json as _json

    baseline = _run_blob(
        {
            "case-a": {"category_match": 1.0},
            "case-removed": {"category_match": 1.0},  # missing from run
        }
    )
    run = _run_blob(
        {
            "case-a": {"category_match": 1.0},
            "case-new": {"category_match": 1.0},  # new in run
        }
    )

    baseline_path = tmp_path / "main.json"
    baseline_path.write_text(_json.dumps(baseline))
    run_path = tmp_path / "run.json"
    run_path.write_text(_json.dumps(run))

    code = _run_main(
        [
            "compare-to-baseline",
            "--baseline",
            str(baseline_path),
            "--run-json",
            str(run_path),
        ]
    )
    captured = capsys.readouterr()
    # paired set is just {case-a} — single case, no regression on that one
    assert code == 0, captured.err
    # Loud surface of the dropped case
    assert "case-removed" in captured.out
    assert "missing from run" in captured.out
    # And the new-in-run case is mentioned (informational)
    assert "case-new" in captured.out
    assert "new cases in run not in baseline" in captured.out


def test_compare_to_baseline_surfaces_baseline_metadata_in_header(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The markdown header includes the baseline's prompt_version + model_id +
    git_sha so a reviewer can spot apples-to-oranges comparisons (e.g. the
    PR bumped the prompt but the baseline is stale)."""
    import json as _json

    baseline = {
        "prompt_version": "v1.2",
        "model_id": "claude-sonnet-4-5",
        "git_sha": "abc1234deadbeef",
        "recorded_at": "2026-05-20T12:34:56+00:00",
        **_run_blob({"case-a": {"category_match": 1.0}}),
    }
    run = _run_blob({"case-a": {"category_match": 1.0}})

    baseline_path = tmp_path / "main.json"
    baseline_path.write_text(_json.dumps(baseline))
    run_path = tmp_path / "run.json"
    run_path.write_text(_json.dumps(run))

    code = _run_main(
        [
            "compare-to-baseline",
            "--baseline",
            str(baseline_path),
            "--run-json",
            str(run_path),
        ]
    )
    assert code == 0
    captured = capsys.readouterr()
    assert "prompt_version=v1.2" in captured.out
    assert "model_id=claude-sonnet-4-5" in captured.out
    assert "git_sha=abc1234deadbeef" in captured.out


def test_readme_patches_between_markers(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`readme` replaces the content between the markers with a summary lifted
    from the most recent <run_id>.md in --results-dir.
    """
    readme = tmp_path / "README.md"
    readme.write_text(
        "# Test Project\n"
        "\n"
        "## Eval results\n"
        "\n"
        f"{cli.README_MARKER_START}\n"
        "*Pending PR 3c — first real corpus run lands the numbers here.*\n"
        f"{cli.README_MARKER_END}\n"
        "\n"
        "## License\n"
    )

    results_dir = tmp_path / "results"
    results_dir.mkdir()
    fake_run_md = results_dir / "00000000-0000-0000-0000-000000000001.md"
    fake_run_md.write_text(
        "# Sentinel Eval Results — 00000000-0000-0000-0000-000000000001\n"
        "\n"
        "**Cases:** 5\n"
        "\n"
        "## Aggregate Metrics\n"
        "\n"
        "| Metric | Value |\n"
        "|---|---|\n"
        "| category_match | 0.90 |\n"
        "\n"
        "## Headline\n"
        "\n"
        "- pass_rate_strict: 70.0%\n"
        "- mean_stability: 0.080\n"
    )

    code = _run_main(
        [
            "readme",
            "--readme",
            str(readme),
            "--results-dir",
            str(results_dir),
        ]
    )
    assert code == 0

    new_text = readme.read_text()
    # The markers must still be present (anchor for the next patch).
    assert cli.README_MARKER_START in new_text
    assert cli.README_MARKER_END in new_text
    # Pre-patch content is gone, post-patch content is present.
    assert "Pending PR 3c" not in new_text
    assert "pass_rate_strict: 70.0%" in new_text
    # Bookend content (outside markers) is preserved.
    assert "# Test Project" in new_text
    assert "## License" in new_text

    captured = capsys.readouterr()
    assert "patched" in captured.out.lower()


def test_readme_errors_when_markers_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`readme` raises (and exits non-zero) if the markers aren't in the README.

    Defensive: the patcher refuses to silently append — operators get a clear
    fail-loud signal that the README scaffold is wrong.
    """
    readme = tmp_path / "README.md"
    readme.write_text("# No markers here\n")

    results_dir = tmp_path / "results"
    results_dir.mkdir()
    (results_dir / "fake.md").write_text("## Headline\n\n- pass_rate_strict: 0.0%\n")

    with pytest.raises((SystemExit, ValueError)):
        cli.main(
            [
                "readme",
                "--readme",
                str(readme),
                "--results-dir",
                str(results_dir),
            ]
        )


# --- Parser flags (--allow-dirty, --no-persist) ------------------------------ #


def test_parser_accepts_allow_dirty() -> None:
    """Parser accepts --allow-dirty flag and sets it to True."""
    parser = cli._build_parser()
    args = parser.parse_args(["--allow-dirty", "run"])
    assert args.allow_dirty is True


def test_parser_allow_dirty_default_false() -> None:
    """--allow-dirty defaults to False when not provided."""
    parser = cli._build_parser()
    args = parser.parse_args(["run"])
    assert args.allow_dirty is False


def test_parser_accepts_no_persist_on_run() -> None:
    """Parser accepts --no-persist flag on run subcommand."""
    parser = cli._build_parser()
    args = parser.parse_args(["run", "--no-persist"])
    assert args.no_persist is True


def test_parser_no_persist_default_false() -> None:
    """--no-persist defaults to False when not provided."""
    parser = cli._build_parser()
    args = parser.parse_args(["run"])
    assert args.no_persist is False


def test_parser_accepts_no_persist_on_baseline() -> None:
    """Parser accepts --no-persist flag on baseline subcommand."""
    parser = cli._build_parser()
    args = parser.parse_args(["baseline", "--no-persist"])
    assert args.no_persist is True


def test_parser_allow_dirty_works_with_baseline() -> None:
    """--allow-dirty flag works with baseline subcommand."""
    parser = cli._build_parser()
    args = parser.parse_args(["--allow-dirty", "baseline"])
    assert args.allow_dirty is True


# --- _discover_run_metadata tests ------------------------------------------- #


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
            return subprocess.CompletedProcess(cmd, 0, stdout=sha + "\n", stderr="")
        if cmd[:3] == ["git", "status", "--porcelain"]:
            stdout = b" M sentinel/evals/cli.py\n" if dirty else b""
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr=b"")
        raise AssertionError(f"unexpected subprocess.run call: {cmd!r}")

    # Patch the name as used inside sentinel.evals.cli so a future
    # `from subprocess import run` refactor won't silently break the mock.
    monkeypatch.setattr("sentinel.evals.cli.subprocess.run", fake_run)


def _write_corpus_fixture(tmp_path: Path) -> tuple[Path, list[Any]]:
    """Write a 1-case corpus YAML to tmp_path and return (corpus_dir, cases)."""
    from sentinel.evals.corpus_loader import load_corpus_dir

    yaml_text = (
        "id: cf-2019-cpu\n"
        "corpus_version: 1\n"
        "source_url: https://example.com/postmortem\n"
        "sources_consulted:\n"
        "  - https://example.com/postmortem\n"
        "alert:\n"
        "  source: generic\n"
        "  service: api\n"
        "  severity: SEV1\n"
        "  title: 'CPU spike'\n"
        "  timestamp: '2019-07-02T00:00:00Z'\n"
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


def test_discover_metadata_trigger_local(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_WORKFLOW", raising=False)
    _patch_git(monkeypatch, sha="abc123" * 6 + "abcd", dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw["trigger"] == "local"
    assert kw["git_sha"] == "abc123" * 6 + "abcd"
    assert kw["status"] == "running"


def test_discover_metadata_trigger_baseline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "nightly-evals")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="baseline", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw["trigger"] == "baseline"  # baseline subcommand wins over CI signals


def test_discover_metadata_trigger_ci_nightly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "nightly-evals")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=True)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw["trigger"] == "ci-nightly"


def test_discover_metadata_trigger_ci_smoke(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "ci")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw["trigger"] == "ci-smoke"  # legacy name preserved


def test_discover_metadata_trigger_manual_workflow_dispatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_WORKFLOW", "manual-replay")
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw["trigger"] == "manual"


def test_discover_metadata_dirty_tree_refused(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=True)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    with pytest.raises(SystemExit) as exc:
        _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "dirty" in err.lower()


def test_discover_metadata_dirty_tree_allowed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=True)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=True, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw["extra"]["allow_dirty"] is True


def test_discover_metadata_corpus_version_deterministic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw1 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]
    kw2 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw1["corpus_version"] == kw2["corpus_version"]
    assert len(kw1["corpus_version"]) == 64  # sha256 hex


def test_discover_metadata_corpus_version_includes_yml_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """_hash_corpus_files must glob both *.yaml and *.yml, mirroring corpus_loader."""
    from sentinel.evals.cli import _hash_corpus_files

    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()

    # Write one .yaml and one .yml file — both must be included in the hash.
    (corpus_dir / "case-a.yaml").write_bytes(b"yaml content a")
    (corpus_dir / "case-b.yml").write_bytes(b"yml content b")

    hash_both = _hash_corpus_files(corpus_dir)

    # Hash of only the .yaml file should differ (proves .yml is being hashed).
    hash_yaml_only = hashlib.sha256(b"yaml content a").hexdigest()
    assert (
        hash_both != hash_yaml_only
    ), "_hash_corpus_files ignored *.yml files — corpus_version would be wrong"
    assert len(hash_both) == 64  # sha256 hex


def test_discover_metadata_fetcher_fixture_hash_deterministic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw1 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]
    kw2 = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw1["fetcher_fixture_hash"] == kw2["fetcher_fixture_hash"]
    assert len(kw1["fetcher_fixture_hash"]) == 64


def test_discover_metadata_includes_settings_fields(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sentinel.evals.cli import _discover_run_metadata

    monkeypatch.delenv("CI", raising=False)
    _patch_git(monkeypatch, sha="0" * 40, dirty=False)
    corpus_dir, cases = _write_corpus_fixture(tmp_path)

    args = SimpleNamespace(subcommand="run", allow_dirty=False, live=False)
    kw = _discover_run_metadata(args, _fake_settings(), corpus_dir, cases)  # type: ignore[arg-type]

    assert kw["model"] == "claude-sonnet-4-5"
    assert kw["prompt_version"] == "v1"
    assert kw["embedding_model_id"] == "BAAI/bge-small-en-v1.5"
    assert kw["corpus_size"] == 1
    assert "shots_per_case" in kw
    assert kw["shots_per_case"] == 1
