"""Unit tests for ``sentinel.evals.cli``.

Four tests per the PR 3b plan §Task 6:

1. ``test_run_requires_eval_mode_true`` — invoking ``run`` with
   ``SENTINEL_EVAL_MODE=false`` exits 1 with a clear stderr message.
2. ``test_record_subcommand_is_a_stub`` — exits 1 with the stub message.
3. ``test_baseline_subcommand_is_a_stub`` — same.
4. ``test_readme_patches_between_markers`` — patches a temp README between the
   ``<!-- evals:start --> .. <!-- evals:end -->`` markers using a fake result
   MD file written into the fallback ``--results-dir``.

The tests deliberately avoid touching the FastAPI app or Postgres — those are
covered by the integration test in Task 7. argparse's ``parse_args`` accepts
a ``list[str]``; the CLI's ``main()`` calls ``sys.exit(...)`` so each test
asserts on the ``SystemExit.code``.
"""

from __future__ import annotations

from pathlib import Path

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


def test_record_subcommand_is_a_stub(capsys: pytest.CaptureFixture[str]) -> None:
    """`record` exits 1 with a clear "not implemented in PR 3b" message."""
    code = _run_main(["record"])
    assert code == 1

    captured = capsys.readouterr()
    assert "PR 3" in captured.err
    assert "record" in captured.err.lower()


def test_baseline_subcommand_is_a_stub(capsys: pytest.CaptureFixture[str]) -> None:
    """`baseline` exits 1 with a "not implemented in PR 3b" message."""
    code = _run_main(["baseline"])
    assert code == 1

    captured = capsys.readouterr()
    assert "PR 3" in captured.err
    assert "baseline" in captured.err.lower()


def test_compare_to_baseline_is_a_stub(capsys: pytest.CaptureFixture[str]) -> None:
    """`compare-to-baseline` is also a PR 3c-deferred stub."""
    code = _run_main(["compare-to-baseline"])
    assert code == 1

    captured = capsys.readouterr()
    assert "PR 3" in captured.err


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
