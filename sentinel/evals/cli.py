"""Command-line entrypoint for the eval harness.

Subcommands (argparse):

  * ``run`` — boots an in-process FastAPI app via ``build_app()``, drives
    requests through ``httpx.AsyncClient(transport=ASGITransport(...))``,
    polls Postgres for the resulting diagnoses, scores each shot against the
    corpus ground truth, and writes a JSON + Markdown report under
    ``evals/results/<run_id>.{json,md}``.
  * ``record`` — drives the corpus end-to-end against the live Anthropic API,
    writing the captured HTTP exchanges into ``--cassette-dir`` as one JSON
    file per (case, shot). Requires ``ANTHROPIC_API_KEY``.
  * ``baseline`` — delegates to ``run``, then transforms the JSON report into
    ``evals/baselines/<name>.json`` (adds git_sha + prompt_version +
    model_id + recorded_at metadata so the regression gate can refuse to
    compare across incompatible runs).
  * ``readme`` — patches ``README.md`` between ``<!-- evals:start -->`` /
    ``<!-- evals:end -->`` markers with the headline metrics from the most
    recent ``evals/results/*.md`` report.
  * ``compare-to-baseline`` — runs the paired-bootstrap regression gate
    (per-metric, vs ``evals/baselines/<name>.json``) and exits non-zero on
    regression. Prints a markdown table for PR comments; optionally writes
    it to ``--output``.

Operational caveat repeated in the ``run`` help text: the runner shares the
compose data tier (Postgres + Kafka + Redis) with the compose ``app`` container
but boots its own FastAPI process. The compose ``app`` container must be
stopped first (or run with a different consumer group) to avoid Kafka
partition contention.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import re
import sys
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from pydantic import SecretStr

from sentinel.config.settings import Settings, load_settings
from sentinel.evals.cassette import CassetteMiss
from sentinel.evals.corpus_loader import load_corpus_dir
from sentinel.evals.report import write_report
from sentinel.evals.runner import EvalCaseResultRecord, RunnerDeps, run_corpus
from sentinel.memory.embeddings import FastEmbedProvider

if TYPE_CHECKING:
    from fastapi import FastAPI

    from sentinel.evals.schema import RegressionVerdict

log = logging.getLogger(__name__)

# README marker constants — kept module-level so tests can import them.
README_MARKER_START = "<!-- evals:start -->"
README_MARKER_END = "<!-- evals:end -->"


# --- Entry point ---------------------------------------------------------- #


def main(argv: Sequence[str] | None = None) -> None:
    """argparse entry point. Drives via ``sys.exit(...)`` — returns None."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(2)

    sys.exit(args.func(args))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m sentinel.evals",
        description="Sentinel eval harness CLI.",
    )
    sub = parser.add_subparsers(dest="subcommand")

    # run -------------------------------------------------------------------
    run_p = sub.add_parser(
        "run",
        help="run the eval corpus end-to-end (cassette replay mode)",
        description=(
            "Boots an in-process FastAPI app and drives the corpus through it. "
            "REQUIRES SENTINEL_EVAL_MODE=true and SENTINEL_EVAL_CORPUS_DIR set. "
            "Stop the compose 'app' container first — it shares the same Kafka "
            "consumer group as the in-process runner."
        ),
    )
    run_p.add_argument("--corpus", type=Path, default=None, help="corpus directory (YAML files)")
    run_p.add_argument(
        "--shots",
        type=int,
        default=1,
        help=(
            "shots per case (default: 1). Multi-shot is structurally limited by "
            "the uq_diagnoses_incident_prompt_model uniqueness constraint — "
            "shots 2+ silently collapse onto shot 0's persisted diagnosis. Set "
            ">1 only if you've also removed the constraint or arranged for "
            "per-shot fingerprint divergence."
        ),
    )
    run_p.add_argument(
        "--cassette-dir",
        type=Path,
        default=None,
        help="cassette replay directory; overrides SENTINEL_EVAL_CASSETTE_DIR",
    )
    run_p.add_argument(
        "--smoke", action="store_true", help="run only the first 5 cases (sorted by id)"
    )
    run_p.add_argument(
        "--live",
        action="store_true",
        help=(
            "Opt-in flag to run against the live Anthropic API (no cassette replay). "
            "Required when neither --cassette-dir nor SENTINEL_EVAL_CASSETTE_DIR is set; "
            "guards against accidentally burning API budget from a typo'd env var."
        ),
    )
    run_p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("evals/results"),
        help="where to write the JSON + MD report",
    )
    run_p.set_defaults(func=_cmd_run)

    # record ----------------------------------------------------------------
    # Mirrors `run` orchestration but flips the cassette transport into
    # record mode (live Anthropic API → write cassette JSON to disk).
    # Cassette dir is REQUIRED (no --live fallback — recording requires a
    # target directory) and ANTHROPIC_API_KEY must be set.
    rec_p = sub.add_parser(
        "record",
        help="record live cassettes from the corpus (writes to cassette dir)",
        description=(
            "Drives the corpus end-to-end against the live Anthropic API, "
            "writing the captured HTTP exchanges into --cassette-dir as one "
            "JSON file per (case, shot). REQUIRES SENTINEL_EVAL_MODE=true, "
            "SENTINEL_EVAL_CORPUS_DIR, a cassette directory, and "
            "ANTHROPIC_API_KEY (the inner transport hits the live API). "
            "Stop the compose 'app' container first."
        ),
    )
    rec_p.add_argument("--corpus", type=Path, default=None, help="corpus directory (YAML files)")
    # Record shots default to 1 to match the run default. Multi-shot record
    # is wasteful given the same uq_diagnoses_incident_prompt_model collapse —
    # only one shot's response actually drives scoring.
    rec_p.add_argument("--shots", type=int, default=1, help="shots per case (default: 1)")
    rec_p.add_argument(
        "--cassette-dir",
        type=Path,
        default=None,
        help="cassette write directory; overrides SENTINEL_EVAL_CASSETTE_DIR",
    )
    rec_p.add_argument(
        "--smoke", action="store_true", help="record only the first 5 cases (sorted by id)"
    )
    rec_p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("evals/results"),
        help="where to write the JSON + MD report (record runs also emit a report)",
    )
    rec_p.set_defaults(func=_cmd_record)

    # baseline --------------------------------------------------------------
    # Runs the corpus through the same pipeline as `run`, then writes a
    # baseline JSON to `evals/baselines/{name}.json` for the regression
    # gate to compare future runs against. Replay-only (cassette dir
    # required) so regenerating a baseline is deterministic and free.
    base_p = sub.add_parser(
        "baseline",
        help="record a baseline corpus run (writes evals/baselines/{name}.json)",
    )
    base_p.add_argument("--corpus", type=Path, default=None, help="corpus directory")
    base_p.add_argument(
        "--cassette-dir",
        type=Path,
        default=None,
        help="cassette replay directory; overrides SENTINEL_EVAL_CASSETTE_DIR",
    )
    base_p.add_argument(
        "--shots",
        type=int,
        default=1,
        help=(
            "shots per case (default: 1; see `run --shots` for the multi-shot "
            "structural limitation tracked in issue #49)"
        ),
    )
    base_p.add_argument(
        "--smoke", action="store_true", help="baseline only the first 5 cases (sorted by id)"
    )
    base_p.add_argument(
        "--name",
        type=str,
        default="main",
        help="baseline name (file: <baseline-dir>/<name>.json); default: main",
    )
    base_p.add_argument(
        "--baseline-dir",
        type=Path,
        default=Path("evals/baselines"),
        help="where to write the baseline JSON; default: evals/baselines",
    )
    base_p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("evals/results"),
        help="intermediate report dir for the underlying run; default: evals/results",
    )
    base_p.add_argument(
        "--live",
        action="store_true",
        help="opt-in to live API calls (default: cassette replay only)",
    )
    base_p.set_defaults(func=_cmd_baseline)

    # readme ----------------------------------------------------------------
    readme_p = sub.add_parser(
        "readme",
        help="patch README.md between <!-- evals:start --> markers with latest run numbers",
    )
    readme_p.add_argument(
        "--results-dir",
        type=Path,
        default=Path("evals/results"),
        help="directory holding <run_id>.md files (used as fallback until PR 1 lands)",
    )
    readme_p.add_argument(
        "--readme",
        type=Path,
        default=Path("README.md"),
        help="path to README.md (default: ./README.md)",
    )
    readme_p.set_defaults(func=_cmd_readme)

    # compare-to-baseline ---------------------------------------------------
    # Reads a baseline JSON + a run result JSON, runs the paired-bootstrap
    # regression gate per metric, prints a markdown table, exits non-zero
    # iff any metric regressed (CI excludes zero AND mean diff worse than
    # the practical floor).
    cmp_p = sub.add_parser(
        "compare-to-baseline",
        help="compare a run's metrics to a baseline file (paired-bootstrap gate)",
    )
    cmp_p.add_argument(
        "--run-json",
        type=Path,
        required=False,
        help=("path to the run's JSON report; default: latest *.json in " "--results-dir by mtime"),
    )
    cmp_p.add_argument(
        "--results-dir",
        type=Path,
        default=Path("evals/results"),
        help="where to look for run JSON if --run-json is omitted; default: evals/results",
    )
    cmp_p.add_argument(
        "--baseline",
        type=Path,
        default=None,
        help="path to baseline JSON; default: <baseline-dir>/<name>.json",
    )
    cmp_p.add_argument(
        "--baseline-dir",
        type=Path,
        default=Path("evals/baselines"),
        help="baseline directory; default: evals/baselines",
    )
    cmp_p.add_argument("--name", type=str, default="main", help="baseline name; default: main")
    cmp_p.add_argument(
        "--practical-floor",
        type=float,
        default=0.05,
        help="mean-diff worse than this triggers regression (default: 0.05 = 5%%)",
    )
    cmp_p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="paired-bootstrap RNG seed; pin for reproducibility (default: 42)",
    )
    cmp_p.add_argument(
        "--output",
        type=Path,
        default=None,
        help="optional path to write the markdown report (also printed to stdout)",
    )
    cmp_p.set_defaults(func=_cmd_compare)

    return parser


# --- Subcommands ---------------------------------------------------------- #


def _cmd_run(args: argparse.Namespace) -> int:
    """Run the corpus end-to-end (replay mode). Returns the process exit code."""
    try:
        settings = load_settings()
    except Exception as exc:  # pydantic-settings ValidationError, etc.
        print(f"error: failed to load settings — {exc}", file=sys.stderr)
        return 1

    if (rc := _check_common_eval_guards(settings)) is not None:
        return rc

    corpus_dir = args.corpus or settings.eval_corpus_dir
    if corpus_dir is None:
        print("error: --corpus or SENTINEL_EVAL_CORPUS_DIR must be provided", file=sys.stderr)
        return 1

    # Cassette-dir guard: prevent silently burning API budget if no cassette dir
    # is set and the operator didn't explicitly opt into live mode.
    cassette_dir = args.cassette_dir or settings.eval_cassette_dir
    if cassette_dir is None and not args.live:
        print(
            "error: no cassette directory set — pass --cassette-dir or set "
            "SENTINEL_EVAL_CASSETTE_DIR for replay; pass --live to opt into "
            "live Anthropic API calls (burns budget)",
            file=sys.stderr,
        )
        return 1

    try:
        return asyncio.run(_run_async(args, settings, corpus_dir))
    except CassetteMiss as exc:
        print(f"error: cassette miss — {exc}", file=sys.stderr)
        return 1
    except RuntimeError as exc:
        print(f"error: runtime error during eval run — {exc}", file=sys.stderr)
        return 1


def _cmd_record(args: argparse.Namespace) -> int:
    """Record the corpus end-to-end against the live Anthropic API.

    Reuses the same orchestration as ``_cmd_run`` — the only behavioral
    difference is the cassette transport mode. We set
    ``SENTINEL_EVAL_CASSETTE_MODE=record`` in ``os.environ`` BEFORE calling
    ``load_settings()`` so the lifespan branch in ``sentinel.api.app`` picks
    up the right mode when it constructs ``CassetteTransport``.

    Additional guards vs. ``run``:
      * cassette dir REQUIRED (no --live fallback)
      * ANTHROPIC_API_KEY (or SENTINEL_ANTHROPIC_API_KEY) must be set
    """
    # Resolve the cassette dir BEFORE flipping the env var: if the operator
    # passed --cassette-dir on the CLI we want that value to win, and the
    # downstream Settings load reads SENTINEL_EVAL_CASSETTE_DIR from env. We
    # don't need to pre-validate the cassette dir against settings here — we
    # do that after load_settings() like the other guards.
    api_key_present = bool(
        os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("SENTINEL_ANTHROPIC_API_KEY")
    )
    if not api_key_present:
        print(
            "error: ANTHROPIC_API_KEY (or SENTINEL_ANTHROPIC_API_KEY) must be set for record mode "
            "(record hits the live Anthropic API; cassette dir captures the exchanges)",
            file=sys.stderr,
        )
        return 1

    # Flip the cassette mode BEFORE load_settings() so the lifespan branch in
    # sentinel.api.app constructs CassetteTransport(mode="record", ...). We
    # remember the prior value so a caller embedding the CLI can restore it
    # — important for the test process, which reuses os.environ across cases.
    prior_mode = os.environ.get("SENTINEL_EVAL_CASSETTE_MODE")
    os.environ["SENTINEL_EVAL_CASSETTE_MODE"] = "record"
    try:
        try:
            settings = load_settings()
        except Exception as exc:
            print(f"error: failed to load settings — {exc}", file=sys.stderr)
            return 1

        if (rc := _check_common_eval_guards(settings)) is not None:
            return rc

        corpus_dir = args.corpus or settings.eval_corpus_dir
        if corpus_dir is None:
            print("error: --corpus or SENTINEL_EVAL_CORPUS_DIR must be provided", file=sys.stderr)
            return 1

        cassette_dir = args.cassette_dir or settings.eval_cassette_dir
        if cassette_dir is None:
            print(
                "error: --cassette-dir or SENTINEL_EVAL_CASSETTE_DIR required for record mode "
                "(recording requires a target directory to write cassette JSON into)",
                file=sys.stderr,
            )
            return 1

        # Ensure the lifespan sees the cassette dir via settings even if the
        # operator only passed --cassette-dir on the CLI. Settings is frozen
        # post-construction; the env var is the public knob the lifespan reads.
        if args.cassette_dir is not None:
            os.environ["SENTINEL_EVAL_CASSETTE_DIR"] = str(args.cassette_dir)
            # Re-load so settings.eval_cassette_dir reflects the override.
            settings = load_settings()

        try:
            return asyncio.run(_run_async(args, settings, corpus_dir))
        except CassetteMiss as exc:
            # CassetteMiss in record mode means the transport refused to write
            # for some reason (or a code path bypassed record-mode). Surface it.
            print(f"error: cassette miss during record run — {exc}", file=sys.stderr)
            return 1
        except RuntimeError as exc:
            print(f"error: runtime error during record run — {exc}", file=sys.stderr)
            return 1
    finally:
        if prior_mode is None:
            os.environ.pop("SENTINEL_EVAL_CASSETTE_MODE", None)
        else:
            os.environ["SENTINEL_EVAL_CASSETTE_MODE"] = prior_mode


def _check_common_eval_guards(settings: Settings) -> int | None:
    """Shared guards for ``run`` and ``record``: eval_mode + diagnosis_consumer.

    Returns an exit code (1) when a guard fails, or ``None`` when all pass.
    Settings already validates ``eval_corpus_dir`` is set when ``eval_mode``
    is True, so we don't re-check that here.
    """
    if not settings.eval_mode:
        print(
            "error: SENTINEL_EVAL_MODE must be true to run evals "
            "(set SENTINEL_EVAL_MODE=true and SENTINEL_EVAL_CORPUS_DIR=...)",
            file=sys.stderr,
        )
        return 1

    # Without the diagnosis consumer, the lifespan branch never wraps the
    # AnthropicClient with the cassette transport — the cassette dir arg
    # would be silently ignored and the runner would attempt live API calls
    # (or fail to write cassettes in record mode). Surface this at startup.
    if not settings.diagnosis_consumer_enabled:
        print(
            "error: SENTINEL_DIAGNOSIS_CONSUMER_ENABLED must be true for eval runs "
            "(the cassette transport wraps the AnthropicClient inside the consumer)",
            file=sys.stderr,
        )
        return 1

    return None


async def _run_async(
    args: argparse.Namespace,
    settings: Settings,
    corpus_dir: Path,
) -> int:
    """Async body of the ``run`` subcommand.

    Builds the FastAPI app, drives its lifespan manually (no asgi-lifespan
    dependency — see the docstring on _build_app_and_lifespan), loads the
    corpus, builds RunnerDeps, calls ``run_corpus``, writes the report.
    """
    # Load corpus first so a malformed YAML fails before we boot Kafka.
    cases = load_corpus_dir(corpus_dir)
    if args.smoke:
        cases = sorted(cases, key=lambda c: c.id)[:5]
    if not cases:
        print(f"error: no corpus cases found in {corpus_dir}", file=sys.stderr)
        return 1

    # Kafka topic reset (stale-message hygiene) is intentionally NOT
    # automated inside the CLI: doing it correctly requires a Kafka admin
    # client + a wait for the delete to propagate before the producer
    # subscribes, and getting that race right is fragile. The eval workflow
    # instead expects the operator to run `make evals-reset` before
    # `make evals-record` / `make evals` — a 3-line shell target that
    # deletes the topic + flushes Redis + truncates Postgres.
    # See plans/2026-05-20-eval-harness-pr3c-corpus-plan.md Task 3 for
    # the canonical recipe.

    # Build the FastAPI app via the same factory uvicorn would use.

    from sentinel.api.app import build_app

    app: FastAPI = build_app()

    # Manually drive the lifespan (avoids adding an asgi-lifespan dep — see
    # https://www.starlette.io/lifespan/). The lifespan context manager is
    # entered/exited via the standard contextmanager protocol; both startup
    # and shutdown errors propagate out.
    async with _lifespan_context(app):
        # ASGITransport drives the FastAPI app in-process.
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://eval") as client:
            # Resolve runner dependencies from settings + app.state.
            cassette_transport = getattr(app.state, "cassette_transport", None)

            # Postgres engine for the truncate-between-cases closure. The
            # lifespan already constructs an engine, but we don't expose it on
            # app.state today; build a separate one rather than reach in.
            from sqlalchemy.ext.asyncio import create_async_engine

            engine = create_async_engine(settings.postgres_dsn, future=True)

            # truncate_between_cases is a no-op in eval mode. We used to
            # TRUNCATE incidents/diagnoses + FLUSHDB Redis here, but:
            #
            # 1. The runner now uses per-shot external_ids
            #    (`{case.id}-shot-{i}`) so every (case, shot) tuple creates a
            #    unique incident → no Postgres collisions across cases.
            #    Redis dedup keys derive from sha256(body), and the body
            #    contains the unique id → no false-duplicates either.
            # 2. Truncating between cases while the diagnoser is still
            #    processing the previous case's events RACES with the
            #    diagnoser's insert — causing
            #    `diagnoses_incident_id_fkey` FK violations when the
            #    incident is gone before the diagnosis row commits.
            # 3. Between RUNS, `make evals-reset` does the full wipe; that
            #    handles the only state-pollution concern that remains.
            from sqlalchemy.ext.asyncio import async_sessionmaker as _amsf

            _truncate_factory = _amsf(engine, expire_on_commit=False)  # kept for the
            # diagnosis repo session factory below

            async def _truncate() -> None:
                # Intentionally a no-op; see comment above.
                return None

            # Diagnosis repo — distinct session factory from the in-process
            # FastAPI app so the runner's polling doesn't fight the lifespan's
            # session pool for connections. PostgresDiagnosisRepository's
            # get_by_incident_id satisfies the runner's DiagnosisLookup
            # Protocol structurally.
            from sqlalchemy.ext.asyncio import async_sessionmaker

            from sentinel.persistence.repositories import PostgresDiagnosisRepository

            session_factory = async_sessionmaker(engine, expire_on_commit=False)
            diagnosis_repo = PostgresDiagnosisRepository(session_factory)

            embed = FastEmbedProvider(
                model_cache_dir=Path(settings.embedding_model_cache_dir),
                compute_timeout_s=settings.embedding_compute_timeout_seconds,
                model_name=settings.embedding_model_name,
            )

            # Webhook secret — the runner only signs ``generic`` synthetic
            # payloads today (the corpus alert.source is "generic"); resolve
            # via the same path the WebhookHandler uses.
            from sentinel.integrations.registry import get_secret_for_source

            sample_source = cases[0].alert.source
            try:
                webhook_secret = get_secret_for_source(settings, sample_source)
            except Exception as exc:
                print(
                    f"error: cannot resolve webhook secret for source={sample_source!r} — {exc}",
                    file=sys.stderr,
                )
                return 1

            # Run id is generated locally; the JSON+MD reports under
            # --output-dir are the canonical record of the run today. Switching
            # to PostgresEvalRunRepository requires building out the
            # ``start_run`` / ``finalize_run`` lifecycle (DB-managed run_id,
            # corpus_version, fetcher_fixture_hash, git_sha, regression
            # comparison) — that's a product change (queryable run history),
            # not a cleanup, and belongs in its own PR.
            run_id = uuid.uuid4()
            shot_persister = _StubShotPersister()

            print(f"eval run starting: run_id={run_id} cases={len(cases)} shots={args.shots}")

            deps = RunnerDeps(
                client=client,
                diagnosis_repo=diagnosis_repo,
                eval_run_repo=shot_persister,
                embed=embed,
                cassette_transport=cassette_transport,
                run_id=run_id,
                prompt_version=settings.diagnosis_prompt_version,
                model_id=settings.anthropic_model,
                truncate_between_cases=_truncate,
                webhook_secret=_ensure_secret(webhook_secret),
            )

            try:
                result = await run_corpus(
                    cases=cases,
                    shots_per_case=args.shots,
                    runner_deps=deps,
                )
            finally:
                await engine.dispose()

    # Only reached if run_corpus succeeded; an exception inside the lifespan/
    # client/engine contexts above propagates out of _run_async to the caller,
    # which catches CassetteMiss/RuntimeError. Guarding with `result is not None`
    # would not help because `result` would be unbound — let the caller see the
    # original exception.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path, md_path = write_report(run_result=result, output_dir=args.output_dir)
    print(f"eval run complete: run_id={run_id}")
    print(f"  json: {json_path}")
    print(f"  md:   {md_path}")
    return 0


def _cmd_baseline(args: argparse.Namespace) -> int:
    """Run the corpus through the same pipeline as ``run``, then transform the
    per-run JSON into a baseline file under ``--baseline-dir``.

    The baseline file is a superset of the run JSON: it adds metadata fields
    (``name``, ``git_sha``, ``prompt_version``, ``model_id``, ``recorded_at``,
    ``shots_per_case``) so the gate can refuse to compare across incompatible
    runs (e.g. prompt version changed → numbers aren't apples-to-apples).
    """
    # Delegate to the run command (same args), then post-process the JSON
    # report into a baseline file. Reusing _cmd_run avoids forking the
    # FastAPI lifespan + cassette + scoring pipeline into a second path.
    rc = _cmd_run(args)
    if rc != 0:
        return rc

    # The run wrote <run_id>.json under args.output_dir; pick the freshest.
    json_files = sorted(
        args.output_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not json_files:
        print(
            f"error: baseline: no JSON report under {args.output_dir} after run",
            file=sys.stderr,
        )
        return 1
    run_json_path = json_files[0]

    import json
    from datetime import UTC, datetime

    try:
        settings = load_settings()
    except Exception as exc:
        print(f"error: baseline: failed to load settings — {exc}", file=sys.stderr)
        return 1

    run_blob = json.loads(run_json_path.read_text())
    baseline = {
        # Metadata first so it's near the top of the file when reviewed.
        # `shots_per_case` is deliberately not duplicated here — `run_blob`
        # already carries the authoritative value the runner actually used,
        # which can differ from `args.shots` (e.g. the issue #49 collapse).
        "name": args.name,
        "git_sha": _git_sha(),
        "prompt_version": settings.diagnosis_prompt_version,
        "model_id": settings.anthropic_model,
        "recorded_at": datetime.now(UTC).isoformat(),
        # Then the run payload verbatim (per_case, aggregate_metrics, headline).
        **run_blob,
    }

    args.baseline_dir.mkdir(parents=True, exist_ok=True)
    baseline_path = args.baseline_dir / f"{args.name}.json"
    baseline_path.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n")
    print(f"baseline written: {baseline_path}")
    return 0


def _cmd_compare(args: argparse.Namespace) -> int:
    """Compare a run's per-case metrics against a baseline file using the
    paired-bootstrap regression gate. Exits non-zero on any regression.
    """
    import json

    baseline_path: Path = args.baseline or (args.baseline_dir / f"{args.name}.json")
    if not baseline_path.exists():
        print(
            f"error: compare-to-baseline: baseline not found at {baseline_path}",
            file=sys.stderr,
        )
        return 1

    run_json_path: Path | None = args.run_json
    if run_json_path is None:
        if not args.results_dir.exists():
            print(
                f"error: compare-to-baseline: --run-json not given and "
                f"--results-dir {args.results_dir} does not exist",
                file=sys.stderr,
            )
            return 1
        json_files = sorted(
            args.results_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )
        if not json_files:
            print(
                f"error: compare-to-baseline: no *.json in {args.results_dir}",
                file=sys.stderr,
            )
            return 1
        run_json_path = json_files[0]

    baseline = json.loads(baseline_path.read_text())
    run = json.loads(run_json_path.read_text())

    verdict, markdown = _compute_regression_verdict(
        baseline=baseline,
        run=run,
        practical_floor=args.practical_floor,
        seed=args.seed,
        baseline_label=str(baseline_path),
        run_label=str(run_json_path),
    )
    print(markdown)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(markdown)

    return 1 if verdict.has_regression else 0


def _compute_regression_verdict(
    *,
    baseline: dict[str, object],
    run: dict[str, object],
    practical_floor: float,
    seed: int,
    baseline_label: str,
    run_label: str,
) -> tuple[RegressionVerdict, str]:
    """Compute the per-metric regression verdict + render the markdown report.

    Factored out so unit tests can hit it without the file-loading dance in
    _cmd_compare.

    Returns ``(RegressionVerdict, markdown_str)``. The verdict is the typed
    aggregate; the markdown is the human-readable report (intended for PR
    comments).
    """
    from sentinel.evals.schema import RegressionResult as _RR
    from sentinel.evals.schema import RegressionVerdict as _RV
    from sentinel.evals.stats import regression_for_metric

    metrics = ["category_match", "hypothesis_cosine", "action_coverage", "evidence_quality"]
    # `evidence_quality` flips lower-is-better in the future spec for
    # hallucinated_evidence_rate, but for now ALL four metrics are
    # higher-is-better; the schema/spec gate handles the sign flip.
    higher_is_better = dict.fromkeys(metrics, True)

    # Index per_case by case_id so we can pair shots without assuming order.
    def _by_case(blob: dict[str, object]) -> dict[str, dict[str, float | None]]:
        per_case = blob.get("per_case", [])
        if not isinstance(per_case, list):
            return {}
        out: dict[str, dict[str, float | None]] = {}
        for entry in per_case:
            if not isinstance(entry, dict):
                continue
            cid = entry.get("case_id")
            metrics_dict = entry.get("metrics")
            if isinstance(cid, str) and isinstance(metrics_dict, dict):
                out[cid] = metrics_dict
        return out

    base_idx = _by_case(baseline)
    run_idx = _by_case(run)
    shared_cases = sorted(set(base_idx.keys()) & set(run_idx.keys()))
    only_in_baseline = sorted(set(base_idx.keys()) - set(run_idx.keys()))
    only_in_run = sorted(set(run_idx.keys()) - set(base_idx.keys()))

    per_metric: list[_RR] = []
    for metric in metrics:
        baseline_vals: list[float] = []
        current_vals: list[float] = []
        for cid in shared_cases:
            b = base_idx[cid].get(metric)
            c = run_idx[cid].get(metric)
            if b is None or c is None:
                # Skip cases where either side is missing this metric — the
                # paired-bootstrap requires matched pairs. A metric that's
                # None everywhere will produce zero pairs and the gate
                # short-circuits to "no signal, no regression".
                continue
            baseline_vals.append(float(b))
            current_vals.append(float(c))

        if not baseline_vals:
            per_metric.append(
                _RR(
                    metric_name=metric,
                    mean_diff=0.0,
                    ci_low=0.0,
                    ci_high=0.0,
                    is_regression=False,
                    reason="no paired cases (metric absent on one side)",
                )
            )
            continue

        per_metric.append(
            regression_for_metric(
                metric_name=metric,
                current_per_case=current_vals,
                baseline_per_case=baseline_vals,
                seed=seed,
                practical_floor=practical_floor,
                higher_is_better=higher_is_better[metric],
            )
        )

    verdict = _RV(per_metric=per_metric)
    markdown = _render_regression_markdown(
        verdict=verdict,
        baseline=baseline,
        baseline_label=baseline_label,
        run_label=run_label,
        n_paired=len(shared_cases),
        only_in_baseline=only_in_baseline,
        only_in_run=only_in_run,
    )
    return verdict, markdown


def _render_regression_markdown(
    *,
    verdict: RegressionVerdict,
    baseline: dict[str, object],
    baseline_label: str,
    run_label: str,
    n_paired: int,
    only_in_baseline: list[str],
    only_in_run: list[str],
) -> str:
    """Render the RegressionVerdict as a markdown table suitable for PR comments.

    Includes the baseline's recorded ``prompt_version`` + ``model_id`` +
    ``git_sha`` so a reviewer can spot when a PR's bumped prompt is being
    compared against a stale baseline. The gate doesn't hard-fail on this
    (corpus + prompt are usually evolved together) but surfacing it lets a
    careful reader catch apples-to-oranges comparisons.
    """
    lines: list[str] = []
    status = "❌ REGRESSION" if verdict.has_regression else "✅ no regression"
    lines.append(f"## Eval regression gate: {status}")
    lines.append("")
    lines.append(f"- baseline: `{baseline_label}`")
    lines.append(f"- run: `{run_label}`")
    lines.append(f"- paired cases: {n_paired}")

    # Baseline provenance — surfaces stale-baseline scenarios.
    base_meta_bits: list[str] = []
    for field in ("prompt_version", "model_id", "git_sha", "recorded_at"):
        val = baseline.get(field)
        if isinstance(val, str) and val:
            base_meta_bits.append(f"{field}={val}")
    if base_meta_bits:
        lines.append(f"- baseline recorded with: {', '.join(base_meta_bits)}")

    # Case-set drift — silently dropping unpaired cases is exactly how a PR
    # that deleted/renamed a corpus case could slip past the gate. Loud.
    if only_in_baseline:
        lines.append(
            f"- ⚠️ cases in baseline missing from run "
            f"(coverage loss, not contributing to the gate): "
            f"{', '.join(only_in_baseline)}"
        )
    if only_in_run:
        lines.append(
            f"- info: new cases in run not in baseline "
            f"(re-run `make evals-baseline` to capture them): "
            f"{', '.join(only_in_run)}"
        )

    if verdict.has_regression:
        lines.append(f"- regressed metrics: {', '.join(verdict.regressed_metrics)}")
    lines.append("")
    lines.append("| Metric | mean diff | 95% CI | verdict |")
    lines.append("|---|---:|---|---|")
    for r in verdict.per_metric:
        flag = "❌" if r.is_regression else "✅"
        lines.append(
            f"| {r.metric_name} | {r.mean_diff:+.3f} | "
            f"[{r.ci_low:+.3f}, {r.ci_high:+.3f}] | {flag} {r.reason} |"
        )
    lines.append("")
    return "\n".join(lines)


def _git_sha() -> str:
    """Resolve the current git SHA; returns ``"unknown"`` when we're not in a
    git checkout (e.g. running from a tarball)."""
    import subprocess

    try:
        # argv is fixed (no shell interpolation), and `git` is intentionally
        # resolved via PATH so this works in any dev environment and CI runner
        # without hard-coding /usr/bin/git or similar.
        argv = ["git", "rev-parse", "HEAD"]
        result = subprocess.run(  # noqa: S603
            argv,
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    if result.returncode != 0:
        return "unknown"
    return result.stdout.strip() or "unknown"


def _cmd_readme(args: argparse.Namespace) -> int:
    """Patch README between markers using the latest eval result MD file.

    Until PR 1's ``PostgresEvalRunRepository`` lands, we fall back to reading
    the most recently modified ``<run_id>.md`` under ``--results-dir``. The
    "headline" section of that MD is lifted verbatim into the README between
    the markers.
    """
    readme_path: Path = args.readme
    if not readme_path.exists():
        print(f"error: README not found at {readme_path}", file=sys.stderr)
        return 1

    results_dir: Path = args.results_dir
    if not results_dir.exists():
        print(
            f"error: no eval results found at {results_dir} — run `make evals` first",
            file=sys.stderr,
        )
        return 1

    md_files = sorted(results_dir.glob("*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not md_files:
        print(
            f"error: no .md files found in {results_dir} — run `make evals` first",
            file=sys.stderr,
        )
        return 1

    latest = md_files[0]
    summary = _extract_headline_summary(latest)
    new_contents = _patch_readme_between_markers(readme_path.read_text(), summary)
    readme_path.write_text(new_contents)
    print(f"patched {readme_path} from {latest}")
    return 0


# --- Helpers --------------------------------------------------------------- #


class _StubShotPersister:
    """In-memory persister — keeps the JSON+MD report files as the canonical
    artifact of an eval run, since the eval CLI doesn't yet construct the
    surrounding ``start_run``/``finalize_run`` lifecycle that
    ``PostgresEvalRunRepository.persist_shot`` requires (the row's FK to
    ``eval_runs.id`` would fail without a matching start_run row).

    Holds the unified ``EvalCaseResultRecord`` defined in
    ``sentinel/persistence/repositories.py`` — the duplicate-shape bridge
    that PR 3b/3c briefly shipped has been removed. Wiring the real
    repository requires extending the CLI with corpus-version and
    fetcher-fixture-hash discovery; tracked separately.
    """

    def __init__(self) -> None:
        self.shots: list[EvalCaseResultRecord] = []

    async def persist_shot(self, shot: EvalCaseResultRecord) -> None:
        self.shots.append(shot)


def _ensure_secret(value: SecretStr) -> SecretStr:
    """Type-narrowing helper — registry returns SecretStr but mypy sometimes
    loses the constraint through ``getattr``. Centralizes the assertion."""
    if not isinstance(value, SecretStr):
        raise TypeError(f"expected SecretStr, got {type(value).__name__}")
    return value


def _extract_headline_summary(md_path: Path) -> str:
    """Pull a compact summary out of a report MD for inclusion in README.

    Returns the report's headline section (everything from "## Headline" to the
    end of file) prefixed with a generated-by note + the run id derived from
    the filename. Robust to the report format evolving — falls back to the
    last 10 lines of the file if no headline section is found.
    """
    text = md_path.read_text()
    # Lift the "## Aggregate Metrics" block + everything through end-of-file
    # (so Headline and any future trailing sections come along). The Per-case
    # table sits between — it's verbose but useful for a portfolio reader
    # checking the headline numbers against per-case detail.
    agg_marker = "## Aggregate Metrics"
    if agg_marker in text:
        body = text[text.index(agg_marker) :].rstrip()
    elif "## Headline" in text:
        body = text[text.index("## Headline") :].rstrip()
    else:
        body = "\n".join(text.splitlines()[-10:])

    run_id = md_path.stem
    return f"_From eval run `{run_id}` (auto-generated; do not edit between markers)._\n\n{body}\n"


def _patch_readme_between_markers(readme_text: str, replacement: str) -> str:
    """Replace the content between ``<!-- evals:start --> .. <!-- evals:end -->``.

    Raises ValueError if the markers are missing or out of order — the readme
    subcommand surfaces that as a clear error rather than silently appending.
    """
    pattern = re.compile(
        re.escape(README_MARKER_START) + r".*?" + re.escape(README_MARKER_END),
        flags=re.DOTALL,
    )
    if not pattern.search(readme_text):
        raise ValueError(
            f"README markers {README_MARKER_START!r} / {README_MARKER_END!r} not found — "
            "add them to the README before running `make readme-numbers`"
        )
    new_block = f"{README_MARKER_START}\n{replacement}{README_MARKER_END}"
    return pattern.sub(new_block, readme_text, count=1)


# --- Lifespan context manager --------------------------------------------- #

# asgi-lifespan would also work here, but adding a dev dep for a single
# usage isn't worth the import surface; the manual startup/shutdown calls
# below are the documented FastAPI test pattern.


class _lifespan_context:
    """Async context manager that drives the FastAPI lifespan manually.

    Equivalent to ``asgi_lifespan.LifespanManager(app)`` but doesn't add a
    dev dependency. Uses the ASGI lifespan protocol directly:
    https://asgi.readthedocs.io/en/latest/specs/lifespan.html
    """

    def __init__(self, app: FastAPI) -> None:
        self._app = app
        self._task: asyncio.Task[None] | None = None
        self._receive_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue()
        self._send_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue()

    async def __aenter__(self) -> _lifespan_context:
        async def _receive() -> dict[str, str]:
            return await self._receive_queue.get()

        async def _send(message: dict[str, str]) -> None:
            await self._send_queue.put(message)

        scope: dict[str, object] = {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
        }
        self._task = asyncio.create_task(self._run_app(scope, _receive, _send))
        await self._receive_queue.put({"type": "lifespan.startup"})
        msg = await self._send_queue.get()
        if msg["type"] == "lifespan.startup.failed":
            raise RuntimeError(f"lifespan startup failed: {msg.get('message', '')}")
        if msg["type"] != "lifespan.startup.complete":
            raise RuntimeError(f"unexpected lifespan message: {msg!r}")
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self._receive_queue.put({"type": "lifespan.shutdown"})
        try:
            msg = await self._send_queue.get()
            if msg["type"] == "lifespan.shutdown.failed":
                log.warning("lifespan shutdown failed: %s", msg.get("message", ""))
        finally:
            if self._task is not None:
                # Best-effort: the lifespan task either completes cleanly after
                # the shutdown message, or it raises (which we log via the
                # lifespan.shutdown.failed branch above) — either way we want
                # cleanup to keep going. Exception is broad on purpose: at
                # shutdown anything from the consumer-stop chain is acceptable
                # to swallow since we've already drained the protocol message.
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self._task, timeout=30.0)

    async def _run_app(
        self,
        scope: dict[str, object],
        receive: object,
        send: object,
    ) -> None:
        # The FastAPI/Starlette ASGI callable expects (scope, receive, send)
        # and drives the lifespan via the messages on those queues.
        await self._app(scope, receive, send)  # type: ignore[arg-type]


__all__ = ["README_MARKER_END", "README_MARKER_START", "main"]
