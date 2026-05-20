"""Command-line entrypoint for the eval harness.

Subcommands (argparse):

  * ``run`` — fully implemented in PR 3b. Boots an in-process FastAPI app via
    ``build_app()``, drives requests through ``httpx.AsyncClient(transport=
    ASGITransport(...))``, polls Postgres for the resulting diagnoses, scores
    each shot against the corpus ground truth, and writes a JSON + Markdown
    report under ``evals/results/<run_id>.{json,md}``.
  * ``record`` — stub. Real cassette recording lands with the 10 corpus YAMLs
    in PR 3c; this stub prints a clear "use manual record flow" message and
    exits 1.
  * ``baseline`` — stub. Wired in PR 3c once the first real corpus run produces
    a baseline.
  * ``readme`` — patches ``README.md`` between
    ``<!-- evals:start -->`` / ``<!-- evals:end -->`` markers with the headline
    metrics from the most recent ``evals/results/*.md`` report (PR 1's
    ``PostgresEvalRunRepository`` will replace this filesystem lookup once it
    lands).
  * ``compare-to-baseline`` — stub. Wired in PR 3c.

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
from uuid import UUID as UUID_t

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
    run_p.add_argument("--shots", type=int, default=3, help="shots per case (default: 3)")
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
    # Record shots default to 3 to match the replay default — the cassette
    # key includes shot_index, so N replay shots need N recorded cassettes.
    rec_p.add_argument("--shots", type=int, default=3, help="shots per case (default: 3)")
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

    # baseline (stub) -------------------------------------------------------
    base_p = sub.add_parser("baseline", help="(PR 3c) record a baseline corpus run")
    base_p.add_argument("--corpus", type=Path, default=None)
    base_p.add_argument("--shots", type=int, default=5)
    base_p.set_defaults(func=_cmd_baseline_stub)

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

    # compare-to-baseline (stub) -------------------------------------------
    cmp_p = sub.add_parser(
        "compare-to-baseline",
        help="(PR 3c) compare a run's metrics to a baseline file",
    )
    cmp_p.add_argument("--run-id", type=str, required=False)
    cmp_p.add_argument("--baseline", type=Path, required=False)
    cmp_p.set_defaults(func=_cmd_compare_stub)

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

            # Truncate helper — raw SQL lives in persistence/ to honor the
            # no-raw-SQL-outside-persistence project invariant.
            from sqlalchemy.ext.asyncio import async_sessionmaker as _amsf

            from sentinel.persistence.repositories import truncate_eval_runtime_state

            _truncate_factory = _amsf(engine, expire_on_commit=False)

            async def _truncate() -> None:
                await truncate_eval_runtime_state(_truncate_factory)

            # Diagnosis repo — distinct session factory from the in-process
            # FastAPI app so the runner's polling doesn't fight the lifespan's
            # session pool for connections.
            from sqlalchemy.ext.asyncio import async_sessionmaker

            from sentinel.persistence.repositories import fetch_latest_diagnosis_for_eval

            session_factory = async_sessionmaker(engine, expire_on_commit=False)

            # Eval-only DiagnosisLookup adapter — bridges the runner's protocol
            # to a free function in persistence/ (the "real" get_by_incident_id
            # method lands in PR 1 / #42). Removed during reconciliation once
            # both PRs merge.
            class _EvalDiagnosisLookup:
                def __init__(self, sf: object) -> None:
                    self._sf = sf

                async def get_by_incident_id(self, incident_id: UUID_t) -> object:
                    return await fetch_latest_diagnosis_for_eval(self._sf, incident_id)  # type: ignore[arg-type]

            diagnosis_repo = _EvalDiagnosisLookup(session_factory)

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

            # TODO: wire to PostgresEvalRunRepository when PR 1 merges; for
            # now generate a uuid locally and use the report files as the
            # sole record of the run.
            run_id = uuid.uuid4()
            shot_persister = _StubShotPersister()

            print(f"eval run starting: run_id={run_id} cases={len(cases)} shots={args.shots}")

            # diagnosis_repo: _EvalDiagnosisLookup (above) structurally satisfies
            # DiagnosisLookup via fetch_latest_diagnosis_for_eval from
            # persistence/. Cast to DiagnosisLookup for explicit typing.
            from typing import cast

            from sentinel.evals.runner import DiagnosisLookup

            deps = RunnerDeps(
                client=client,
                diagnosis_repo=cast(DiagnosisLookup, diagnosis_repo),
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


def _cmd_baseline_stub(_args: argparse.Namespace) -> int:
    print(
        "baseline: not implemented in PR 3b — baseline workflow lands with PR 3c "
        "once the first real corpus run produces a tagged baseline.",
        file=sys.stderr,
    )
    return 1


def _cmd_compare_stub(_args: argparse.Namespace) -> int:
    print(
        "compare-to-baseline: not implemented in PR 3b — wired in PR 3c once the "
        "first baseline exists.",
        file=sys.stderr,
    )
    return 1


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
    """Placeholder until PR 1's ``PostgresEvalRunRepository`` lands.

    Records shots in memory so the runner contract is satisfied; the real
    persistence happens in the report files emitted at the end of the run.
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
    headline_marker = "## Headline"
    if headline_marker in text:
        body = text[text.index(headline_marker) :].rstrip()
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
