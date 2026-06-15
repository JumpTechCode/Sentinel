"""End-to-end integration test for the eval runner (#50).

Boots the real FastAPI app (via the same ``_run_async`` path the ``evals`` CLI
uses), drives a one-case fixture corpus through the full
webhook → Kafka → enrichment → diagnosis → persistence pipeline against real
Postgres + Kafka + Redis (testcontainers), with the Anthropic call served from
a committed cassette in replay mode. No live API call.

Each assertion pins one regression surfaced during PR 3.5, so a re-introduction
fails here in one shot:

1. ``incident.opened`` outbox payload carries ``fingerprint`` + ``source``.
2. ``incident.enriched`` outbox payload carries ``fingerprint`` + ``source``
   (the enricher used to drop them → diagnoser rejected the envelope).
3. The persisted diagnosis has a non-empty hypothesis — the cassette body
   (an SSE stream tagged ``content-encoding: gzip`` but already-decoded) is
   read exactly once, not double-decoded.
4. The single incident's ``external_id`` encodes its shot index
   (``…-shot-0``) — the per-shot external-id contract.
5. The persisted diagnosis resolves its evidence (non-empty,
   ``hallucinated_evidence`` false) via the lenient (kind, id) matcher.
6. ``eval_runs`` has exactly one ``status='ok'`` row and ``eval_case_results``
   has ``corpus_size * shots_per_case`` rows — the PR 5 lifecycle persisted a
   complete, terminal run.

Runs with ``shots=1``: PR 6's stability shots (1+) deliberately bypass the
pipeline and aren't persisted, so a single shot keeps the persistence
assertions deterministic. Multi-shot semantics are covered at the unit level
(tests/unit/evals/test_runner_unit.py).
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path
from typing import Any

import pytest
from sentinel.persistence.models import DiagnosisModel, IncidentModel, OutboxEventModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

pytestmark = pytest.mark.integration

_FIXTURES = Path(__file__).parent / "fixtures"
_CASE_ID = "cloudflare-2019-07-02-regex"


async def _ensure_topic(brokers: str, topic: str) -> None:
    """Pre-create the incidents topic, as ``make evals-reset`` does for the
    compose-based eval workflow. The CLI's ``_run_async`` assumes the topic
    already exists; against a fresh testcontainers Kafka it does not, and the
    enrichment/diagnosis consumers would otherwise spin on "topic not found"
    until the diagnosis poll times out.
    """
    import contextlib

    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    await admin.start()
    try:
        # Already-exists is fine (re-run against the session-scoped broker); any
        # other failure should surface loudly as a broken test setup.
        with contextlib.suppress(TopicAlreadyExistsError):
            await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    finally:
        await admin.close()


def _build_run_args(
    *, corpus_dir: Path, cassette_dir: Path, output_dir: Path
) -> argparse.Namespace:
    """The arg surface ``_run_async`` + ``_discover_run_metadata`` read for a
    persisted, replay-mode ``run`` of a single case at one shot."""
    return argparse.Namespace(
        corpus=corpus_dir,
        cassette_dir=cassette_dir,
        shots=1,
        smoke=False,
        live=False,
        output_dir=output_dir,
        no_persist=False,
        allow_dirty=True,  # the working tree is dirty during local test runs
        subcommand="run",
    )


@pytest.mark.asyncio
async def test_runner_e2e_full_pipeline_replay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    pg_dsn: str,
    kafka_brokers: str,
    redis_url: str,
) -> None:
    from sentinel.config.settings import load_settings
    from sentinel.evals.cli import _run_async

    corpus_dir = _FIXTURES / "corpus"
    cassette_dir = _FIXTURES / "cassettes"

    # Point the app + runner at the testcontainers and into eval/replay mode.
    # Env is read by both this test's load_settings() and the lifespan's own
    # load_settings() (uncached), so they agree.
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", pg_dsn)
    monkeypatch.setenv("SENTINEL_REDIS_URL", redis_url)
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", kafka_brokers)
    monkeypatch.setenv("SENTINEL_EVAL_MODE", "true")
    monkeypatch.setenv("SENTINEL_EVAL_CORPUS_DIR", str(corpus_dir))
    monkeypatch.setenv("SENTINEL_EVAL_CASSETTE_DIR", str(cassette_dir))
    monkeypatch.setenv("SENTINEL_EVAL_CASSETTE_MODE", "replay")
    monkeypatch.setenv("SENTINEL_DIAGNOSIS_CONSUMER_ENABLED", "true")
    # Memory consumer off: the runner doesn't need it, and disabling it avoids a
    # second embedding model load + Kafka consumer group.
    monkeypatch.setenv("SENTINEL_MEMORY_CONSUMER_ENABLED", "false")
    monkeypatch.setenv("SENTINEL_GENERIC_WEBHOOK_SECRET", "test-secret")
    # Replay never calls Anthropic, but the client is still constructed.
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test-not-used")
    # Small embedding model + a persistent cache dir so scoring doesn't pull the
    # 1.3GB default model; honor an ambient cache dir (CI sets one).
    monkeypatch.setenv("SENTINEL_EMBEDDING_MODEL_NAME", "BAAI/bge-small-en-v1.5")
    monkeypatch.setenv(
        "SENTINEL_EMBEDDING_MODEL_CACHE_DIR",
        os.environ.get("SENTINEL_EMBEDDING_MODEL_CACHE_DIR")
        or str(Path(tempfile.gettempdir()) / "sentinel-fastembed-cache"),
    )

    settings = load_settings()
    await _ensure_topic(kafka_brokers, settings.kafka_topic_incidents)
    args = _build_run_args(
        corpus_dir=corpus_dir, cassette_dir=cassette_dir, output_dir=tmp_path / "results"
    )

    rc = await _run_async(args, settings, corpus_dir)
    assert rc == 0, "eval run should complete cleanly against the replay cassette"

    # --- assert on the persisted state via a fresh engine --------------------
    engine = create_async_engine(pg_dsn)
    try:
        async with AsyncSession(engine) as session:
            outbox_rows = list((await session.execute(select(OutboxEventModel))).scalars().all())
            diag_rows = list((await session.execute(select(DiagnosisModel))).scalars().all())
            incident_rows = list((await session.execute(select(IncidentModel))).scalars().all())
            eval_run_status = list(
                (await session.execute(text("SELECT status FROM eval_runs"))).scalars().all()
            )
            case_result_count = (
                await session.execute(text("SELECT count(*) FROM eval_case_results"))
            ).scalar_one()
    finally:
        await engine.dispose()

    payloads_by_event: dict[str, dict[str, Any]] = {}
    for row in outbox_rows:
        payload = row.payload
        payloads_by_event[str(payload.get("event"))] = payload

    # 1. incident.opened envelope is complete.
    opened = payloads_by_event.get("incident.opened")
    assert opened is not None, f"no incident.opened outbox row; saw {list(payloads_by_event)}"
    assert opened.get("fingerprint"), "incident.opened payload missing fingerprint"
    assert opened.get("source"), "incident.opened payload missing source"

    # 2. incident.enriched envelope is complete (regression: enricher dropped these).
    enriched = payloads_by_event.get("incident.enriched")
    assert enriched is not None, f"no incident.enriched outbox row; saw {list(payloads_by_event)}"
    assert enriched.get("fingerprint"), "incident.enriched payload missing fingerprint"
    assert enriched.get("source"), "incident.enriched payload missing source"

    # 3. Diagnosis persisted with real content (cassette body decoded once).
    assert len(diag_rows) == 1, f"expected exactly one diagnosis row, got {len(diag_rows)}"
    diagnosis = diag_rows[0]
    assert diagnosis.hypothesis.strip(), "diagnosis hypothesis empty (content-encoding regression?)"
    assert diagnosis.reasoning.strip(), "diagnosis reasoning is empty"

    # 4. Per-shot external_id contract: the single incident encodes shot 0.
    assert len(incident_rows) == 1, f"expected exactly one incident, got {len(incident_rows)}"
    assert incident_rows[0].external_id == f"{_CASE_ID}-shot-0"

    # 5. Evidence resolved via the lenient (kind, id) matcher.
    assert (
        diagnosis.hallucinated_evidence is False
    ), "evidence was marked hallucinated — the lenient (kind, id) matcher regressed"
    assert len(diagnosis.evidence) > 0, "no evidence refs survived verification"

    # 6. The PR 5 lifecycle persisted one terminal, ok run + one case result.
    assert eval_run_status == ["ok"], f"expected one ok eval_run, got {eval_run_status}"
    assert (
        case_result_count == 1
    ), f"expected corpus_size*shots=1 case results, got {case_result_count}"
