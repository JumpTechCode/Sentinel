# tests/unit/memory/test_consumer.py
"""MemoryConsumer.handle_message — envelope validation, branching, idempotency, errors."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sentinel.memory.consumer import MemoryConsumer
from sentinel.memory.deps import MemoryConsumerDeps
from sentinel.memory.pipeline import MemoryPipeline
from sentinel.persistence.repositories import MemoryIncidentRow, ResolutionData

pytestmark = pytest.mark.asyncio


def _msg(payload: dict[str, Any] | None, *, raw: bytes | None = None) -> Any:
    if raw is not None:
        value = raw
    elif payload is not None:
        value = json.dumps(payload).encode()
    else:
        value = b""
    return SimpleNamespace(value=value, offset=0, partition=0)


def _make_consumer(
    *,
    incident_repo: Any = None,
    embedder: Any = None,
    pipeline: MemoryPipeline | None = None,
) -> tuple[MemoryConsumer, Any]:
    kafka = MagicMock()
    kafka.commit = AsyncMock()
    deps = MemoryConsumerDeps(
        incident_repo=incident_repo or MagicMock(),
        embedding_provider=embedder or MagicMock(),
        pipeline=pipeline or MemoryPipeline(),
    )
    return MemoryConsumer(consumer=kafka, deps=deps), kafka


async def test_invalid_envelope_commits_offset_as_poison_pill() -> None:
    consumer, kafka = _make_consumer()
    await consumer.handle_message(_msg(None, raw=b"not json"))
    kafka.commit.assert_awaited_once()


async def test_unknown_event_type_commits_offset() -> None:
    consumer, kafka = _make_consumer()
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(uuid4()),
                "event": "some.other.event",
                "incident_id": str(uuid4()),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )
    kafka.commit.assert_awaited_once()


async def test_opened_event_writes_initial_embedding() -> None:
    incident_id = uuid4()
    event_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="api",
            title="db timeout",
            status="open",
            resolution=None,
        )
    )
    incident_repo.set_embedding = AsyncMock(return_value="written")
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.1] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(event_id),
                "event": "incident.opened",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    embedder.embed.assert_awaited_once_with("api db timeout")
    incident_repo.set_embedding.assert_awaited_once()
    kafka.commit.assert_awaited_once()


async def test_resolved_event_writes_resolved_embedding() -> None:
    incident_id = uuid4()
    event_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="api",
            title="db timeout",
            status="resolved",
            resolution=ResolutionData(
                root_cause="pool exhausted",
                remediation="raised pool",
                diagnosis_was_correct=True,
            ),
        )
    )
    incident_repo.set_embedding = AsyncMock(return_value="written")
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.2] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(event_id),
                "event": "incident.resolved",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    embedder.embed.assert_awaited_once_with("db timeout\npool exhausted\nraised pool")
    incident_repo.set_embedding.assert_awaited_once()
    kafka.commit.assert_awaited_once()


async def test_resolved_event_without_resolution_row_commits() -> None:
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="api",
            title="t",
            status="resolved",
            resolution=None,
        )
    )
    incident_repo.set_embedding = AsyncMock()
    embedder = MagicMock()
    embedder.embed = AsyncMock()

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(uuid4()),
                "event": "incident.resolved",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    embedder.embed.assert_not_awaited()
    incident_repo.set_embedding.assert_not_awaited()
    kafka.commit.assert_awaited_once()


async def test_unknown_incident_commits_offset() -> None:
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(return_value=None)
    embedder = MagicMock()
    embedder.embed = AsyncMock()

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(uuid4()),
                "event": "incident.opened",
                "incident_id": str(uuid4()),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    embedder.embed.assert_not_awaited()
    kafka.commit.assert_awaited_once()


async def test_duplicate_event_id_commits_offset() -> None:
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="a",
            title="b",
            status="open",
            resolution=None,
        )
    )
    incident_repo.set_embedding = AsyncMock(return_value="duplicate")
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.0] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(uuid4()),
                "event": "incident.opened",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    kafka.commit.assert_awaited_once()


async def test_embedding_timeout_commits_as_poison_pill() -> None:
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="a",
            title="b",
            status="open",
            resolution=None,
        )
    )
    incident_repo.set_embedding = AsyncMock()
    embedder = MagicMock()
    embedder.embed = AsyncMock(side_effect=TimeoutError())

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    await consumer.handle_message(
        _msg(
            {
                "event_id": str(uuid4()),
                "event": "incident.opened",
                "incident_id": str(incident_id),
                "ts": "2026-05-19T16:00:00+00:00",
            }
        )
    )

    incident_repo.set_embedding.assert_not_awaited()
    kafka.commit.assert_awaited_once()


async def test_db_write_failure_does_not_commit() -> None:
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="a",
            title="b",
            status="open",
            resolution=None,
        )
    )
    incident_repo.set_embedding = AsyncMock(side_effect=RuntimeError("db down"))
    embedder = MagicMock()
    embedder.embed = AsyncMock(return_value=[0.0] * 1024)

    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    with pytest.raises(RuntimeError):
        await consumer.handle_message(
            _msg(
                {
                    "event_id": str(uuid4()),
                    "event": "incident.opened",
                    "incident_id": str(incident_id),
                    "ts": "2026-05-19T16:00:00+00:00",
                }
            )
        )

    kafka.commit.assert_not_awaited()


# ---- Embedding poison-pill ----------------------------------------------
#
# A corrupt ONNX file or a permanent fastembed initialization error makes
# every `embed()` call raise the same non-TimeoutError exception (e.g.
# OnnxRuntimeException). Without a bound, the consumer hot-loops on the
# same offset forever: run() catches Exception, doesn't commit, Kafka
# redelivers, repeat. The bound: after N consecutive failures on the SAME
# event_id, commit as a poison pill so the stream advances.


async def _make_failing_embed_consumer(
    incident_id: Any, exc: Exception
) -> tuple[MemoryConsumer, Any, Any]:
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="a",
            title="b",
            status="open",
            resolution=None,
        )
    )
    incident_repo.set_embedding = AsyncMock()
    embedder = MagicMock()
    embedder.embed = AsyncMock(side_effect=exc)
    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)
    return consumer, kafka, incident_repo


def _opened_payload(incident_id: Any, event_id: Any) -> dict[str, str]:
    return {
        "event_id": str(event_id),
        "event": "incident.opened",
        "incident_id": str(incident_id),
        "ts": "2026-05-19T16:00:00+00:00",
    }


async def test_first_embed_runtime_error_re_raises_for_redelivery() -> None:
    """One transient embed failure must re-raise so run() refuses to commit.

    `RuntimeError` from fastembed could be a corrupt model (persistent) or
    a one-off ONNX hiccup (transient). We can't tell from one sample, so
    treat it as transient on the first try.
    """
    consumer, kafka, repo = await _make_failing_embed_consumer(
        uuid4(), RuntimeError("ONNX session init failed")
    )
    with pytest.raises(RuntimeError):
        await consumer.handle_message(_msg(_opened_payload(uuid4(), uuid4())))

    repo.set_embedding.assert_not_awaited()
    kafka.commit.assert_not_awaited()


async def test_third_consecutive_embed_failure_on_same_event_commits_poison_pill() -> None:
    """N=3 consecutive failures on the SAME event_id → commit + metric.

    Without this bound, a corrupt ONNX model wedges the consumer on the
    first failing offset forever (Kafka at-least-once + no commit + same
    error every retry).
    """
    incident_id = uuid4()
    event_id = uuid4()
    consumer, kafka, repo = await _make_failing_embed_consumer(
        incident_id, RuntimeError("ONNX session init failed")
    )
    msg = _msg(_opened_payload(incident_id, event_id))

    # Two attempts re-raise (run() loop would catch + not-commit + redeliver).
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await consumer.handle_message(msg)
    kafka.commit.assert_not_awaited()

    # Third attempt: do NOT raise, commit offset, label as poison pill.
    await consumer.handle_message(msg)

    repo.set_embedding.assert_not_awaited()
    kafka.commit.assert_awaited_once()


async def test_different_event_id_resets_failure_counter() -> None:
    """A different event_id is a different message — counter resets."""
    incident_id = uuid4()
    consumer, kafka, repo = await _make_failing_embed_consumer(
        incident_id, RuntimeError("ONNX session init failed")
    )

    # Two failures on event A.
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await consumer.handle_message(_msg(_opened_payload(incident_id, uuid4())))

    # Event B arrives, also fails — counter resets to 1, so this re-raises
    # (not a poison-pill commit).
    with pytest.raises(RuntimeError):
        await consumer.handle_message(_msg(_opened_payload(incident_id, uuid4())))

    kafka.commit.assert_not_awaited()
    # Pin the reset semantics: counter == 1, not 0 (the new event_id had a
    # failure, so it counts as the first attempt on that event).
    assert consumer._embed_consecutive_failures == 1


async def test_successful_event_resets_failure_counter() -> None:
    """After a clean handle_message, the counter resets.

    Pattern: 2 failures on event A → success on event B → failure on event C.
    Event C must NOT be poison-pilled (counter was reset by B's success).
    """
    incident_id = uuid4()
    incident_repo = MagicMock()
    incident_repo.load_for_memory = AsyncMock(
        return_value=MemoryIncidentRow(
            id=incident_id,
            service="a",
            title="b",
            status="open",
            resolution=None,
        )
    )
    incident_repo.set_embedding = AsyncMock(return_value="written")
    embedder = MagicMock()
    # Fail twice, succeed once, fail twice more.
    embedder.embed = AsyncMock(
        side_effect=[
            RuntimeError("hiccup"),
            RuntimeError("hiccup"),
            [0.0] * 1024,
            RuntimeError("hiccup"),
            RuntimeError("hiccup"),
        ]
    )
    consumer, kafka = _make_consumer(incident_repo=incident_repo, embedder=embedder)

    # Event A: 2 failures, no commit.
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await consumer.handle_message(_msg(_opened_payload(incident_id, uuid4())))

    # Event B: success — commits, resets counter.
    await consumer.handle_message(_msg(_opened_payload(incident_id, uuid4())))
    assert kafka.commit.await_count == 1

    # Event C: 2 failures must NOT trip poison pill (counter was reset).
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await consumer.handle_message(_msg(_opened_payload(incident_id, uuid4())))

    # Only the successful event committed; no poison-pill commit.
    assert kafka.commit.await_count == 1
