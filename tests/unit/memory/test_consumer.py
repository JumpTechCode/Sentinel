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
