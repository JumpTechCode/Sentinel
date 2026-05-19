from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sentinel.ingestion.outbox_drainer import OutboxDrainer
from sentinel.persistence.repositories import OutboxEvent
from sqlalchemy.ext.asyncio import AsyncSession


def _now() -> datetime:
    return datetime.now(UTC)


class _FakeBatch:
    def __init__(self, events: list[OutboxEvent]) -> None:
        self.events = events
        self.published: list[UUID] = []
        self.failed: list[tuple[UUID, str]] = []

    def mark_published(self, id_: UUID) -> None:
        self.published.append(id_)

    def mark_failed(self, id_: UUID, *, error: str) -> None:
        self.failed.append((id_, error))


class _FakeRepo:
    def __init__(self, events: list[OutboxEvent]) -> None:
        self._events = events
        self.last_batch: _FakeBatch | None = None

    async def enqueue(
        self,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
        session: AsyncSession | None = None,
    ) -> UUID:
        raise NotImplementedError("not used in drainer tests")

    async def unpublished_stats(self) -> tuple[int, float]:
        return (len(self._events), 0.0)

    async def stuck_count(self, *, max_attempts: int) -> int:
        return sum(1 for e in self._events if e.attempts >= max_attempts)

    def claim_batch(self, *, limit: int, max_attempts: int = 10) -> Any:
        events = self._events
        repo = self

        @asynccontextmanager
        async def _cm() -> AsyncIterator[_FakeBatch]:
            batch = _FakeBatch(events)
            repo.last_batch = batch
            yield batch

        return _cm()


@pytest.mark.asyncio
async def test_drainer_publishes_each_event() -> None:
    events = [
        OutboxEvent(id=uuid4(), topic="t", key="k", payload={"i": i}, attempts=0, created_at=_now())
        for i in range(3)
    ]
    repo = _FakeRepo(events)
    producer = MagicMock()
    producer.emit = AsyncMock()
    drainer = OutboxDrainer(outbox_repo=repo, producer=producer, poll_interval=0.01)
    await drainer._tick()
    assert producer.emit.await_count == 3
    assert repo.last_batch is not None
    assert len(repo.last_batch.published) == 3
    assert len(repo.last_batch.failed) == 0


@pytest.mark.asyncio
async def test_drainer_marks_failed_on_emit_error() -> None:
    events = [
        OutboxEvent(id=uuid4(), topic="t", key="k", payload={}, attempts=0, created_at=_now())
    ]
    repo = _FakeRepo(events)
    producer = MagicMock()
    producer.emit = AsyncMock(side_effect=RuntimeError("kafka down"))
    drainer = OutboxDrainer(outbox_repo=repo, producer=producer, poll_interval=0.01)
    await drainer._tick()
    assert repo.last_batch is not None
    assert len(repo.last_batch.published) == 0
    assert len(repo.last_batch.failed) == 1
    assert "kafka down" in repo.last_batch.failed[0][1]


@pytest.mark.asyncio
async def test_drainer_skips_stuck_events() -> None:
    events = [
        OutboxEvent(id=uuid4(), topic="t", key="k", payload={}, attempts=10, created_at=_now())
    ]
    repo = _FakeRepo(events)
    producer = MagicMock()
    producer.emit = AsyncMock()
    drainer = OutboxDrainer(
        outbox_repo=repo, producer=producer, poll_interval=0.01, max_attempts=10
    )
    await drainer._tick()
    producer.emit.assert_not_awaited()


@pytest.mark.asyncio
async def test_drainer_run_stops_on_event() -> None:
    repo = _FakeRepo([])
    producer = MagicMock()
    producer.emit = AsyncMock()
    drainer = OutboxDrainer(outbox_repo=repo, producer=producer, poll_interval=0.01)
    task = asyncio.create_task(drainer.run())
    await asyncio.sleep(0.05)
    drainer.stop()
    await asyncio.wait_for(task, timeout=1.0)
