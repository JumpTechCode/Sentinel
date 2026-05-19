import dataclasses
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sentinel.persistence.repositories import (
    OutboxBatch,
    OutboxEvent,
)


def _now() -> datetime:
    return datetime.now(UTC)


def test_outbox_event_dataclass_is_frozen() -> None:
    event = OutboxEvent(
        id=uuid4(),
        topic="t",
        key="k",
        payload={"a": 1},
        attempts=0,
        created_at=_now(),
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        event.topic = "x"  # type: ignore[misc]  # frozen — verified at runtime


def test_outbox_batch_records_published_and_failed() -> None:
    session = MagicMock()
    e1 = OutboxEvent(id=uuid4(), topic="t", key="k", payload={}, attempts=0, created_at=_now())
    e2 = OutboxEvent(id=uuid4(), topic="t", key="k", payload={}, attempts=0, created_at=_now())
    batch = OutboxBatch(session, [e1, e2])
    batch.mark_published(e1.id)
    batch.mark_failed(e2.id, error="boom")
    # Implementation detail check: internal lists track entries (used by __aexit__)
    assert e1.id in batch._published
    assert any(e2.id == fid for fid, _ in batch._failed)
