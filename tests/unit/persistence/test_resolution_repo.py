# tests/unit/persistence/test_resolution_repo.py
"""PostgresResolutionRepository unit tests — exception paths via fake session.

Atomic-commit + outbox-payload behavior is covered in the integration tests.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sentinel.persistence.errors import IncidentAlreadyResolved, IncidentNotFound
from sentinel.persistence.repositories import PostgresResolutionRepository
from sentinel.schemas.api import ResolveIncidentRequest

pytestmark = pytest.mark.asyncio


def _body() -> ResolveIncidentRequest:
    return ResolveIncidentRequest(
        root_cause="rc",
        remediation="rm",
        category="data",
        diagnosis_was_correct=True,
    )


def _make_session_factory(*, lock_returns: Any) -> Any:
    """Fake async_sessionmaker yielding a session whose first execute()
    returns a result whose scalar_one_or_none() returns lock_returns."""
    session = MagicMock()
    first_result = MagicMock()
    first_result.scalar_one_or_none.return_value = lock_returns
    session.execute = AsyncMock(return_value=first_result)
    session.add = MagicMock()

    @asynccontextmanager
    async def _begin() -> Any:
        yield

    session.begin = _begin

    @asynccontextmanager
    async def factory() -> Any:
        yield session

    return factory, session


async def test_raises_incident_not_found_when_missing() -> None:
    factory, _ = _make_session_factory(lock_returns=None)
    repo = PostgresResolutionRepository(session_factory=factory)
    with pytest.raises(IncidentNotFound):
        await repo.record(uuid4(), _body(), outbox_topic="t")


async def test_raises_incident_already_resolved_when_status_resolved() -> None:
    incident_row = SimpleNamespace(status="resolved")
    factory, _ = _make_session_factory(lock_returns=incident_row)
    repo = PostgresResolutionRepository(session_factory=factory)
    with pytest.raises(IncidentAlreadyResolved):
        await repo.record(uuid4(), _body(), outbox_topic="t")


async def test_raises_incident_already_resolved_when_status_closed() -> None:
    incident_row = SimpleNamespace(status="closed")
    factory, _ = _make_session_factory(lock_returns=incident_row)
    repo = PostgresResolutionRepository(session_factory=factory)
    with pytest.raises(IncidentAlreadyResolved):
        await repo.record(uuid4(), _body(), outbox_topic="t")
