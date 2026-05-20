# tests/unit/memory/test_store.py
"""PgVectorIncidentStore unit tests — embed-failure path and repo-failure path.

DB-level top_k correctness is exercised in tests/integration/memory/test_store_e2e.py.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sentinel.memory.store import PgVectorIncidentStore

pytestmark = pytest.mark.asyncio


class _FakeEmbedder:
    def __init__(self, *, raises: Exception | None = None) -> None:
        self._raises = raises

    async def embed(self, text: str) -> list[float]:
        if self._raises:
            raise self._raises
        return [0.1] * 1024


async def test_top_k_returns_failed_on_embed_exception() -> None:
    embedder = _FakeEmbedder(raises=RuntimeError("model crashed"))
    store = PgVectorIncidentStore(
        incident_repo=AsyncMock(),
        embedding_provider=embedder,
    )
    result = await store.top_k(query_text="anything", k=5, exclude_incident_id=None)
    assert result.status == "failed"
    assert result.error is not None
    assert "RuntimeError" in result.error
    assert result.data == []


async def test_top_k_returns_failed_on_embed_timeout() -> None:
    embedder = _FakeEmbedder(raises=TimeoutError())
    store = PgVectorIncidentStore(
        incident_repo=AsyncMock(),
        embedding_provider=embedder,
    )
    result = await store.top_k(query_text="anything", k=5, exclude_incident_id=None)
    assert result.status == "failed"
    assert result.error is not None
    assert "TimeoutError" in result.error


async def test_top_k_returns_failed_on_repo_exception() -> None:
    embedder = _FakeEmbedder()  # embed succeeds
    repo = AsyncMock()
    repo.similar_resolved_incidents = AsyncMock(side_effect=RuntimeError("db down"))
    store = PgVectorIncidentStore(
        incident_repo=repo,
        embedding_provider=embedder,
    )
    result = await store.top_k(query_text="anything", k=5, exclude_incident_id=None)
    assert result.status == "failed"
    assert result.error is not None
    assert "RuntimeError" in result.error
