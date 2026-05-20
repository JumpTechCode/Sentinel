# sentinel/memory/store.py
"""pgvector-backed SimilarIncidentRetrieval — wraps the IncidentRepository.

The actual SQL lives in sentinel/persistence/repositories.py per the project's
architectural rule (only persistence/ writes SQL). This module:
  1. embeds the query text (own failure → degraded result),
  2. delegates the cosine top-k to incident_repo.similar_resolved_incidents,
  3. wraps results into SimilarIncidentItem + FetcherResult.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sentinel.observability.metrics import (
    similar_incidents_query_duration_seconds,
    similar_incidents_returned_total,
)
from sentinel.schemas.context import FetcherResult, SimilarIncidentItem
from sentinel.schemas.ids import similar_id

if TYPE_CHECKING:
    from sentinel.enrichment.protocols import EmbeddingProvider
    from sentinel.persistence.repositories import IncidentRepository

_LOG = logging.getLogger("sentinel.memory.store")


class PgVectorIncidentStore:
    """Concrete SimilarIncidentRetrieval — delegates SQL to IncidentRepository."""

    def __init__(
        self,
        *,
        incident_repo: IncidentRepository,
        embedding_provider: EmbeddingProvider,
    ) -> None:
        self._repo = incident_repo
        self._embedder = embedding_provider

    async def top_k(
        self,
        *,
        query_text: str,
        k: int,
        exclude_incident_id: UUID | None,
    ) -> FetcherResult[SimilarIncidentItem]:
        try:
            query_vec = await self._embedder.embed(query_text)
        except Exception as e:
            _LOG.warning(
                "similar_incidents_embed_failed",
                extra={"err": repr(e), "exc_type": type(e).__name__},
            )
            return FetcherResult(
                status="failed",
                data=[],
                error=f"embed_failed: {type(e).__name__}",
                fetched_at=datetime.now(UTC),
            )

        start = time.monotonic()
        try:
            rows = await self._repo.similar_resolved_incidents(
                query_embedding=query_vec,
                k=k,
                exclude_incident_id=exclude_incident_id,
            )
        except Exception as e:
            _LOG.warning(
                "similar_incidents_query_failed",
                extra={"err": repr(e), "exc_type": type(e).__name__},
            )
            return FetcherResult(
                status="failed",
                data=[],
                error=f"query_failed: {type(e).__name__}",
                fetched_at=datetime.now(UTC),
            )
        finally:
            similar_incidents_query_duration_seconds.observe(time.monotonic() - start)

        items = [
            SimilarIncidentItem(
                id=similar_id(r.id),
                title=r.title,
                root_cause=r.root_cause,
                remediation=r.remediation,
                cosine_similarity=r.cosine_similarity,
            )
            for r in rows
        ]
        similar_incidents_returned_total.observe(len(items))
        return FetcherResult(
            status="ok",
            data=items,
            error=None,
            fetched_at=datetime.now(UTC),
        )
