# sentinel/memory/deps.py
"""Dependency bundle for MemoryConsumer.

Constructed once in app.py lifespan and reused across events.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentinel.enrichment.protocols import EmbeddingProvider
    from sentinel.memory.pipeline import MemoryPipeline
    from sentinel.persistence.repositories import IncidentRepository


@dataclass(frozen=True, slots=True)
class MemoryConsumerDeps:
    incident_repo: IncidentRepository
    embedding_provider: EmbeddingProvider
    pipeline: MemoryPipeline
