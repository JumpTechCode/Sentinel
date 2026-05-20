# sentinel/memory/__init__.py
"""Memory & feedback loop — embedding provider, retrieval store, pipeline, consumer."""

from sentinel.memory.consumer import MemoryConsumer
from sentinel.memory.deps import MemoryConsumerDeps
from sentinel.memory.embeddings import FastEmbedProvider
from sentinel.memory.pipeline import MemoryPipeline
from sentinel.memory.store import PgVectorIncidentStore

__all__ = [
    "FastEmbedProvider",
    "MemoryConsumer",
    "MemoryConsumerDeps",
    "MemoryPipeline",
    "PgVectorIncidentStore",
]
