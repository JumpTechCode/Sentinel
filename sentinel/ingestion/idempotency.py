"""Redis-backed webhook body idempotency.

Key: webhook:{source}:{sha256(body).hex()}. TTL: 24h. Set semantics: NX.
"""

from __future__ import annotations

import hashlib
from typing import Protocol

from redis.asyncio import Redis

_TTL_SECONDS = 24 * 60 * 60


class IdempotencyStore(Protocol):
    async def check_and_mark(self, source: str, body: bytes) -> bool:
        """Return True if (source, body) was already seen; False if first."""
        ...

    async def unmark(self, source: str, body: bytes) -> None:
        """Drop the idempotency key so a retry of (source, body) re-processes.

        Compensating action when persistence fails *after* the key was marked —
        without it, the sender's byte-identical retry would dedup into silent loss
        (ADR 0001, "never lose an event").
        """
        ...


def _idempotency_key(source: str, body: bytes) -> str:
    return f"webhook:{source}:{hashlib.sha256(body).hexdigest()}"


class RedisIdempotencyStore:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def check_and_mark(self, source: str, body: bytes) -> bool:
        key = _idempotency_key(source, body)
        # NX EX semantics: SET if not exists, with expiry.
        # Returns True on first set, None if already present.
        result = await self._redis.set(key, b"1", nx=True, ex=_TTL_SECONDS)
        return result is None

    async def unmark(self, source: str, body: bytes) -> None:
        await self._redis.delete(_idempotency_key(source, body))
