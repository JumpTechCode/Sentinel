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


class RedisIdempotencyStore:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def check_and_mark(self, source: str, body: bytes) -> bool:
        key = f"webhook:{source}:{hashlib.sha256(body).hexdigest()}"
        # NX EX semantics: SET if not exists, with expiry.
        # Returns True on first set, None if already present.
        result = await self._redis.set(key, b"1", nx=True, ex=_TTL_SECONDS)
        return result is None
