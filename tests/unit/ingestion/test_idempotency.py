from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from sentinel.ingestion.idempotency import RedisIdempotencyStore


@pytest_asyncio.fixture
async def redis() -> AsyncIterator[FakeRedis]:
    r = FakeRedis()
    try:
        yield r
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_first_body_is_not_duplicate(redis: FakeRedis) -> None:
    store = RedisIdempotencyStore(redis)
    assert await store.check_and_mark("sentry", b'{"a":1}') is False


@pytest.mark.asyncio
async def test_second_body_is_duplicate(redis: FakeRedis) -> None:
    store = RedisIdempotencyStore(redis)
    body = b'{"a":1}'
    assert await store.check_and_mark("sentry", body) is False
    assert await store.check_and_mark("sentry", body) is True


@pytest.mark.asyncio
async def test_different_body_is_not_duplicate(redis: FakeRedis) -> None:
    store = RedisIdempotencyStore(redis)
    assert await store.check_and_mark("sentry", b'{"a":1}') is False
    assert await store.check_and_mark("sentry", b'{"a":2}') is False


@pytest.mark.asyncio
async def test_different_source_is_not_duplicate(redis: FakeRedis) -> None:
    store = RedisIdempotencyStore(redis)
    body = b'{"a":1}'
    assert await store.check_and_mark("sentry", body) is False
    assert await store.check_and_mark("datadog", body) is False


@pytest.mark.asyncio
async def test_ttl_is_24h(redis: FakeRedis) -> None:
    store = RedisIdempotencyStore(redis)
    await store.check_and_mark("sentry", b'{"a":1}')
    keys = await redis.keys("webhook:sentry:*")
    assert len(keys) == 1
    ttl = await redis.ttl(keys[0])
    assert 86399 <= ttl <= 86400
