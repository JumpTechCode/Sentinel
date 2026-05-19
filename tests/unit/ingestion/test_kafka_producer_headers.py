"""KafkaProducer.emit forwards headers kwarg to aiokafka."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from sentinel.ingestion.kafka_producer import KafkaProducer


@pytest.mark.asyncio
async def test_emit_forwards_headers() -> None:
    kp = KafkaProducer(brokers="ignored")
    fake = AsyncMock()
    kp._producer = fake
    headers: list[tuple[str, bytes]] = [("traceparent", b"00-abc-def-01")]
    await kp.emit(
        topic="t",
        key="k",
        payload={"a": 1},
        headers=headers,
    )
    kwargs: dict[str, Any] = fake.send_and_wait.call_args.kwargs
    assert kwargs.get("headers") == headers


@pytest.mark.asyncio
async def test_emit_without_headers_omits_kwarg() -> None:
    kp = KafkaProducer(brokers="ignored")
    fake = AsyncMock()
    kp._producer = fake
    await kp.emit(topic="t", key="k", payload={"a": 1})
    kwargs: dict[str, Any] = fake.send_and_wait.call_args.kwargs
    assert "headers" not in kwargs or kwargs["headers"] is None
