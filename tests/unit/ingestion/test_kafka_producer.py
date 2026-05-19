from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest
from sentinel.ingestion.kafka_producer import KafkaProducer


@pytest.mark.asyncio
async def test_emit_serializes_and_calls_send() -> None:
    with patch("sentinel.ingestion.kafka_producer.AIOKafkaProducer") as MockCls:
        mock = MockCls.return_value
        mock.start = AsyncMock()
        mock.stop = AsyncMock()
        mock.send_and_wait = AsyncMock()

        producer = KafkaProducer("localhost:9092")
        await producer.start()

        await producer.emit(
            topic="sentinel.incidents",
            key="abc",
            payload={"event": "incident.opened"},
        )

        mock.send_and_wait.assert_awaited_once()
        # Accept either keyword or positional form
        call = mock.send_and_wait.await_args
        topic_arg = call.kwargs.get("topic") or call.args[0]
        value_arg = call.kwargs.get("value")
        if value_arg is None and len(call.args) >= 2:
            value_arg = call.args[1]
        assert topic_arg == "sentinel.incidents"
        assert json.loads(value_arg) == {"event": "incident.opened"}


@pytest.mark.asyncio
async def test_start_stop_lifecycle() -> None:
    with patch("sentinel.ingestion.kafka_producer.AIOKafkaProducer") as MockCls:
        mock = MockCls.return_value
        mock.start = AsyncMock()
        mock.stop = AsyncMock()
        producer = KafkaProducer("localhost:9092")
        await producer.start()
        await producer.stop()
        mock.start.assert_awaited_once()
        mock.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_emit_before_start_raises() -> None:
    producer = KafkaProducer("localhost:9092")
    with pytest.raises(RuntimeError):
        await producer.emit(topic="t", key="k", payload={"a": 1})


@pytest.mark.asyncio
async def test_stop_when_not_started_is_noop() -> None:
    producer = KafkaProducer("localhost:9092")
    # Should not raise
    await producer.stop()
