"""aiokafka producer wrapper.

Lifecycle owned by the FastAPI app via lifespan. The producer is the only
component that talks to Kafka; the webhook handler never imports this module.
"""

from __future__ import annotations

import json
from typing import Any

from aiokafka import AIOKafkaProducer


class KafkaProducer:
    def __init__(self, brokers: str) -> None:
        self._brokers = brokers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        # aiokafka's producer enforces in-flight≤5 internally when
        # enable_idempotence=True; no separate kwarg in this version.
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._brokers,
            enable_idempotence=True,
            acks="all",
            compression_type="gzip",
            linger_ms=10,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def emit(
        self,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaProducer.start() must be awaited before emit()")
        value = json.dumps(payload).encode()
        kwargs: dict[str, Any] = {"value": value, "key": key.encode()}
        if headers is not None:
            kwargs["headers"] = headers
        await self._producer.send_and_wait(topic, **kwargs)
