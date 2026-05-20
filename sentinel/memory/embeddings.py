# sentinel/memory/embeddings.py
"""Local CPU embedding provider via fastembed (ONNX, no torch, no API keys).

Concrete impl of EmbeddingProvider Protocol (declared in enrichment/protocols.py).
Lazy-loads the bge-large-en-v1.5 model on first call; subsequent calls are
~80-150ms on CPU. Wrapped in run_in_executor so the model call doesn't block
the event loop; bounded by an asyncio timeout so a pathological input can't
stall a consumer.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastembed import TextEmbedding

_LOG = logging.getLogger("sentinel.memory.embeddings")


class FastEmbedProvider:
    DIM: int = 1024
    MODEL_NAME: str = "BAAI/bge-large-en-v1.5"

    def __init__(
        self,
        *,
        model_cache_dir: Path,
        compute_timeout_s: float = 5.0,
        model_name: str = MODEL_NAME,
    ) -> None:
        # `model_name` overrides the default so `Settings.embedding_model_name`
        # is an honest knob (was previously dead config). The Dockerfile pre-
        # downloads MODEL_NAME; selecting a different model requires either
        # re-baking the image or accepting a one-time download on cold start.
        self._cache_dir = Path(model_cache_dir)
        self._timeout_s = compute_timeout_s
        self._model_name = model_name
        self._model: TextEmbedding | None = None
        self._lock = asyncio.Lock()

    async def _ensure_loaded(self) -> None:
        async with self._lock:
            if self._model is not None:
                return
            from fastembed import TextEmbedding

            _LOG.info(
                "loading_embedding_model",
                extra={"model": self._model_name, "cache_dir": str(self._cache_dir)},
            )
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            self._model = TextEmbedding(model_name=self._model_name, cache_dir=str(self._cache_dir))

    def _embed_sync(self, text: str) -> list[float]:
        if self._model is None:
            raise RuntimeError("model not loaded; call _ensure_loaded() first")
        for arr in self._model.embed([text]):
            return [float(x) for x in arr]
        raise RuntimeError("fastembed produced no output")

    async def embed(self, text: str) -> list[float]:
        await self._ensure_loaded()
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(None, self._embed_sync, text),
            timeout=self._timeout_s,
        )
