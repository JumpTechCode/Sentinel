# tests/unit/memory/test_embeddings.py
"""FastEmbedProvider unit tests — lazy load, dim, determinism, timeout."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from sentinel.memory.embeddings import FastEmbedProvider

pytestmark = pytest.mark.asyncio

# Module-scoped cache dir so the model is downloaded once per test-session
# (not once per test). The directory persists across runs so subsequent runs
# benefit from the on-disk cache. Tests still get a fresh FastEmbedProvider
# instance each time; they share only the downloaded model files.
_MODEL_CACHE = Path(__file__).parent.parent.parent.parent / ".test-model-cache"


@pytest.fixture(scope="module")
def cache_dir() -> Path:
    _MODEL_CACHE.mkdir(parents=True, exist_ok=True)
    return _MODEL_CACHE


async def test_embed_returns_1024_dim_vector(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    vec = await provider.embed("hello world")
    assert isinstance(vec, list)
    assert len(vec) == 1024
    assert all(isinstance(x, float) for x in vec)


async def test_embed_is_deterministic(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    a = await provider.embed("same input")
    b = await provider.embed("same input")
    assert a == b


async def test_embed_different_inputs_differ(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    a = await provider.embed("first input text")
    b = await provider.embed("second different text")
    assert a != b


async def test_dim_class_attribute_matches_output(cache_dir: Path) -> None:
    provider = FastEmbedProvider(model_cache_dir=cache_dir)
    vec = await provider.embed("test")
    assert FastEmbedProvider.DIM == len(vec)


async def test_timeout_raises_timeouterror(
    cache_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """embed() raises TimeoutError when _embed_sync exceeds compute_timeout_s.

    Patches _embed_sync to sleep past the timeout, isolating the asyncio.wait_for
    boundary from real model inference speed (which can dip below a millisecond
    once the ONNX JIT is warm).
    """
    import time as time_mod

    def slow_embed(text: str) -> list[float]:
        time_mod.sleep(0.5)
        return [0.0] * FastEmbedProvider.DIM

    provider = FastEmbedProvider(model_cache_dir=cache_dir, compute_timeout_s=0.01)
    monkeypatch.setattr(provider, "_embed_sync", slow_embed)
    with pytest.raises(asyncio.TimeoutError):
        await provider.embed("anything")
