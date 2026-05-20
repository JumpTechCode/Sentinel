# 0006 — Local embeddings via fastembed (bge-large-en-v1.5)

**Status:** Accepted
**Date:** 2026-05-19

## Context

Sentinel needs a 1024-dim text embedding provider for the memory & retrieval loop (Work Area H). The spec (§H deliverables) suggests Voyage AI, Anthropic, or OpenAI. Anthropic does not currently expose an embeddings endpoint. Voyage AI and OpenAI both require paid API keys.

This repo is a public portfolio. Reviewers must be able to clone it, run `docker compose up`, and see the full system work — including similar-incident retrieval — without provisioning any third-party API credentials.

## Decision

Use the `fastembed` Python library with the `BAAI/bge-large-en-v1.5` model. Properties:

- **Local** — runs ONNX-format model in-process on CPU. No network calls at inference time.
- **High quality** — MTEB average ~64 on retrieval tasks, comparable to or better than OpenAI's `text-embedding-3-small` for English technical text.
- **1024-dim** — matches the migration-0005 column shape.
- **No torch dependency** — fastembed pulls only `onnxruntime` (~50MB), keeping install footprint reasonable.

The provider lives behind the existing `EmbeddingProvider` Protocol in `sentinel/enrichment/protocols.py`; a future remote-provider impl is a one-file change.

## Consequences

### Positive

- Zero API keys required for the public demo.
- Zero per-call cost; evals can run nightly in CI without budget concerns.
- Deterministic output (fixed model, no temperature) — reproducible retrieval rankings.
- No network dependency on a third-party LLM provider for embedding compute.

### Negative

- ~500MB install footprint (fastembed + onnxruntime + ~400MB model weights). Mitigated by pre-downloading the model into the Docker image (`Dockerfile` step in Task 12) so cold-start is offline-fast.
- ~80-150ms CPU inference per call. Acceptable because the embedding runs off the request path (`MemoryConsumer` after Kafka, not in the resolve handler).
- bge-large-en-v1.5 produces 1024-dim vectors, not the spec's originally-aspirational 1536. Migration 0005 alters the existing `Vector(1536)` columns (which were empty) to `Vector(1024)`.

### Migration story to a remote provider

If we later want to swap to a paid embedding provider (e.g., OpenAI `text-embedding-3-large` for higher quality):

1. Add a new `OpenAIEmbeddings` class implementing the same `EmbeddingProvider` Protocol.
2. Introduce a `Settings.embedding_provider` switch between `"local"` and `"openai"` (a future pluggable layer; not present in V1 — today only the local provider is wired).
3. If the new provider produces a different dimension, another migration changes the column shape — same drop+recreate pattern as 0005.

The Protocol seam means no other code changes.

### Model-name knob

`Settings.embedding_model_name` is plumbed through `FastEmbedProvider(model_name=...)`. The default (`BAAI/bge-large-en-v1.5`) matches the model pre-downloaded into the Docker image, so the out-of-the-box install is offline-fast. Changing the setting selects a different fastembed-supported model; on first use the runtime downloads it into `embedding_model_cache_dir`, which is slow on cold start and requires network access. For reproducible CI/demo, leave the default in place and rebuild the image rather than overriding at runtime.

## Alternatives rejected

- **OpenAI `text-embedding-3-small`** (1536-dim, natively the right shape). Rejected because it requires a paid API key, which contradicts the public-portfolio goal.
- **Voyage AI `voyage-3`** (1024-dim, slightly higher MTEB than bge-large). Rejected for the same reason.
- **`sentence-transformers` library** with the same bge-large model. Rejected because the install footprint is ~2GB (pulls torch). fastembed delivers the same model via ONNX without torch.
- **`bge-small-en-v1.5`** (384-dim, ~130MB model, MTEB ~62). Rejected because bge-large's quality gain is worth the extra ~270MB on a portfolio project. If install footprint becomes a real problem, swapping to bge-small is a one-line change + a column-dim migration.
