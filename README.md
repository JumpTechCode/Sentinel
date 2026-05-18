# Sentinel

Sentinel is an AI on-call copilot: it ingests production alerts, assembles incident context in parallel from your existing tooling, and produces an evidence-cited diagnosis with confidence — single-shot, deterministic, schema-validated. It learns from resolutions via a pgvector incident memory.

This repo is scaffolding for a portfolio-grade production system; see `docs/plans/` for the implementation roadmap.

## Local development

### Requirements
- Python 3.12
- Docker (for the local stack: Postgres 16 + pgvector, Redis 7, Kafka 3.7)
- GNU Make

### Bootstrap

```bash
make bootstrap          # create .venv and install runtime + dev deps
cp .env.example .env    # fill in SENTINEL_ANTHROPIC_API_KEY
make compose-up         # boot Postgres, Redis, Kafka, app, worker
curl localhost:8000/healthz   # → {"status":"ok"}
make compose-down       # tear down
```

### Quality gates

```bash
make lint               # ruff format check + lint
make typecheck          # mypy --strict
make test               # unit tests
make test-integration   # integration tests (needs docker)
```

CI runs `lint`, `typecheck`, unit tests, integration tests, and `evals-smoke` on
every PR; a nightly workflow runs the full eval corpus.