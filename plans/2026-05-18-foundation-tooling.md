# Foundation & Tooling Implementation Plan

> Steps use checkbox (`- [ ]`) syntax for tracking. Each task includes the exact files to touch, the commands to run, and the expected output.

**Goal:** Stand up the Sentinel monorepo skeleton so that `make bootstrap && make lint && make typecheck && make test` is green on a clean checkout, `docker compose up` boots Postgres+pgvector / Redis / Kafka / app / worker, `curl localhost:8000/healthz` returns 200, and CI runs the same gates on every PR. No business logic — just the harness that every later work area depends on.

**Architecture:** Single Python 3.12 package `sentinel/` with module subdirectories that match the spec's module map. `pydantic-settings`-driven config (env + per-env YAML). FastAPI app + Kafka worker as separate process entrypoints. Postgres 16 + pgvector, Redis 7, Kafka in docker-compose with healthchecks. Alembic skeleton (no migrations yet — those land in Work Area B). CI runs lint (ruff) + typecheck (mypy --strict) + unit tests + a placeholder eval-smoke target.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, pydantic-settings, SQLAlchemy 2.x + asyncpg, alembic, redis-py, aiokafka, anthropic, structlog, prometheus-client, opentelemetry-*, httpx, pyyaml. Dev: pytest + pytest-asyncio + pytest-cov, ruff, mypy, testcontainers, locust. Docker Compose. GitHub Actions.

---

## Pre-flight: what already exists

```
.
├── .git/
├── .gitignore
├── LICENSE
├── README.md            # stub
└── docs/plans/          # this plan lives here
```

Every path below is **relative to the repo root** unless prefixed with `/`.

## File structure being built

```
sentinel/                          # the Python package
├── __init__.py                    # version string
├── config/
│   ├── __init__.py
│   └── settings.py                # pydantic-settings Settings class
├── api/
│   ├── __init__.py
│   ├── app.py                     # FastAPI app factory + lifespan
│   └── routes/
│       ├── __init__.py
│       └── health.py              # /healthz, /readyz (placeholders)
├── workers/
│   ├── __init__.py
│   └── diagnosis_worker.py        # placeholder entrypoint (no logic yet)
├── ingestion/__init__.py          # empty packages for later work areas
├── enrichment/__init__.py
├── diagnosis/__init__.py
├── memory/__init__.py
├── integrations/__init__.py
├── persistence/__init__.py
├── observability/__init__.py
├── schemas/__init__.py
└── evals/__init__.py
migrations/
├── env.py                         # alembic async env (no revisions yet)
├── script.py.mako                 # alembic template
└── versions/.gitkeep
config/
├── default.yaml                   # per-env config (loaded by pydantic-settings)
└── test.yaml
tests/
├── __init__.py
├── conftest.py                    # shared fixtures
├── unit/
│   ├── __init__.py
│   ├── test_config.py
│   └── test_health.py
└── integration/__init__.py        # empty for now
.github/workflows/
├── ci.yml
└── nightly-evals.yml
Dockerfile                         # multi-stage, used by docker-compose
docker-compose.yml
.dockerignore
.env.example
.pre-commit-config.yaml
alembic.ini
Makefile
pyproject.toml                     # deps + ruff + mypy + pytest config
README.md                          # add bootstrap section (existing file modified)
```

## Commit boundaries

When you reach a "STOP — notify user" step, output something like:

> Foundation Task N complete. Files added/modified:
>  - path/to/file_1
>  - path/to/file_2
>
> Suggested commit message: `<type>: <subject>`
>
> Please review and commit. I'll wait.

After the user confirms the commit is in, continue with the next task.

---

## Task 1: Package skeleton + namespace `__init__` files

**Files:**
- Create: `sentinel/__init__.py`
- Create: `sentinel/api/__init__.py`
- Create: `sentinel/api/routes/__init__.py`
- Create: `sentinel/config/__init__.py`
- Create: `sentinel/workers/__init__.py`
- Create: `sentinel/ingestion/__init__.py`
- Create: `sentinel/enrichment/__init__.py`
- Create: `sentinel/diagnosis/__init__.py`
- Create: `sentinel/memory/__init__.py`
- Create: `sentinel/integrations/__init__.py`
- Create: `sentinel/persistence/__init__.py`
- Create: `sentinel/observability/__init__.py`
- Create: `sentinel/schemas/__init__.py`
- Create: `sentinel/evals/__init__.py`

- [ ] **Step 1: Create top-level package init**

`sentinel/__init__.py`:
```python
"""Sentinel — AI on-call copilot."""

__version__ = "0.0.0"
```

- [ ] **Step 2: Create empty `__init__.py` for every module subdirectory**

For each of the 13 subdirectories listed in "Files" above (other than the top-level `sentinel/__init__.py`), create an empty file. Example for `sentinel/api/__init__.py`:
```python
```
(literally an empty file)

- [ ] **Step 3: Verify package layout**

Run: `find sentinel -name __init__.py | sort`
Expected output (exactly these 14 lines):
```
sentinel/__init__.py
sentinel/api/__init__.py
sentinel/api/routes/__init__.py
sentinel/config/__init__.py
sentinel/diagnosis/__init__.py
sentinel/enrichment/__init__.py
sentinel/evals/__init__.py
sentinel/ingestion/__init__.py
sentinel/integrations/__init__.py
sentinel/memory/__init__.py
sentinel/observability/__init__.py
sentinel/persistence/__init__.py
sentinel/schemas/__init__.py
sentinel/workers/__init__.py
```

---

## Task 2: `pyproject.toml` — deps + tool configs

**Files:**
- Create: `pyproject.toml`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "sentinel"
version = "0.0.0"
description = "AI on-call copilot — ingests alerts, assembles context, produces evidence-cited diagnoses."
readme = "README.md"
requires-python = ">=3.12,<3.13"
license = { text = "MIT" }
authors = [{ name = "Arik Levinsky" }]

dependencies = [
  "fastapi>=0.115,<0.116",
  "uvicorn[standard]>=0.32,<0.33",
  "pydantic>=2.9,<3",
  "pydantic-settings>=2.6,<3",
  "sqlalchemy[asyncio]>=2.0.36,<2.1",
  "asyncpg>=0.30,<0.31",
  "alembic>=1.14,<1.15",
  "redis>=5.2,<5.3",
  "aiokafka>=0.12,<0.13",
  "anthropic>=0.39,<0.40",
  "httpx>=0.27,<0.28",
  "structlog>=24.4,<25",
  "prometheus-client>=0.21,<0.22",
  "opentelemetry-sdk>=1.28,<2",
  "opentelemetry-exporter-otlp-proto-grpc>=1.28,<2",
  "opentelemetry-instrumentation-fastapi>=0.49b0,<1",
  "pyyaml>=6.0.2,<7",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.3,<9",
  "pytest-asyncio>=0.24,<0.25",
  "pytest-cov>=6.0,<7",
  "ruff>=0.8,<0.9",
  "mypy>=1.13,<2",
  "testcontainers[postgres,redis,kafka]>=4.8,<5",
  "locust>=2.32,<3",
  "types-pyyaml>=6.0,<7",
]

[project.scripts]
sentinel-api = "sentinel.api.app:run"
sentinel-worker = "sentinel.workers.diagnosis_worker:run"

[build-system]
requires = ["setuptools>=75", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["sentinel*"]

[tool.ruff]
line-length = 100
target-version = "py312"
src = ["sentinel", "tests"]

[tool.ruff.lint]
select = [
  "E", "F", "W",     # pycodestyle + pyflakes
  "I",                # isort
  "B",                # bugbear
  "UP",               # pyupgrade
  "SIM",              # simplify
  "TID",              # tidy imports
  "RUF",
]
ignore = [
  "E501",  # line length handled by formatter
]

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["B011", "SIM"]

[tool.ruff.format]
quote-style = "double"

[tool.mypy]
python_version = "3.12"
strict = true
warn_unreachable = true
warn_redundant_casts = true
warn_unused_ignores = true
no_implicit_reexport = true
show_error_codes = true
files = ["sentinel", "tests"]
plugins = ["pydantic.mypy"]

[[tool.mypy.overrides]]
module = ["aiokafka.*", "testcontainers.*", "locust.*"]
ignore_missing_imports = true

[tool.pytest.ini_options]
addopts = "-ra --strict-markers --strict-config"
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
  "integration: requires docker-compose services (postgres, redis, kafka)",
]

[tool.coverage.run]
source = ["sentinel"]
branch = true

[tool.coverage.report]
fail_under = 0  # bumped per work area; A doesn't gate coverage
show_missing = true
```

- [ ] **Step 2: Verify pyproject parses**

Run: `python -c "import tomllib, pathlib; tomllib.loads(pathlib.Path('pyproject.toml').read_text())" && echo OK`
Expected: `OK`

---

## Task 3: `Makefile` — full target set

**Files:**
- Create: `Makefile`

- [ ] **Step 1: Create `Makefile`**

```makefile
.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV ?= .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: help bootstrap fmt lint typecheck test test-unit test-integration \
        migrate migrate-down evals evals-smoke load chaos \
        compose-up compose-down openapi clean

help:  ## Show available targets
	@awk 'BEGIN{FS=":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*##/ {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

$(VENV)/bin/activate:
	python3.12 -m venv $(VENV)
	$(PIP) install --upgrade pip wheel

bootstrap: $(VENV)/bin/activate  ## Create venv and install dev+runtime deps
	$(PIP) install -e ".[dev]"
	$(VENV)/bin/pre-commit install || true

fmt:  ## Format with ruff
	$(VENV)/bin/ruff format sentinel tests
	$(VENV)/bin/ruff check --fix sentinel tests

lint:  ## Lint with ruff (check only)
	$(VENV)/bin/ruff format --check sentinel tests
	$(VENV)/bin/ruff check sentinel tests

typecheck:  ## mypy --strict over sentinel + tests
	$(VENV)/bin/mypy

test: test-unit  ## Default = unit tests

test-unit:  ## pytest unit tests
	$(VENV)/bin/pytest tests/unit -v

test-integration:  ## pytest integration tests (requires docker)
	$(VENV)/bin/pytest tests/integration -v -m integration

migrate:  ## alembic upgrade head
	$(VENV)/bin/alembic upgrade head

migrate-down:  ## alembic downgrade -1
	$(VENV)/bin/alembic downgrade -1

evals:  ## Full eval corpus (placeholder until Work Area K)
	@echo "evals: not implemented yet (Work Area K)"; exit 0

evals-smoke:  ## 5-case smoke set (placeholder until Work Area K)
	@echo "evals-smoke: not implemented yet (Work Area K)"; exit 0

load:  ## Locust load test (placeholder until Work Area M)
	@echo "load: not implemented yet (Work Area M)"; exit 0

chaos:  ## Toxiproxy chaos test (placeholder until Work Area M)
	@echo "chaos: not implemented yet (Work Area M)"; exit 0

compose-up:  ## docker compose up -d
	docker compose up -d --wait

compose-down:  ## docker compose down -v
	docker compose down -v

openapi:  ## Export OpenAPI JSON (placeholder until Work Area I)
	@echo "openapi: not implemented yet (Work Area I)"; exit 0

clean:  ## Remove venv and caches
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
```

- [ ] **Step 2: Verify make targets list**

Run: `make help`
Expected: a list of all targets above with their descriptions.

---

## Task 4: `.env.example` + `sentinel/config/settings.py`

**Files:**
- Create: `.env.example`
- Create: `sentinel/config/settings.py`
- Create: `config/default.yaml`
- Create: `config/test.yaml`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/unit/test_config.py`

- [ ] **Step 1: Write the failing test**

`tests/__init__.py`:
```python
```
(empty)

`tests/unit/__init__.py`:
```python
```
(empty)

`tests/integration/__init__.py`:
```python
```
(empty)

`tests/unit/test_config.py`:
```python
from sentinel.config.settings import Settings


def test_settings_load_from_env(monkeypatch) -> None:
    monkeypatch.setenv("SENTINEL_ENV", "test")
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://u:p@h:5432/d")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test")

    settings = Settings()  # type: ignore[call-arg]

    assert settings.env == "test"
    assert "asyncpg" in settings.postgres_dsn
    assert settings.anthropic_model == "claude-sonnet-4-5"


def test_settings_rejects_unknown_env(monkeypatch) -> None:
    monkeypatch.setenv("SENTINEL_ENV", "bogus")
    monkeypatch.setenv("SENTINEL_POSTGRES_DSN", "postgresql+asyncpg://u:p@h:5432/d")
    monkeypatch.setenv("SENTINEL_REDIS_URL", "redis://h:6379/0")
    monkeypatch.setenv("SENTINEL_KAFKA_BROKERS", "kafka:9092")
    monkeypatch.setenv("SENTINEL_ANTHROPIC_API_KEY", "sk-test")

    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make bootstrap` (one-time)
Then: `.venv/bin/pytest tests/unit/test_config.py -v`
Expected: `ModuleNotFoundError: No module named 'sentinel.config.settings'`

- [ ] **Step 3: Write the Settings class**

`sentinel/config/settings.py`:
```python
"""Application settings — loaded from environment via pydantic-settings.

Per-env YAML files in `config/{env}.yaml` provide defaults; env vars override.
"""
from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

Env = Literal["dev", "test", "prod"]


def _yaml_defaults(env: Env) -> dict[str, object]:
    path = Path(__file__).resolve().parents[2] / "config" / f"{env}.yaml"
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text()) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must be a YAML mapping at the top level")
    return loaded


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SENTINEL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: Env = "dev"

    # Persistence
    postgres_dsn: str = Field(..., description="async SQLAlchemy DSN, e.g. postgresql+asyncpg://...")
    redis_url: str = Field(..., description="redis://host:port/db")
    kafka_brokers: str = Field(..., description="comma-separated host:port list")
    kafka_topic_incidents: str = "sentinel.incidents"

    # LLM
    anthropic_api_key: SecretStr = Field(..., description="Anthropic API key")
    anthropic_model: str = "claude-sonnet-4-5"

    # Observability
    log_level: str = "INFO"
    otel_endpoint: str | None = None
    llm_audit_log_path: str = "logs/llm-audit.log"

    # HTTP server
    http_host: str = "0.0.0.0"
    http_port: int = 8000


def load_settings() -> Settings:
    """Load Settings, layering YAML defaults under env vars."""
    import os

    env: Env = os.environ.get("SENTINEL_ENV", "dev")  # type: ignore[assignment]
    defaults = _yaml_defaults(env)
    return Settings(**defaults)  # type: ignore[arg-type]
```

- [ ] **Step 4: Write the YAML defaults**

`config/default.yaml`:
```yaml
# Defaults loaded for env=dev when env vars are absent.
# Real secrets must come from environment, not from this file.
kafka_topic_incidents: sentinel.incidents
log_level: INFO
http_host: 0.0.0.0
http_port: 8000
llm_audit_log_path: logs/llm-audit.log
```

`config/test.yaml`:
```yaml
log_level: WARNING
http_port: 8000
```

- [ ] **Step 5: Write `.env.example`**

`.env.example`:
```bash
# Sentinel — copy to .env and fill in. NEVER commit .env.

# Runtime mode: dev | test | prod
SENTINEL_ENV=dev

# Persistence
SENTINEL_POSTGRES_DSN=postgresql+asyncpg://sentinel:sentinel@localhost:5432/sentinel
SENTINEL_REDIS_URL=redis://localhost:6379/0
SENTINEL_KAFKA_BROKERS=localhost:9092
SENTINEL_KAFKA_TOPIC_INCIDENTS=sentinel.incidents

# LLM
SENTINEL_ANTHROPIC_API_KEY=sk-ant-...
SENTINEL_ANTHROPIC_MODEL=claude-sonnet-4-5

# Observability
SENTINEL_LOG_LEVEL=INFO
SENTINEL_OTEL_ENDPOINT=
SENTINEL_LLM_AUDIT_LOG_PATH=logs/llm-audit.log

# HTTP server
SENTINEL_HTTP_HOST=0.0.0.0
SENTINEL_HTTP_PORT=8000
```

- [ ] **Step 6: Run test to verify it passes**

Run: `.venv/bin/pytest tests/unit/test_config.py -v`
Expected: 2 passed.

- [ ] **Step 7: Run typecheck and lint**

Run: `make lint && make typecheck`
Expected: both green.

---

## Task 5: Healthz endpoint + FastAPI app factory

**Files:**
- Create: `sentinel/api/app.py`
- Create: `sentinel/api/routes/health.py`
- Create: `tests/unit/test_health.py`

- [ ] **Step 1: Write the failing test**

`tests/unit/test_health.py`:
```python
from fastapi.testclient import TestClient

from sentinel.api.app import build_app


def test_healthz_returns_ok() -> None:
    app = build_app()
    client = TestClient(app)
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"status": "ok"}


def test_readyz_reports_dependencies() -> None:
    app = build_app()
    client = TestClient(app)
    resp = client.get("/readyz")
    # In A we only assert the shape; real dep checks land in Work Area I.
    assert resp.status_code in (200, 503)
    body = resp.json()
    assert "status" in body
    assert "checks" in body
    assert isinstance(body["checks"], dict)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/unit/test_health.py -v`
Expected: `ModuleNotFoundError: No module named 'sentinel.api.app'`

- [ ] **Step 3: Write the health routes**

`sentinel/api/routes/health.py`:
```python
"""Liveness + readiness endpoints.

Liveness (`/healthz`) returns 200 unconditionally — the process is up.
Readiness (`/readyz`) will gain real dependency checks (pg, redis, kafka)
in Work Area I. For now it returns a 200 with an empty `checks` map so the
shape is stable for clients.
"""
from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz")
async def readyz() -> dict[str, object]:
    checks: dict[str, str] = {}
    return {"status": "ok", "checks": checks}
```

- [ ] **Step 4: Write the app factory**

`sentinel/api/app.py`:
```python
"""FastAPI application factory and uvicorn entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from sentinel import __version__
from sentinel.api.routes.health import router as health_router


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    # Work Area I will start Kafka producer / consumer here.
    yield


def build_app() -> FastAPI:
    app = FastAPI(
        title="Sentinel",
        version=__version__,
        description="AI on-call copilot — alert ingestion, context assembly, evidence-cited diagnosis.",
        lifespan=lifespan,
    )
    app.include_router(health_router)
    return app


def run() -> None:
    import uvicorn

    from sentinel.config.settings import load_settings

    settings = load_settings()
    uvicorn.run(
        "sentinel.api.app:build_app",
        factory=True,
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `.venv/bin/pytest tests/unit/test_health.py -v`
Expected: 2 passed.

- [ ] **Step 6: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: green.

---

## Task 6: Worker entrypoint stub

**Files:**
- Create: `sentinel/workers/diagnosis_worker.py`

- [ ] **Step 1: Write the stub entrypoint**

`sentinel/workers/diagnosis_worker.py`:
```python
"""Diagnosis worker — Kafka consumer for `incident.enriched`.

Stub for Work Area A. Real consumer lands in Work Area G/I.
The entrypoint exists so docker-compose can declare a `worker` service
that boots cleanly and stays alive.
"""
from __future__ import annotations

import asyncio
import signal
import sys


async def _main() -> None:
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _shutdown() -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    sys.stdout.write("sentinel-worker: idle (stub) — waiting for shutdown\n")
    sys.stdout.flush()
    await stop.wait()


def run() -> None:
    asyncio.run(_main())


if __name__ == "__main__":  # pragma: no cover
    run()
```

- [ ] **Step 2: Verify it boots and exits on SIGTERM**

Run: `timeout 2 .venv/bin/python -m sentinel.workers.diagnosis_worker; echo exit=$?`
Expected: prints `sentinel-worker: idle (stub) — waiting for shutdown`, exits with code 124 (timeout's kill).

- [ ] **Step 3: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: green.

---

## STOP — notify user (commit boundary 1)

Tasks 1–6 form a coherent first commit: package skeleton + config + healthz + worker stub + tests.

Files staged for commit (verify with `git status`):
- `sentinel/` (whole tree)
- `tests/__init__.py`, `tests/unit/__init__.py`, `tests/unit/test_config.py`, `tests/unit/test_health.py`, `tests/integration/__init__.py`
- `config/default.yaml`, `config/test.yaml`
- `pyproject.toml`
- `Makefile`
- `.env.example`

Suggested commit message:
```
chore: scaffold sentinel package, config, healthz, worker stub

Adds the Python package layout matching the spec's module map, pydantic-settings
config (env + per-env YAML), FastAPI app factory with /healthz + /readyz, and
a Kafka worker stub entrypoint. Tooling: pyproject (ruff + mypy strict + pytest),
Makefile with full target set (placeholders for evals/load/chaos until later
work areas land).
```

Notify the user. Do not run `git commit`. Wait for confirmation before continuing.

---

## Task 7: Alembic skeleton

**Files:**
- Create: `alembic.ini`
- Create: `migrations/env.py`
- Create: `migrations/script.py.mako`
- Create: `migrations/versions/.gitkeep`

- [ ] **Step 1: Create `alembic.ini`**

`alembic.ini`:
```ini
[alembic]
script_location = migrations
prepend_sys_path = .
sqlalchemy.url =

# Use sentinel-managed timezone-aware filenames
file_template = %%(year)d%%(month).2d%%(day).2d_%%(hour).2d%%(minute).2d_%%(rev)s_%%(slug)s
timezone = UTC

[post_write_hooks]
hooks = ruff_format
ruff_format.type = console_scripts
ruff_format.entrypoint = ruff
ruff_format.options = format REVISION_SCRIPT_FILENAME

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

- [ ] **Step 2: Create `migrations/env.py`**

`migrations/env.py`:
```python
"""Alembic async env.

Work Area B will register the metadata target (SQLAlchemy declarative Base).
Work Area A only proves the runner boots and connects.
"""
from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from sentinel.config.settings import load_settings

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Inject the DSN from settings into the alembic config at runtime.
_settings = load_settings()
config.set_main_option("sqlalchemy.url", _settings.postgres_dsn)

# Filled in by Work Area B:
target_metadata = None


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    connectable = async_engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
```

- [ ] **Step 3: Create `migrations/script.py.mako`**

`migrations/script.py.mako`:
```
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision: str = ${repr(up_revision)}
down_revision: Union[str, None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
```

- [ ] **Step 4: Create the versions placeholder**

`migrations/versions/.gitkeep`:
```
```
(empty)

- [ ] **Step 5: Verify alembic CLI loads the env**

Run: `.venv/bin/alembic check 2>&1 | head -5 || true`
Expected: alembic loads `env.py` without ImportError. (It may report "No revision files" — that's fine; real migrations land in Work Area B.) The important thing is that the `from sentinel.config.settings import load_settings` import works.

A faster check that doesn't need a DB:
Run: `.venv/bin/python -c "import importlib.util, pathlib; spec = importlib.util.spec_from_file_location('e', 'migrations/env.py'); print('env.py imports OK' if spec else 'fail')"`
Expected: `env.py imports OK` (we don't execute it — execution requires a DSN).

---

## Task 8: `Dockerfile` + `.dockerignore`

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

- [ ] **Step 1: Create `.dockerignore`**

`.dockerignore`:
```
.git
.gitignore
.venv
.mypy_cache
.pytest_cache
.ruff_cache
.coverage
htmlcov
__pycache__
**/__pycache__
*.pyc
.env
.env.*
!.env.example
docs/
docs/
tests/
node_modules
web/.next
web/node_modules
```

- [ ] **Step 2: Create `Dockerfile`**

`Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1.7

FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml ./
COPY sentinel ./sentinel
COPY config ./config
COPY migrations ./migrations
COPY alembic.ini ./

RUN pip install --upgrade pip wheel \
 && pip install -e .

# Default to the API; compose overrides for the worker service.
EXPOSE 8000
CMD ["sentinel-api"]
```

- [ ] **Step 3: Verify the Dockerfile builds**

Run: `docker build -t sentinel:local . 2>&1 | tail -20`
Expected: ends with `Successfully tagged sentinel:local` (or the buildkit equivalent). If docker isn't available locally, skip the build but verify the Dockerfile parses by running: `docker buildx build --dry-run -t sentinel:local . 2>&1 | tail -5 || true`.

---

## Task 9: `docker-compose.yml`

**Files:**
- Create: `docker-compose.yml`

- [ ] **Step 1: Write `docker-compose.yml`**

`docker-compose.yml`:
```yaml
# Sentinel local stack. `make compose-up` waits for healthchecks.
services:
  postgres:
    image: pgvector/pgvector:pg16
    environment:
      POSTGRES_USER: sentinel
      POSTGRES_PASSWORD: sentinel
      POSTGRES_DB: sentinel
    volumes:
      - sentinel_pg:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U sentinel -d sentinel"]
      interval: 2s
      timeout: 3s
      retries: 30

  redis:
    image: redis:7-alpine
    command: ["redis-server", "--appendonly", "yes"]
    volumes:
      - sentinel_redis:/data
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 3s
      retries: 30

  kafka:
    image: bitnami/kafka:3.7
    environment:
      KAFKA_CFG_NODE_ID: "1"
      KAFKA_CFG_PROCESS_ROLES: "broker,controller"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://kafka:9092"
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      ALLOW_PLAINTEXT_LISTENER: "yes"
    ports:
      - "9092:9092"
    volumes:
      - sentinel_kafka:/bitnami/kafka
    healthcheck:
      test: ["CMD-SHELL", "/opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1"]
      interval: 5s
      timeout: 5s
      retries: 30

  app:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["sentinel-api"]
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka:
        condition: service_healthy
    environment:
      SENTINEL_ENV: dev
      SENTINEL_POSTGRES_DSN: postgresql+asyncpg://sentinel:sentinel@postgres:5432/sentinel
      SENTINEL_REDIS_URL: redis://redis:6379/0
      SENTINEL_KAFKA_BROKERS: kafka:9092
      SENTINEL_ANTHROPIC_API_KEY: ${SENTINEL_ANTHROPIC_API_KEY:-sk-placeholder}
      SENTINEL_HTTP_HOST: 0.0.0.0
      SENTINEL_HTTP_PORT: "8000"
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:8000/healthz || exit 1"]
      interval: 3s
      timeout: 3s
      retries: 20

  worker:
    build:
      context: .
      dockerfile: Dockerfile
    command: ["sentinel-worker"]
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    environment:
      SENTINEL_ENV: dev
      SENTINEL_POSTGRES_DSN: postgresql+asyncpg://sentinel:sentinel@postgres:5432/sentinel
      SENTINEL_REDIS_URL: redis://redis:6379/0
      SENTINEL_KAFKA_BROKERS: kafka:9092
      SENTINEL_ANTHROPIC_API_KEY: ${SENTINEL_ANTHROPIC_API_KEY:-sk-placeholder}

volumes:
  sentinel_pg:
  sentinel_redis:
  sentinel_kafka:
```

- [ ] **Step 2: Verify compose file parses**

Run: `docker compose config -q && echo OK`
Expected: `OK` (no validation errors).

- [ ] **Step 3: Bring the stack up and verify `/healthz`**

Run: `make compose-up`
Expected: all services report healthy within ~60s.

Then: `curl -sS http://localhost:8000/healthz`
Expected: `{"status":"ok"}`

Then: `make compose-down`
Expected: stack tears down cleanly with volumes removed.

> If docker is not available in the dev environment running this plan, mark Step 3 as "deferred to local verification by user" in the notify-user message. Do not proceed past Task 11 without somebody running this once.

---

## Task 10: `.pre-commit-config.yaml`

**Files:**
- Create: `.pre-commit-config.yaml`

- [ ] **Step 1: Write `.pre-commit-config.yaml`**

`.pre-commit-config.yaml`:
```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-toml
      - id: check-merge-conflict
      - id: check-added-large-files
        args: ["--maxkb=512"]

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.4
    hooks:
      - id: ruff
        args: ["--fix"]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.13.0
    hooks:
      - id: mypy
        args: ["--config-file=pyproject.toml"]
        additional_dependencies:
          - pydantic>=2.9
          - pydantic-settings>=2.6
          - types-pyyaml
        files: ^(sentinel|tests)/
```

- [ ] **Step 2: Verify pre-commit installs**

Run: `.venv/bin/pre-commit install`
Expected: `pre-commit installed at .git/hooks/pre-commit`

- [ ] **Step 3: Run pre-commit on all files**

Run: `.venv/bin/pre-commit run --all-files`
Expected: all hooks pass. Any auto-fixes applied by ruff are part of the next commit (re-run after fixes).

---

## Task 11: GitHub Actions — `ci.yml` + `nightly-evals.yml`

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/nightly-evals.yml`

- [ ] **Step 1: Write `ci.yml`**

`.github/workflows/ci.yml`:
```yaml
name: ci

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  lint-typecheck-unit:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.12"]
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
          cache: pip
          cache-dependency-path: pyproject.toml

      - name: Install deps
        run: |
          python -m pip install --upgrade pip wheel
          pip install -e ".[dev]"

      - name: Lint (ruff)
        run: |
          ruff format --check sentinel tests
          ruff check sentinel tests

      - name: Typecheck (mypy --strict)
        run: mypy

      - name: Unit tests
        run: pytest tests/unit -v

  integration:
    runs-on: ubuntu-latest
    needs: lint-typecheck-unit
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: pyproject.toml

      - name: Install deps
        run: |
          python -m pip install --upgrade pip wheel
          pip install -e ".[dev]"

      - name: Start compose stack
        run: docker compose up -d --wait postgres redis kafka

      - name: Integration tests
        env:
          SENTINEL_ENV: test
          SENTINEL_POSTGRES_DSN: postgresql+asyncpg://sentinel:sentinel@localhost:5432/sentinel
          SENTINEL_REDIS_URL: redis://localhost:6379/0
          SENTINEL_KAFKA_BROKERS: localhost:9092
          SENTINEL_ANTHROPIC_API_KEY: sk-placeholder
        run: pytest tests/integration -v -m integration

      - name: Tear down
        if: always()
        run: docker compose down -v

  evals-smoke:
    runs-on: ubuntu-latest
    needs: lint-typecheck-unit
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: pyproject.toml
      - run: |
          python -m pip install --upgrade pip wheel
          pip install -e ".[dev]"
      - name: Evals smoke (placeholder until Work Area K)
        run: make evals-smoke
```

- [ ] **Step 2: Write `nightly-evals.yml`**

`.github/workflows/nightly-evals.yml`:
```yaml
name: nightly-evals

on:
  schedule:
    - cron: "0 6 * * *"   # 06:00 UTC nightly
  workflow_dispatch:

jobs:
  evals:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: pyproject.toml
      - run: |
          python -m pip install --upgrade pip wheel
          pip install -e ".[dev]"
      - name: Run full evals (placeholder until Work Area K)
        env:
          SENTINEL_ANTHROPIC_API_KEY: ${{ secrets.SENTINEL_ANTHROPIC_API_KEY }}
        run: make evals
      # Work Area K adds: upload evals/results/, post a comment with diff vs baseline.
```

- [ ] **Step 3: Validate workflow YAML syntactically**

Run: `.venv/bin/python -c "import yaml, pathlib; [yaml.safe_load(p.read_text()) for p in pathlib.Path('.github/workflows').glob('*.yml')]; print('workflows parse OK')"`
Expected: `workflows parse OK`

---

## Task 12: README — bootstrap section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Read existing README**

Run: `cat README.md`
Note the current contents so the bootstrap section is appended cleanly (do not duplicate the title).

- [ ] **Step 2: Append a "Local development" section**

Edit `README.md` to add (or replace, if a placeholder section already exists) the section below. Insert above the License section if one exists.

```markdown
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
```

- [ ] **Step 3: Verify README renders without broken fences**

Run: `awk '/^```/{n++} END{exit (n%2)}' README.md && echo "fences balanced"`
Expected: `fences balanced`

---

## Task 13: Final acceptance — the whole gate is green

- [ ] **Step 1: Wipe and re-bootstrap to simulate a clean clone**

Run: `make clean && make bootstrap`
Expected: venv is created, dependencies install without error.

- [ ] **Step 2: Run the full local gate**

Run: `make lint && make typecheck && make test`
Expected: all green. Note any warnings; if mypy reports unused-ignore or missing-stub errors, fix them at the call site (do not add module-level ignores without a comment naming the library).

- [ ] **Step 3: Verify compose stack still boots end-to-end**

Run: `make compose-up`
Then: `curl -sS http://localhost:8000/healthz`
Expected: `{"status":"ok"}`
Then: `make compose-down`

If docker is not available in the agent environment, mark this step as **"requires user verification"** in the notify-user message.

- [ ] **Step 4: Confirm no business logic snuck in**

Run: `find sentinel -name '*.py' | xargs wc -l | tail -1`
Expected: total line count is small (<400). Foundation should be a thin shell — if it's larger, look for scope creep and trim.

Run: `grep -rn "TODO\|FIXME\|XXX" sentinel tests || true`
Expected: no matches. Any TODO at this stage is a placeholder bug — replace with a real implementation or remove.

---

## STOP — notify user (commit boundary 2 — final)

Tasks 7–13 form the second commit: alembic skeleton, Docker, compose, pre-commit, CI workflows, README bootstrap section, and the verified end-to-end gate.

Files staged for commit (verify with `git status`):
- `alembic.ini`
- `migrations/env.py`, `migrations/script.py.mako`, `migrations/versions/.gitkeep`
- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`
- `.pre-commit-config.yaml`
- `.github/workflows/ci.yml`
- `.github/workflows/nightly-evals.yml`
- `README.md` (modified)

Suggested commit message:
```
chore: add docker stack, alembic skeleton, CI, pre-commit

docker-compose boots pgvector/postgres:16, redis:7, kafka 3.7 (KRaft mode),
the FastAPI app, and a worker stub, all with healthchecks. Dockerfile builds
the app as a -slim image with the package installed editable. Alembic env is
async-capable and pulls the DSN from Settings; the metadata target is left
open for Work Area B. pre-commit runs ruff + mypy on staged files. CI runs
lint + typecheck + unit + integration on every PR; nightly workflow is a
placeholder for the eval corpus until Work Area K lands.
```

Notify the user, list the files, paste the suggested commit message. **Do not run `git commit`.** Wait for confirmation.

After the user confirms commits + push:
1. Mark Work Area A complete in the program-of-work tracker.
2. Suggest Work Areas B, C, and J(skeleton) as the next parallel-eligible plans.

---

## Spec coverage map (self-review)

Mapping back to Area A "Deliverables" from the program of work:

| Deliverable | Task |
|---|---|
| `pyproject.toml` with full deps + tool configs | 2 |
| `Makefile` with full target set | 3 |
| `docker-compose.yml` (pg+pgvector, redis, kafka, app, worker; healthchecks) | 9 |
| `.env.example` | 4 |
| `.github/workflows/ci.yml` (lint, typecheck, unit, integration, evals-smoke; matrix py3.12) | 11 |
| `.github/workflows/nightly-evals.yml` | 11 |
| `.pre-commit-config.yaml` (ruff + mypy on staged) | 10 |
| `ruff.toml`/`mypy.ini` (in pyproject) | 2 |
| `alembic.ini` + `migrations/env.py` skeleton | 7 |

Acceptance criteria from Area A:
- `make bootstrap && make lint && make typecheck && make test` green on clean checkout → Task 13.
- `docker compose up` boots stack + `/healthz` returns 200 → Tasks 9, 13.
- CI green on a no-op PR → Task 11 (verified by the user after push).

Risks called out (Area A spec):
- pgvector container — pinned to `pgvector/pgvector:pg16` in Task 9.
- Kafka slow start — KRaft mode (no zookeeper), healthcheck waits for broker via `kafka-topics.sh --list`, in Task 9.
- mypy strict aggressiveness on aiokafka — overrides for aiokafka/testcontainers/locust in `pyproject.toml`, Task 2.
