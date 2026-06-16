# syntax=docker/dockerfile:1.7
# Multi-stage build. The builder compiles the project + dependencies into an
# isolated virtualenv and pre-downloads the embedding model; the runtime stage
# copies only those artifacts, so build tooling never ships in the final image.
# Runs non-root with a liveness HEALTHCHECK.

# ---- Builder ---------------------------------------------------------------
FROM python:3.12-slim AS builder
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

# Build into an isolated venv so the runtime stage copies a single self-contained
# tree (/opt/venv) instead of mutating the system interpreter.
RUN python -m venv "$VIRTUAL_ENV" \
 && pip install --upgrade pip wheel

WORKDIR /build
# pyproject declares `readme = "README.md"`, so the wheel build reads it — it must
# be present in the build context for a non-editable install.
COPY pyproject.toml README.md ./
COPY sentinel ./sentinel
# Non-editable install: the package is baked into the venv's site-packages, so
# the runtime stage needs neither the source tree nor build backends.
RUN pip install .

# Pre-download the fastembed model so the first container start is offline-fast.
ENV SENTINEL_EMBEDDING_MODEL_CACHE_DIR=/opt/fastembed
RUN python -c "from fastembed import TextEmbedding; \
               TextEmbedding(model_name='BAAI/bge-large-en-v1.5', cache_dir='/opt/fastembed')"

# ---- Runtime ---------------------------------------------------------------
FROM python:3.12-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    SENTINEL_EMBEDDING_MODEL_CACHE_DIR=/opt/fastembed

# The package is installed non-editable, so the per-env YAML loader's
# `__file__`-relative default would resolve into site-packages. Point it at the
# config tree COPY'd to /app/config below.
ENV SENTINEL_CONFIG_DIR=/app/config

# curl is needed for the HEALTHCHECK below; nothing else from the toolchain.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Run as a non-root user for security.
RUN useradd --create-home --uid 1000 sentinel

# Copy the built venv and the pre-downloaded model cache from the builder.
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder --chown=sentinel:sentinel /opt/fastembed /opt/fastembed

WORKDIR /app
# Alembic config + migrations and the per-env config tree are not part of the
# wheel; carry them so `alembic upgrade` can run against the container and
# settings can resolve per-env defaults.
COPY alembic.ini ./
COPY migrations ./migrations
COPY config ./config

# Pre-create the LLM audit log directory so the non-root user can write to it.
# (Default Settings.llm_audit_log_path is `logs/llm-audit.log`, resolved
# relative to WORKDIR /app.)
RUN mkdir -p /app/logs && chown sentinel:sentinel /app/logs

USER sentinel

# Single entrypoint: the API process owns HTTP plus the in-process Kafka consumers.
EXPOSE 8000

# Liveness probe. /healthz returns 200 once the process is up; it deliberately
# does NOT depend on Postgres/Redis/Kafka (that is /readyz's job), so it is the
# correct signal for image-level health. start-period covers model load + boot.
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -fsS http://localhost:8000/healthz || exit 1

CMD ["sentinel-api"]
