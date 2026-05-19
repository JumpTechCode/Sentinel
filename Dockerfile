# syntax=docker/dockerfile:1.7
# NOTE: single-stage editable install is intentional for the skeleton phase.
# Work Area M replaces this with a non-editable multi-stage build.

FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
# curl is needed for the compose healthcheck; build-essential is not needed
# because cp312 wheels cover all compiled deps.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

# Run as a non-root user for security.
RUN useradd --create-home --uid 1000 sentinel

WORKDIR /app
COPY pyproject.toml ./
COPY sentinel ./sentinel
COPY config ./config
COPY migrations ./migrations
COPY alembic.ini ./

RUN pip install --upgrade pip wheel \
 && pip install -e .

USER sentinel

# Single entrypoint: the API process owns HTTP plus the in-process Kafka consumers.
EXPOSE 8000
CMD ["sentinel-api"]
