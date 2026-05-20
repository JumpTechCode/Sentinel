.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV ?= .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: help bootstrap fmt lint typecheck test test-unit test-integration \
        migrate migrate-down evals evals-smoke evals-record evals-baseline \
        readme-numbers load chaos compose-up compose-down openapi clean

help:  ## Show available targets
	@awk 'BEGIN{FS=":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*##/ {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

$(VENV)/bin/activate:
	python3.12 -m venv $(VENV)
	$(PIP) install --upgrade pip wheel

bootstrap: $(VENV)/bin/activate  ## Create venv and install dev+runtime deps
	$(PIP) install -e ".[dev]"
	$(VENV)/bin/pre-commit install

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

test-integration:  ## pytest integration tests (requires docker; coverage gate is unit-only)
	$(VENV)/bin/pytest tests/integration -v -m integration --no-cov

migrate:  ## alembic upgrade head
	$(VENV)/bin/alembic upgrade head

migrate-down:  ## alembic downgrade -1
	$(VENV)/bin/alembic downgrade -1

evals:  ## Full eval corpus (cassette replay)
	$(PY) -m sentinel.evals run --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes

evals-smoke:  ## 5-case smoke set (cassette replay)
	$(PY) -m sentinel.evals run --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes --smoke

evals-record:  ## (PR 3c) record cassettes from live API
	$(PY) -m sentinel.evals record --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes

evals-baseline:  ## (PR 3c) tag a baseline corpus run
	$(PY) -m sentinel.evals baseline --corpus sentinel/evals/corpus --shots 5

readme-numbers:  ## Patch README between evals:start/end markers from latest run
	$(PY) -m sentinel.evals readme

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
