.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV ?= .venv
# Prefer $(VENV)/bin/python (local dev) but fall back to plain python on PATH
# so CI (which pip install -e .[dev]'s into the host Python without a venv)
# can run the same targets without rewriting them.
PY := $(shell test -x $(VENV)/bin/python && echo $(VENV)/bin/python || echo python)
PIP := $(VENV)/bin/pip

.PHONY: help bootstrap fmt lint typecheck test test-unit test-integration \
        migrate migrate-down evals evals-smoke evals-record evals-baseline \
        evals-compare evals-reset \
        readme-numbers load load-smoke chaos compose-up compose-down openapi clean

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
	$(PY) -m alembic upgrade head

migrate-down:  ## alembic downgrade -1
	$(PY) -m alembic downgrade -1

# Eval targets source .env into the shell so vars not loaded by pydantic-settings
# (e.g. ANTHROPIC_API_KEY used by the cassette transport's record-mode guard
# directly via os.environ) are visible to the CLI.
_LOAD_ENV := set -a && [ -f .env ] && . ./.env; set +a

evals-reset:  ## Wipe Kafka topic + Redis + Postgres state between eval runs
	# Use `docker compose exec` (not `docker exec` against hard-coded container
	# names) so this works regardless of compose project naming (CI's
	# checkout dir doesn't always produce the same project name as local dev).
	# Kafka topic delete is tolerated-if-missing — first run has nothing to
	# delete — but Redis FLUSHDB and Postgres TRUNCATE failures are real and
	# must surface, so no leading `-` on those.
	-docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic sentinel.incidents 2>/dev/null
	docker compose exec -T redis redis-cli FLUSHDB
	docker compose exec -T postgres psql -U sentinel -d sentinel -c "TRUNCATE incidents, diagnoses, outbox_events RESTART IDENTITY CASCADE;"

evals:  ## Full eval corpus (cassette replay)
	$(_LOAD_ENV); $(PY) -m sentinel.evals run --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes

evals-smoke:  ## 5-case smoke set (cassette replay)
	$(_LOAD_ENV); $(PY) -m sentinel.evals run --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes --smoke

evals-compare:  ## Compare the latest run JSON against evals/baselines/main.json (CI gate)
	$(_LOAD_ENV); $(PY) -m sentinel.evals compare-to-baseline

evals-record:  ## (PR 3c) record cassettes from live API
	$(_LOAD_ENV); $(PY) -m sentinel.evals record --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes

evals-baseline:  ## Write evals/baselines/main.json from a fresh corpus replay
	$(_LOAD_ENV); $(PY) -m sentinel.evals baseline --corpus sentinel/evals/corpus --cassette-dir sentinel/evals/cassettes

readme-numbers:  ## Patch README between evals:start/end markers from latest run
	$(PY) -m sentinel.evals readme

load:  ## Locust load: 100 req/s x 5min against a consumers-off stack (creditless)
	docker compose -f docker-compose.yml -f docker-compose.load.yml up -d --wait
	$(VENV)/bin/locust -f tests/load/locustfile.py --host http://localhost:8000 \
		--headless -u 100 -r 50 -t 5m --only-summary
	@echo "Stack left up for inspection; 'make compose-down' to tear down."

load-smoke:  ## Fast in-process load invariants (zero-drop + p95); needs docker
	$(PY) -m pytest tests/load -v -m load --no-cov

chaos:  ## Resilience suite: breaker fault-injection (B2) + toxiproxy Redis outage (B1)
	$(PY) -m pytest tests/chaos -v -m chaos --no-cov

compose-up:  ## docker compose up -d
	docker compose up -d --wait

compose-down:  ## docker compose down -v
	docker compose down -v

openapi:  ## Export OpenAPI JSON (placeholder until Work Area I)
	@echo "openapi: not implemented yet (Work Area I)"; exit 0

clean:  ## Remove venv and caches
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
