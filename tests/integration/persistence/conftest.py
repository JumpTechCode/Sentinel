# tests/integration/persistence/conftest.py
"""Testcontainers-backed Postgres with pgvector."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg_container() -> Iterator[PostgresContainer]:
    image = os.environ.get("SENTINEL_TEST_PG_IMAGE", "pgvector/pgvector:pg16")
    with PostgresContainer(image, driver="asyncpg") as pg:
        yield pg


@pytest.fixture()
def pg_dsn(pg_container: PostgresContainer) -> str:
    # testcontainers returns a sync-flavored URL; rewrite to asyncpg.
    raw: str = pg_container.get_connection_url()
    return raw.replace("postgresql+psycopg2", "postgresql+asyncpg").replace(
        "postgresql://", "postgresql+asyncpg://"
    )
