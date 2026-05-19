"""Alembic async env.

Work Area B will register the metadata target (SQLAlchemy declarative Base).
Work Area A only proves the runner boots and connects.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sentinel.persistence.dsn_resolution import choose_dsn
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Filled in by Work Area B:
target_metadata = None


def _resolve_dsn() -> str:
    def _load_from_settings() -> str:
        from sentinel.config.settings import load_settings

        return load_settings().postgres_dsn

    return choose_dsn(config.get_main_option("sqlalchemy.url"), _load_from_settings)


def run_migrations_offline() -> None:
    url = _resolve_dsn()
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
    dsn = _resolve_dsn()
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = dsn
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
