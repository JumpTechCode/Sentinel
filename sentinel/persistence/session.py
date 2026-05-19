# sentinel/persistence/session.py
"""Async SQLAlchemy engine + session factory.

Construction is pure (no connection attempt); callers control lifecycle. The
API process owns a single engine for HTTP handlers and the in-process Kafka
consumers; tests and utilities can build their own via `make_async_engine`.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sentinel.config.settings import Settings


def make_async_engine(settings: Settings) -> AsyncEngine:
    dsn = settings.postgres_dsn
    if "+asyncpg" not in dsn:
        raise ValueError(
            f"postgres_dsn must use the asyncpg driver " f"(postgresql+asyncpg://...), got: {dsn}"
        )
    return create_async_engine(
        dsn,
        pool_size=10,
        max_overflow=5,
        pool_pre_ping=True,
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
    )
