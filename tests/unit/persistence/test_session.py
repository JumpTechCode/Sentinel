# tests/unit/persistence/test_session.py
"""Session module exposes async engine + session factory bound to settings."""

import pytest
from sentinel.config.settings import Settings
from sentinel.persistence.session import make_async_engine, make_session_factory
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker


def _settings() -> Settings:
    return Settings(
        postgres_dsn="postgresql+asyncpg://u:p@localhost:5432/db",
        redis_url="redis://localhost:6379/0",
        kafka_brokers="localhost:9092",
        anthropic_api_key="x",
    )


def test_engine_construction_does_not_connect() -> None:
    eng = make_async_engine(_settings())
    assert isinstance(eng, AsyncEngine)
    assert "asyncpg" in str(eng.url)


def test_session_factory_is_async_sessionmaker() -> None:
    eng = make_async_engine(_settings())
    factory = make_session_factory(eng)
    assert isinstance(factory, async_sessionmaker)


def test_engine_rejects_sync_dsn() -> None:
    s = _settings()
    s = s.model_copy(update={"postgres_dsn": "postgresql://u:p@localhost/db"})
    with pytest.raises(ValueError, match="asyncpg"):
        make_async_engine(s)
