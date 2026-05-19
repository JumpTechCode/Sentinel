"""Pure DSN-selection helper for alembic env.py.

Lives outside `migrations/env.py` so it can be unit-tested without importing
that module (which executes alembic-context code at import time and so cannot
be loaded outside an actual alembic invocation).
"""

from __future__ import annotations

from collections.abc import Callable


def choose_dsn(alembic_url: str | None, settings_loader: Callable[[], str]) -> str:
    """Resolve the Postgres DSN used by alembic.

    Precedence (highest first):
    1. Explicit alembic config URL (non-empty) — how testcontainers integration
       tests inject an ephemeral DSN via ``cfg.set_main_option("sqlalchemy.url", ...)``.
    2. ``settings_loader()`` — the production path used by ``make migrate``
       (reads from ``SENTINEL_POSTGRES_DSN`` env / YAML defaults).

    Earlier this preferred settings unconditionally, which silently overrode
    test-injected URLs. The new order keeps the CLI behavior identical (no
    explicit URL → settings) while letting tests pin a different DSN.
    """
    if alembic_url:
        return alembic_url
    try:
        return settings_loader()
    except Exception as err:
        raise RuntimeError(
            "alembic could not resolve a Postgres DSN — set SENTINEL_POSTGRES_DSN "
            "or [alembic] sqlalchemy.url"
        ) from err
