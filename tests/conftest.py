"""Top-level pytest conftest.

Disables pydantic-settings auto-loading of `.env` / `.env.local` during all
test runs. Without this, a populated `.env` (e.g. an operator's eval-harness
config with real webhook secrets, ANTHROPIC_API_KEY, etc.) bleeds into tests
that assert default values — for example
``tests/unit/test_config.py::test_webhook_secrets_default_to_none`` failed
when an operator had populated `.env` for `make evals`.

The fix is environmental: we monkeypatch Settings.model_config to use a
non-existent file as the env_file, which makes pydantic-settings skip the
auto-load entirely. Tests that want to exercise specific settings continue
to use ``monkeypatch.setenv`` (env vars are still honored).
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _disable_settings_env_file_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """Prevent pydantic-settings from auto-loading .env / .env.local."""
    from sentinel.config.settings import Settings

    # Use a definitely-nonexistent path so SettingsConfigDict's env_file
    # lookup silently misses (rather than reading whatever the operator
    # happens to have at the project root).
    monkeypatch.setitem(Settings.model_config, "env_file", "/nonexistent/.env-test-only")
