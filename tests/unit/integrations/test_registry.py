from __future__ import annotations

import pytest
from pydantic import SecretStr
from sentinel.config.settings import Settings
from sentinel.integrations.base import MissingSecretError
from sentinel.integrations.registry import (
    ADAPTERS,
    get_adapter,
    get_secret_for_source,
)
from sentinel.integrations.sentry import SentryAdapter


def test_adapters_dict_keys() -> None:
    assert set(ADAPTERS.keys()) == {"sentry", "pagerduty", "datadog", "generic"}


def test_get_adapter_returns_correct_class() -> None:
    assert isinstance(get_adapter("sentry"), SentryAdapter)


def test_get_adapter_unknown_raises_key_error() -> None:
    with pytest.raises(KeyError):
        get_adapter("nonesuch")


def test_get_secret_for_source_returns_field() -> None:
    s = Settings.model_construct(sentry_webhook_secret=SecretStr("xyz"))
    assert get_secret_for_source(s, "sentry").get_secret_value() == "xyz"


def test_get_secret_for_source_missing_raises() -> None:
    s = Settings.model_construct(sentry_webhook_secret=None)
    with pytest.raises(MissingSecretError):
        get_secret_for_source(s, "sentry")
