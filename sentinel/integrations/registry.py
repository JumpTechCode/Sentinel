"""Webhook adapter registry + per-source secret resolver."""

from __future__ import annotations

from typing import Final

from pydantic import SecretStr

from sentinel.config.settings import Settings
from sentinel.integrations.base import MissingSecretError, WebhookAdapter
from sentinel.integrations.datadog import DatadogAdapter
from sentinel.integrations.generic import GenericAdapter
from sentinel.integrations.pagerduty import PagerDutyAdapter
from sentinel.integrations.sentry import SentryAdapter

ADAPTERS: Final[dict[str, WebhookAdapter]] = {
    "sentry": SentryAdapter(),
    "pagerduty": PagerDutyAdapter(),
    "datadog": DatadogAdapter(),
    "generic": GenericAdapter(),
}


def get_adapter(source: str) -> WebhookAdapter:
    return ADAPTERS[source]


def get_secret_for_source(settings: Settings, source: str) -> SecretStr:
    field = f"{source}_webhook_secret"
    value: SecretStr | None = getattr(settings, field, None)
    if value is None:
        raise MissingSecretError(f"webhook secret for source={source!r} is not configured")
    return value
