"""Application settings — loaded from environment via pydantic-settings.

Per-env YAML files in `config/{env}.yaml` provide defaults; env vars override.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import Field, SecretStr, model_validator
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

Env = Literal["dev", "test", "prod"]


class YamlConfigSettingsSource(PydanticBaseSettingsSource):
    """Loads settings from a per-env YAML file.

    Priority is *lower* than env vars — place this source after env_settings
    in ``settings_customise_sources`` so that environment variables win.
    """

    def __init__(self, settings_cls: type[BaseSettings], env_name: str) -> None:
        super().__init__(settings_cls)
        # Resolve the config directory. SENTINEL_CONFIG_DIR lets a deployment point
        # at an explicit location; it is required once the package is installed
        # non-editable (e.g. the Docker image), where `__file__` lives under
        # site-packages and the `parents[2]`-relative path would miss the COPY'd
        # `config/` tree. Defaults to the in-tree `config/` for local/dev/test.
        config_dir = os.environ.get("SENTINEL_CONFIG_DIR")
        base = Path(config_dir) if config_dir else Path(__file__).resolve().parents[2] / "config"
        path = base / f"{env_name}.yaml"
        if path.exists():
            data = yaml.safe_load(path.read_text()) or {}
            if not isinstance(data, dict):
                raise ValueError(f"{path} must be a YAML mapping at the top level")
            self._data: dict[str, object] = data
        else:
            self._data = {}

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        value = self._data.get(field_name)
        return value, field_name, value is not None

    def __call__(self) -> dict[str, object]:
        return dict(self._data)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SENTINEL_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    env: Env = "dev"

    # Persistence
    postgres_dsn: str = Field(
        ..., description="async SQLAlchemy DSN, e.g. postgresql+asyncpg://..."
    )
    redis_url: str = Field(..., description="redis://host:port/db")
    kafka_brokers: str = Field(..., description="comma-separated host:port list")
    kafka_topic_incidents: str = "sentinel.incidents"
    kafka_consumer_group_enricher: str = "sentinel-enricher"

    # LLM
    anthropic_api_key: SecretStr = Field(..., description="Anthropic API key")
    anthropic_model: str = "claude-sonnet-4-5"

    # Diagnosis agent (Work Area G)
    diagnosis_consumer_enabled: bool = True
    diagnosis_prompt_version: str = "v1"
    diagnosis_max_input_tokens: int = 12_000
    diagnosis_max_output_tokens: int = 2048
    diagnosis_llm_timeout_seconds: float = 30.0
    kafka_consumer_group_diagnoser: str = "sentinel-diagnoser"

    # Memory & embeddings (Work Area H)
    embedding_model_name: str = "BAAI/bge-large-en-v1.5"
    embedding_model_cache_dir: str = "/var/cache/fastembed"
    embedding_compute_timeout_seconds: float = 5.0
    kafka_consumer_group_memory: str = "sentinel-memory"
    memory_consumer_enabled: bool = True

    # Observability
    log_level: str = "INFO"
    otel_endpoint: str | None = None
    llm_audit_log_path: str = "logs/llm-audit.log"

    # HTTP server
    http_host: str = "127.0.0.1"
    http_port: int = 8000

    # Webhook secrets — per source, optional. Adapter rejects 401 when unset.
    sentry_webhook_secret: SecretStr | None = None
    pagerduty_webhook_secret: SecretStr | None = None
    datadog_webhook_secret: SecretStr | None = None
    generic_webhook_secret: SecretStr | None = None

    # Eval harness (Work Area K) — off by default; runner CLI flips eval_mode.
    eval_mode: bool = False
    eval_corpus_dir: Path | None = None  # required when eval_mode=True
    eval_cassette_dir: Path | None = None  # if set, AnthropicClient uses cassette transport
    # Cassette transport mode. "replay" reads existing cassettes from disk; "record"
    # hits the live Anthropic API and writes captured exchanges into eval_cassette_dir.
    # Default "replay" preserves existing PR 3b behavior; the `record` CLI subcommand
    # sets SENTINEL_EVAL_CASSETTE_MODE=record in os.environ before constructing the
    # app so the lifespan picks it up at startup.
    eval_cassette_mode: Literal["record", "replay"] = "replay"

    @model_validator(mode="after")
    def _eval_mode_requires_corpus_dir(self) -> Settings:
        """Fail loud at startup when eval_mode is on but no corpus dir is set."""
        if self.eval_mode and self.eval_corpus_dir is None:
            raise ValueError(
                "eval_mode=True requires eval_corpus_dir to be set (SENTINEL_EVAL_CORPUS_DIR)"
            )
        return self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        env_name: str = os.environ.get("SENTINEL_ENV", "dev")
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            YamlConfigSettingsSource(settings_cls, env_name),
            file_secret_settings,
        )


def load_settings() -> Settings:
    """Load Settings, layering YAML defaults under env vars."""
    return Settings()
