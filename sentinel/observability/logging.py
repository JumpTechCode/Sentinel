# sentinel/observability/logging.py
"""structlog configuration + LLM audit log handler.

`configure_logging(settings)` is safe to call more than once — repeated calls
reapply the same config. Context vars are managed via `structlog.contextvars`
so they propagate across async tasks correctly.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import structlog

from sentinel.config.settings import Settings

_LLM_AUDIT_LOGGER_NAME = "sentinel.llm_audit"


def configure_logging(settings: Settings) -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(message)s", force=True)
    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
    ]
    if settings.env == "prod":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=False))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _configure_llm_audit(settings.llm_audit_log_path)


def _configure_llm_audit(path_str: str) -> None:
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(_LLM_AUDIT_LOGGER_NAME)
    logger.handlers.clear()
    handler = RotatingFileHandler(path, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def get_llm_audit_logger() -> Any:
    """Returns a structlog bound logger writing to the LLM audit log file.

    Typed as Any because structlog.wrap_logger's return type is Any in the
    published stubs — callers use it via the standard structlog log-method API.
    """
    return structlog.wrap_logger(
        logging.getLogger(_LLM_AUDIT_LOGGER_NAME),
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
    )


def bind_request_context(
    *, incident_id: str | None = None, correlation_id: str, source: str | None = None
) -> None:
    fields: dict[str, str] = {"correlation_id": correlation_id}
    if incident_id is not None:
        fields["incident_id"] = incident_id
    if source is not None:
        fields["source"] = source
    structlog.contextvars.bind_contextvars(**fields)


def clear_request_context() -> None:
    structlog.contextvars.clear_contextvars()
