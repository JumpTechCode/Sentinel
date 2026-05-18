"""FastAPI application factory and uvicorn entrypoint."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from sentinel import __version__
from sentinel.api.routes.health import router as health_router


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    # Work Area I will start Kafka producer / consumer here.
    yield


def build_app() -> FastAPI:
    app = FastAPI(
        title="Sentinel",
        version=__version__,
        description="AI on-call copilot — alert ingestion, context assembly, evidence-cited diagnosis.",
        lifespan=lifespan,
    )
    app.include_router(health_router)
    return app


def run() -> None:
    import uvicorn

    from sentinel.config.settings import load_settings

    settings = load_settings()
    uvicorn.run(
        "sentinel.api.app:build_app",
        factory=True,
        host=settings.http_host,
        port=settings.http_port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":  # pragma: no cover
    run()
