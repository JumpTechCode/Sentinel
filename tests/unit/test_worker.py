"""Smoke tests for the diagnosis worker stub."""

from __future__ import annotations

import asyncio
from unittest.mock import patch


def test_worker_run_stops_on_event() -> None:
    """Worker coroutine terminates cleanly when the shutdown event fires."""
    from sentinel.workers.diagnosis_worker import _main

    async def _run_with_immediate_shutdown() -> None:
        # Patch asyncio.Event so stop.wait() returns immediately.
        class _ImmediateEvent:
            def set(self) -> None:
                pass

            async def wait(self) -> None:
                return

        with patch("sentinel.workers.diagnosis_worker.asyncio.Event", _ImmediateEvent):
            await _main()

    asyncio.run(_run_with_immediate_shutdown())


def test_worker_run_entrypoint() -> None:
    """The module-level ``run`` entrypoint is importable and is a callable."""
    from sentinel.workers import diagnosis_worker

    assert callable(diagnosis_worker.run)
