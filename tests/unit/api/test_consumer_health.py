"""Consumer-aliveness surface: /readyz and `_start_then_run` failure behavior.

After issue #36, Kafka consumers run as background tasks decoupled from the
FastAPI lifespan. The hazard introduced by that decoupling is silent failure:
a consumer can crash (start exception or run-loop exception) while the app
keeps serving `/healthz 200`. These tests pin the contract that closes the
hazard: `_start_then_run` surfaces start failures to the task (so a
done-callback can observe them), and `/readyz` reflects per-consumer aliveness.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sentinel.api.app import _on_consumer_task_done, _start_then_run
from sentinel.api.routes.health import router as health_router


def _app_with_state(consumer_alive: dict[str, bool] | None) -> FastAPI:
    app = FastAPI()
    if consumer_alive is not None:
        app.state.consumer_alive = consumer_alive
    app.include_router(health_router)
    return app


# ---- /readyz ------------------------------------------------------------


def test_readyz_returns_200_when_all_consumers_alive() -> None:
    app = _app_with_state({"memory": True, "enrichment": True, "diagnosis": True})
    client = TestClient(app)
    r = client.get("/readyz")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["checks"]["consumer:memory"] == "ok"
    assert body["checks"]["consumer:enrichment"] == "ok"
    assert body["checks"]["consumer:diagnosis"] == "ok"


def test_readyz_returns_503_when_any_consumer_dead() -> None:
    app = _app_with_state({"memory": False, "enrichment": True, "diagnosis": True})
    client = TestClient(app)
    r = client.get("/readyz")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "degraded"
    assert body["checks"]["consumer:memory"] == "dead"
    assert body["checks"]["consumer:enrichment"] == "ok"


def test_readyz_returns_503_when_consumer_state_missing() -> None:
    # Lifespan didn't run (test-only app, or shutdown raced ahead of probe).
    # A readiness gate must not route traffic to a pod whose state is unknown.
    app = _app_with_state(None)
    client = TestClient(app)
    r = client.get("/readyz")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unknown"


def test_readyz_returns_503_when_consumer_state_empty() -> None:
    # No consumers enabled (e.g., minimal integration-test config). Treated as
    # "no signal" rather than "everything OK" — readiness gate must be explicit.
    app = _app_with_state({})
    client = TestClient(app)
    r = client.get("/readyz")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unknown"


# ---- _start_then_run ----------------------------------------------------


@pytest.mark.asyncio
async def test_start_then_run_reraises_start_exception() -> None:
    """If `kafka.start()` raises, propagate so a done_callback can observe it.

    The previous behavior swallowed the exception and returned, which made the
    task complete normally — invisible to any monitoring code looking at
    `task.exception()`. Re-raise so the asyncio.Task carries the failure.
    """
    kafka = MagicMock()
    kafka.start = AsyncMock(side_effect=RuntimeError("broker unreachable"))
    run_mock = AsyncMock()

    with pytest.raises(RuntimeError, match="broker unreachable"):
        await _start_then_run(kafka, run_mock, "test-consumer")

    run_mock.assert_not_called()


@pytest.mark.asyncio
async def test_start_then_run_invokes_run_when_start_succeeds() -> None:
    kafka = MagicMock()
    kafka.start = AsyncMock()
    run_mock = AsyncMock()

    await _start_then_run(kafka, run_mock, "test-consumer")

    kafka.start.assert_awaited_once()
    run_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_then_run_propagates_run_exception() -> None:
    """A run-loop crash must surface to the task, not be silently dropped."""
    kafka = MagicMock()
    kafka.start = AsyncMock()
    run_mock = AsyncMock(side_effect=ValueError("consumer crashed mid-poll"))

    with pytest.raises(ValueError, match="consumer crashed mid-poll"):
        await _start_then_run(kafka, run_mock, "test-consumer")


@pytest.mark.asyncio
async def test_start_then_run_propagates_cancellation_during_start() -> None:
    """Shutdown cancels the task — CancelledError must propagate cleanly."""
    kafka = MagicMock()
    kafka.start = AsyncMock(side_effect=asyncio.CancelledError())
    run_mock = AsyncMock()

    with pytest.raises(asyncio.CancelledError):
        await _start_then_run(kafka, run_mock, "test-consumer")

    run_mock.assert_not_called()


# ---- done-callback ------------------------------------------------------


@pytest.mark.asyncio
async def test_done_callback_marks_consumer_dead_and_bumps_metric_on_exception() -> None:
    """End-to-end contract this whole fix exists to ensure: a crashed task →
    consumer_alive flipped, crash metric incremented, ERROR logged via exc_info.
    """
    from sentinel.observability import metrics as M

    app = FastAPI()
    app.state.consumer_alive = {"probe-crash": True}
    app.state.shutting_down = False

    label = f"probe-crash-{uuid4()}"
    callback = _on_consumer_task_done(label, app)
    # Keep the dict key aligned with the label used by the callback.
    app.state.consumer_alive = {label: True}

    before = M.consumer_crashed_total.labels(consumer=label)._value.get()

    async def boom() -> None:
        raise RuntimeError("simulated consumer crash")

    task = asyncio.create_task(boom())
    with pytest.raises(RuntimeError):
        await task
    callback(task)

    assert app.state.consumer_alive[label] is False
    after = M.consumer_crashed_total.labels(consumer=label)._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_done_callback_noops_on_cancellation() -> None:
    """Cancellation is the shutdown signal — must not be treated as a crash."""
    from sentinel.observability import metrics as M

    app = FastAPI()
    label = f"probe-cancel-{uuid4()}"
    app.state.consumer_alive = {label: True}
    app.state.shutting_down = False
    callback = _on_consumer_task_done(label, app)

    before = M.consumer_crashed_total.labels(consumer=label)._value.get()

    async def forever() -> None:
        await asyncio.sleep(60)

    task = asyncio.create_task(forever())
    await asyncio.sleep(0)  # let it start
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    callback(task)

    assert app.state.consumer_alive[label] is True
    after = M.consumer_crashed_total.labels(consumer=label)._value.get()
    assert after == before


@pytest.mark.asyncio
async def test_done_callback_noops_on_clean_return_during_shutdown() -> None:
    """`consumer.run()` returns cleanly when `stop()` is called from lifespan
    teardown. That MUST NOT be counted as a crash — otherwise every graceful
    shutdown (SIGTERM, k8s rotation, docker compose down) would spam the
    headline crash metric and ERROR-log three lines per restart, destroying
    the signal value of `sentinel_consumer_crashed_total`.
    """
    from sentinel.observability import metrics as M

    app = FastAPI()
    label = f"probe-clean-{uuid4()}"
    app.state.consumer_alive = {label: True}
    app.state.shutting_down = True  # lifespan finally-block has started
    callback = _on_consumer_task_done(label, app)

    before = M.consumer_crashed_total.labels(consumer=label)._value.get()

    async def clean_return() -> None:
        return  # consumer stopped, run() exited cleanly

    task = asyncio.create_task(clean_return())
    await task
    callback(task)

    assert app.state.consumer_alive[label] is True
    after = M.consumer_crashed_total.labels(consumer=label)._value.get()
    assert after == before


@pytest.mark.asyncio
async def test_done_callback_flags_clean_return_outside_shutdown_as_crash() -> None:
    """If `run()` returns cleanly while we are NOT shutting down, that's a bug
    in the consumer wrapper (loop should be infinite) — flag it.
    """
    from sentinel.observability import metrics as M

    app = FastAPI()
    label = f"probe-clean-running-{uuid4()}"
    app.state.consumer_alive = {label: True}
    app.state.shutting_down = False
    callback = _on_consumer_task_done(label, app)

    before = M.consumer_crashed_total.labels(consumer=label)._value.get()

    async def clean_return() -> None:
        return

    task = asyncio.create_task(clean_return())
    await task
    callback(task)

    assert app.state.consumer_alive[label] is False
    after = M.consumer_crashed_total.labels(consumer=label)._value.get()
    assert after == before + 1


# ---- crash metric -------------------------------------------------------


def test_consumer_crashed_total_metric_is_registered() -> None:
    from sentinel.observability import metrics as M

    # Counter exists with a {consumer} label so dashboards can alert per stream.
    # Use a unique label so this assertion doesn't leak state to other tests.
    label = f"test-probe-{uuid4()}"
    counter = M.consumer_crashed_total
    counter.labels(consumer=label).inc()
    metrics = list(counter.collect())
    samples: list[Any] = list(metrics[0].samples)
    matched = [
        s
        for s in samples
        if s.name == "sentinel_consumer_crashed_total" and s.labels.get("consumer") == label
    ]
    assert matched, f"expected sentinel_consumer_crashed_total{{consumer='{label}'}} sample"
    assert matched[0].value >= 1.0
