# tests/unit/api/test_instrumentation.py
"""The FastAPI app must be OpenTelemetry-instrumented at build time.

gh#64: `opentelemetry-instrumentation-fastapi` was a declared dependency but
`FastAPIInstrumentor` was never imported, so no request spans were ever emitted.
`instrument_app` sets `app._is_instrumented_by_opentelemetry = True`, which is
the observable contract this test pins.
"""

from __future__ import annotations

from sentinel.api.app import build_app


def test_build_app_instruments_fastapi() -> None:
    app = build_app()
    assert getattr(app, "_is_instrumented_by_opentelemetry", False) is True
