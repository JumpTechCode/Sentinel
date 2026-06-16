"""The app's OpenAPI schema must generate cleanly and cover the new routes."""

from __future__ import annotations

import json

from sentinel.api.app import build_app


def test_openapi_generates_and_covers_new_paths() -> None:
    app = build_app()
    schema = app.openapi()
    # Publishable: the schema round-trips through JSON.
    json.dumps(schema)
    paths = schema["paths"]
    assert "/incidents" in paths
    assert "/incidents/{incident_id}" in paths
    # /metrics is a real FastAPI route here (not a Prometheus middleware mount),
    # so it appears in the OpenAPI paths.
    assert "/metrics" in paths
    assert "get" in paths["/incidents"]
    assert "post" in paths["/incidents"]
