# tests/load/locustfile.py
"""Locust load scenario — sustained webhook ingestion against a running stack.

Drives `POST /webhooks/generic` with unique, HMAC-signed payloads. Run against a
compose stack that has the diagnosis + memory consumers DISABLED so the load
never reaches the Anthropic API (see docker-compose.load.yml / `make load`).

    make load            # 100 req/s for 5 min, headless, summary only

Each request is unique (monotonic counter), so it creates a fresh incident
rather than deduping. Non-2xx responses are marked as locust failures, which
surfaces dropped events in the run summary.
"""

from __future__ import annotations

import itertools
import json
import os

from locust import HttpUser, constant_throughput, task
from sentinel.integrations.base import compute_hmac_sha256

_SECRET = os.environ.get("SENTINEL_GENERIC_WEBHOOK_SECRET", "loadtest-secret").encode()
_counter = itertools.count()


class WebhookUser(HttpUser):
    # constant_throughput(1) => each simulated user issues 1 request/sec, so
    # `-u 100` yields ~100 req/s regardless of server latency.
    wait_time = constant_throughput(1)

    @task
    def post_webhook(self) -> None:
        n = next(_counter)
        body = json.dumps(
            {
                "id": f"load-{n}",
                "service": f"svc-{n}",
                "severity": "SEV2",
                "title": f"synthetic load alert {n}",
            }
        ).encode()
        headers = {
            "X-Sentinel-Signature": "sha256=" + compute_hmac_sha256(body, _SECRET),
            "Content-Type": "application/json",
        }
        with self.client.post(
            "/webhooks/generic",
            data=body,
            headers=headers,
            name="/webhooks/generic",
            catch_response=True,
        ) as resp:
            if resp.status_code not in (200, 202):
                resp.failure(f"unexpected status {resp.status_code}")
