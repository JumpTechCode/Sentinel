"""VCR-style HTTP record/replay layer for the Anthropic client.

The eval runner (PR 3b) constructs an AsyncAnthropic wrapping a custom
httpx.AsyncClient whose transport is a CassetteTransport. Before each
diagnosis call the runner sets a CassetteContext on the transport;
the transport computes a stable key from (prompt_version, model_id,
case_id, shot_index) and either records the live HTTP exchange (record
mode) or replays a previously-recorded one (replay mode).

Design choices per spec §7:
- Cassettes live under sentinel/evals/cassettes/<prompt_version>/<model_id>/
  with filename = <16-char key>.json.
- Replay-mode miss raises CassetteMiss with a regenerate guidance message;
  no silent fallback to the network.
- Record mode requires ANTHROPIC_API_KEY env var (the inner transport
  forwards to api.anthropic.com).
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import httpx

CassetteMode = Literal["record", "replay"]


class CassetteMiss(Exception):
    """No recorded cassette matches the current key — replay cannot proceed.

    Carries the missing key and the cassette dir in the message so the
    operator knows exactly what to re-record.
    """


@dataclass(frozen=True, slots=True)
class CassetteContext:
    """Per-request context the transport uses to derive the cassette key."""

    prompt_version: str
    model_id: str
    case_id: str
    shot_index: int


def compute_cassette_key(
    *,
    prompt_version: str,
    model_id: str,
    case_id: str,
    shot_index: int,
) -> str:
    """sha256-derived 16-char hex key from the four context fields. Stable
    across runs and across machines; sensitive to any input change."""
    payload = json.dumps(
        {
            "prompt_version": prompt_version,
            "model_id": model_id,
            "case_id": case_id,
            "shot_index": shot_index,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


class CassetteTransport(httpx.AsyncBaseTransport):
    """httpx transport that records or replays HTTP exchanges by cassette key."""

    def __init__(
        self,
        *,
        mode: CassetteMode,
        cassette_dir: Path,
        inner_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._mode: CassetteMode = mode
        self._cassette_dir = cassette_dir
        self._cassette_dir.mkdir(parents=True, exist_ok=True)
        # The inner transport forwards record-mode requests to the network.
        # Default is httpx's standard AsyncHTTPTransport. Tests inject a fake
        # to avoid hitting the network.
        self._inner = inner_transport or httpx.AsyncHTTPTransport()
        self._context: CassetteContext | None = None
        self._override_key: str | None = None  # test-only escape hatch

    def set_context(self, ctx: CassetteContext) -> None:
        """Runner calls this before each diagnosis request."""
        self._context = ctx
        self._override_key = None

    def override_next_key(self, key: str) -> None:
        """Test-only — bypass key derivation for the next request.

        Not used by the runner; only by unit tests that need to match a
        hand-written cassette file.
        """
        self._override_key = key

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._context is None and self._override_key is None:
            raise RuntimeError(
                "CassetteTransport.set_context must be called before "
                "handle_async_request — the runner sets context per-request"
            )

        if self._override_key is not None:
            key = self._override_key
        elif self._context is not None:
            key = compute_cassette_key(
                prompt_version=self._context.prompt_version,
                model_id=self._context.model_id,
                case_id=self._context.case_id,
                shot_index=self._context.shot_index,
            )
        else:  # pragma: no cover — guarded by the no-context check above
            raise RuntimeError("unreachable: context/override invariant violated")
        path = self._cassette_dir / f"{key}.json"

        if self._mode == "replay":
            if not path.exists():
                raise CassetteMiss(
                    f"no cassette at {path} (key={key}) — re-record via "
                    "`make evals-record` and commit the new cassette"
                )
            return _response_from_cassette(path)

        # record mode: forward to inner, capture, write, return
        response = await self._inner.handle_async_request(request)
        body = await response.aread()
        _write_cassette(path, key=key, request=request, response=response, body=body)
        # Return a fresh response with the captured body (the original stream
        # has been drained by .aread()).
        return httpx.Response(
            status_code=response.status_code,
            headers=response.headers,
            content=body,
            request=request,
        )


# --- helpers ---


def _response_from_cassette(path: Path) -> httpx.Response:
    payload = json.loads(path.read_text())
    body = base64.b64decode(payload["response"]["body_b64"])
    return httpx.Response(
        status_code=payload["response"]["status_code"],
        headers=payload["response"]["headers"],
        content=body,
    )


def _write_cassette(
    path: Path,
    *,
    key: str,
    request: httpx.Request,
    response: httpx.Response,
    body: bytes,
) -> None:
    payload = {
        "key": key,
        "request": {
            "method": request.method,
            "url": str(request.url),
        },
        "response": {
            "status_code": response.status_code,
            "headers": [[k.decode(), v.decode()] for k, v in response.headers.raw],
            "body_b64": base64.b64encode(body).decode("ascii"),
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
