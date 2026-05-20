"""Unit tests for sentinel.evals.cassette (VCR-style Anthropic HTTP layer)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest


def test_cassette_key_is_stable_across_runs() -> None:
    """Same context → same key (so the runner can find recorded responses)."""
    from sentinel.evals.cassette import compute_cassette_key

    k1 = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    k2 = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    assert k1 == k2
    assert len(k1) == 16  # 16-char prefix per the design


def test_cassette_key_changes_on_any_input_change() -> None:
    from sentinel.evals.cassette import compute_cassette_key

    base = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    different_prompt = compute_cassette_key(
        prompt_version="v2",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    different_model = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-opus-4-1",
        case_id="cloudflare-bgp",
        shot_index=0,
    )
    different_case = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="other-case",
        shot_index=0,
    )
    different_shot = compute_cassette_key(
        prompt_version="v1",
        model_id="claude-sonnet-4-5-20250929",
        case_id="cloudflare-bgp",
        shot_index=1,
    )
    assert len({base, different_prompt, different_model, different_case, different_shot}) == 5


@pytest.mark.asyncio
async def test_replay_returns_recorded_response(tmp_path: Path) -> None:
    """A pre-written cassette file is replayed verbatim."""
    from sentinel.evals.cassette import (
        CassetteContext,
        CassetteTransport,
    )

    key = "deadbeefcafe1234"
    cassette_path = tmp_path / f"{key}.json"
    cassette_path.write_text(
        json.dumps(
            {
                "key": key,
                "request": {"method": "POST", "url": "https://api.anthropic.com/v1/messages"},
                "response": {
                    "status_code": 200,
                    "headers": [["content-type", "application/json"]],
                    "body_b64": "eyJpZCI6Im1zZ18xMjMifQ==",  # b64("{\"id\":\"msg_123\"}")
                },
            }
        )
    )

    transport = CassetteTransport(mode="replay", cassette_dir=tmp_path)
    transport.set_context(
        CassetteContext(
            prompt_version="v1",
            model_id="claude-sonnet-4-5-20250929",
            case_id="x",
            shot_index=0,
        )
    )
    # Override key to the one we wrote (so the test doesn't depend on hash output)
    transport.override_next_key(key)

    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={})
    response = await transport.handle_async_request(request)
    assert response.status_code == 200
    body = await response.aread()
    assert body == b'{"id":"msg_123"}'


@pytest.mark.asyncio
async def test_replay_miss_raises_loudly(tmp_path: Path) -> None:
    """No matching cassette → CassetteMiss with the missing key in the message
    so the operator knows to re-record."""
    from sentinel.evals.cassette import (
        CassetteContext,
        CassetteMiss,
        CassetteTransport,
    )

    transport = CassetteTransport(mode="replay", cassette_dir=tmp_path)
    transport.set_context(
        CassetteContext(
            prompt_version="v1",
            model_id="claude-sonnet-4-5-20250929",
            case_id="x",
            shot_index=0,
        )
    )
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={})
    with pytest.raises(CassetteMiss) as exc:
        await transport.handle_async_request(request)
    assert "re-record" in str(exc.value).lower() or "regenerate" in str(exc.value).lower()


def test_record_mode_requires_set_context_before_handle(tmp_path: Path) -> None:
    """Without a CassetteContext the transport can't compute a key — programming
    bug. Raise loudly on the first request, not silently overwrite."""
    from sentinel.evals.cassette import CassetteTransport

    transport = CassetteTransport(mode="record", cassette_dir=tmp_path)
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={})
    with pytest.raises(RuntimeError, match="set_context"):
        # We don't actually call the network in this test — the no-context check
        # fires first. (handle_async_request is async, but the validation is sync.)
        import asyncio

        asyncio.run(transport.handle_async_request(request))


@pytest.mark.asyncio
async def test_record_mode_writes_cassette_then_returns_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify record-mode flow without hitting the real network: monkeypatch the
    inner transport's send to return a canned response."""
    from sentinel.evals.cassette import (
        CassetteContext,
        CassetteTransport,
    )

    canned_response = httpx.Response(
        status_code=200,
        headers={"content-type": "application/json"},
        content=b'{"id":"recorded_msg"}',
    )

    class _FakeInner(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return canned_response

    transport = CassetteTransport(
        mode="record", cassette_dir=tmp_path, inner_transport=_FakeInner()
    )
    transport.set_context(
        CassetteContext(
            prompt_version="v1",
            model_id="claude-sonnet-4-5-20250929",
            case_id="x",
            shot_index=0,
        )
    )

    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages", json={"q": 1})
    response = await transport.handle_async_request(request)
    assert response.status_code == 200

    # Cassette file written under tmp_path with the computed key
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload["response"]["status_code"] == 200
