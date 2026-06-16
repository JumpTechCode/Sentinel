# tests/unit/diagnosis/test_llm_client.py
"""Unit tests for AnthropicClient wrapper (Task 12)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import anthropic
import httpx
import pytest
import sentinel.diagnosis.llm_client as llm_client_module
from sentinel.diagnosis.errors import LLMNoToolCall, LLMTimeout, LLMTransport
from sentinel.diagnosis.llm_client import AnthropicClient, LLMResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TOOL_NAME = "diagnose"
_TOOL_SCHEMA: dict[str, Any] = {
    "name": _TOOL_NAME,
    "description": "Diagnose the incident.",
    "input_schema": {"type": "object", "properties": {}},
}
_TOOL_INPUT: dict[str, Any] = {"root_cause": "OOM", "confidence": 0.8}


def _fake_api_status_error() -> anthropic.APIStatusError:
    """Construct a minimal APIStatusError for use as a side-effect."""
    resp = httpx.Response(
        500,
        text="internal server error",
        request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"),
    )
    return anthropic.APIStatusError("server error", response=resp, body=None)


def _make_fake_message(
    *,
    tool_name: str = _TOOL_NAME,
    tool_input: dict[str, Any] | None = None,
    include_tool_block: bool = True,
    tool_use_id: str = "toolu_abc123",
    input_tokens: int = 100,
    output_tokens: int = 200,
    stop_reason: str = "tool_use",
) -> MagicMock:
    """Build a fake ``Message`` returned by ``get_final_message()``."""
    msg = MagicMock()
    msg.stop_reason = stop_reason
    msg.usage.input_tokens = input_tokens
    msg.usage.output_tokens = output_tokens

    if include_tool_block:
        block = MagicMock()
        block.type = "tool_use"
        block.name = tool_name
        block.id = tool_use_id
        block.input = tool_input if tool_input is not None else _TOOL_INPUT
        msg.content = [block]
    else:
        # text-only block — no tool_use
        block = MagicMock()
        block.type = "text"
        block.name = None
        msg.content = [block]

    return msg


class _FakeStream:
    """Async context manager that returns a fake final message."""

    def __init__(self, final_message: Any, *, sleep_s: float = 0.0) -> None:
        self._final = final_message
        self._sleep_s = sleep_s

    async def __aenter__(self) -> _FakeStream:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    async def get_final_message(self) -> Any:
        if self._sleep_s > 0:
            await asyncio.sleep(self._sleep_s)
        return self._final


def _make_client(
    *,
    stream_result: Any = None,
    timeout_s: float = 10.0,
) -> AnthropicClient:
    """Build an ``AnthropicClient`` with a fake inner ``AsyncAnthropic``."""
    fake_inner = MagicMock()
    if stream_result is not None:
        fake_inner.messages.stream.return_value = stream_result
    return AnthropicClient(
        api_key=__import__("pydantic").SecretStr("sk-fake"),
        model="claude-sonnet-4-5",
        timeout_s=timeout_s,
        max_output_tokens=1024,
        client=fake_inner,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_happy_path_returns_llm_result() -> None:
    """A normal streaming response with a tool_use block returns LLMResult."""
    msg = _make_fake_message(input_tokens=50, output_tokens=150, stop_reason="tool_use")
    stream = _FakeStream(msg)
    client = _make_client(stream_result=stream)

    result = await client.diagnose_call(
        system="sys",
        user="user",
        tool_schema=_TOOL_SCHEMA,
        tool_name=_TOOL_NAME,
    )

    assert isinstance(result, LLMResult)
    assert result.tool_input == _TOOL_INPUT
    assert result.input_tokens == 50
    assert result.output_tokens == 150
    assert result.stop_reason == "tool_use"
    assert result.latency_ms >= 0


async def test_stream_call_sets_temperature_zero() -> None:
    """The diagnosis call must pin temperature=0 for run-to-run stability
    (issue #63: previously unset → API default 1.0)."""
    msg = _make_fake_message()
    stream = _FakeStream(msg)
    client = _make_client(stream_result=stream)

    await client.diagnose_call(
        system="sys",
        user="user",
        tool_schema=_TOOL_SCHEMA,
        tool_name=_TOOL_NAME,
    )

    _, kwargs = client._client.messages.stream.call_args  # type: ignore[attr-defined]
    assert kwargs["temperature"] == 0


async def test_timeout_raises_llm_timeout() -> None:
    """Stream that sleeps past timeout_s raises LLMTimeout."""
    msg = _make_fake_message()
    # sleep_s > timeout_s so asyncio.wait_for fires
    stream = _FakeStream(msg, sleep_s=0.5)
    client = _make_client(stream_result=stream, timeout_s=0.01)

    with pytest.raises(LLMTimeout):
        await client.diagnose_call(
            system="sys",
            user="user",
            tool_schema=_TOOL_SCHEMA,
            tool_name=_TOOL_NAME,
        )


async def test_transport_retry_succeeds_on_second_call() -> None:
    """First call raises APIStatusError; second succeeds; returns LLMResult."""
    msg = _make_fake_message()
    success_stream = _FakeStream(msg)
    error = _fake_api_status_error()

    fake_inner = MagicMock()
    # First call raises, second call returns a good stream
    fake_inner.messages.stream.side_effect = [error, success_stream]

    client = AnthropicClient(
        api_key=__import__("pydantic").SecretStr("sk-fake"),
        model="claude-sonnet-4-5",
        timeout_s=10.0,
        max_output_tokens=1024,
        client=fake_inner,
    )

    # Patch backoff to 0 so the test doesn't actually sleep
    with patch.object(llm_client_module, "_BACKOFF_S", 0.0):
        result = await client.diagnose_call(
            system="sys",
            user="user",
            tool_schema=_TOOL_SCHEMA,
            tool_name=_TOOL_NAME,
        )

    assert isinstance(result, LLMResult)
    assert fake_inner.messages.stream.call_count == 2


async def test_transport_exhaustion_raises_llm_transport() -> None:
    """Both calls raise APIStatusError → LLMTransport is raised."""
    error1 = _fake_api_status_error()
    error2 = _fake_api_status_error()

    fake_inner = MagicMock()
    fake_inner.messages.stream.side_effect = [error1, error2]

    client = AnthropicClient(
        api_key=__import__("pydantic").SecretStr("sk-fake"),
        model="claude-sonnet-4-5",
        timeout_s=10.0,
        max_output_tokens=1024,
        client=fake_inner,
    )

    with patch.object(llm_client_module, "_BACKOFF_S", 0.0):
        with pytest.raises(LLMTransport):
            await client.diagnose_call(
                system="sys",
                user="user",
                tool_schema=_TOOL_SCHEMA,
                tool_name=_TOOL_NAME,
            )

    assert fake_inner.messages.stream.call_count == 2


async def test_http_client_passthrough() -> None:
    """An injected httpx.AsyncClient is forwarded to AsyncAnthropic on construction."""
    from pydantic import SecretStr

    fake_http_client = httpx.AsyncClient()
    try:
        with patch.object(llm_client_module, "AsyncAnthropic") as mock_cls:
            AnthropicClient(
                api_key=SecretStr("sk-fake"),
                model="claude-sonnet-4-5",
                timeout_s=10.0,
                max_output_tokens=1024,
                http_client=fake_http_client,
            )
        mock_cls.assert_called_once_with(api_key="sk-fake", http_client=fake_http_client)
    finally:
        await fake_http_client.aclose()


async def test_llm_result_carries_tool_use_id() -> None:
    """The tool_use block id must be captured so a repair turn can reference it."""
    msg = _make_fake_message(tool_use_id="toolu_xyz")
    client = _make_client(stream_result=_FakeStream(msg))
    result = await client.diagnose_call(
        system="sys", user="user", tool_schema=_TOOL_SCHEMA, tool_name=_TOOL_NAME
    )
    assert result.tool_use_id == "toolu_xyz"


async def test_repair_builds_tool_result_turn() -> None:
    """On repair, the request replays the prior assistant tool_use turn plus a
    tool_result describing the error — a real multi-turn exchange, not prose
    appended to the user string (issue #63)."""
    from sentinel.diagnosis.llm_client import RepairTurn

    msg = _make_fake_message()
    client = _make_client(stream_result=_FakeStream(msg))
    repair = RepairTurn(
        tool_use_id="toolu_prev",
        tool_input={"hypothesis": ""},
        error="hypothesis must be non-empty",
    )

    await client.diagnose_call(
        system="sys",
        user="original-user",
        tool_schema=_TOOL_SCHEMA,
        tool_name=_TOOL_NAME,
        repair=repair,
    )

    _, kwargs = client._client.messages.stream.call_args  # type: ignore[attr-defined]
    messages = kwargs["messages"]
    assert [m["role"] for m in messages] == ["user", "assistant", "user"]
    # Original user prompt preserved as the first turn.
    assert messages[0]["content"] == "original-user"
    # Assistant turn replays the exact prior tool_use block (id + name + input).
    tool_use = messages[1]["content"][0]
    assert tool_use["type"] == "tool_use"
    assert tool_use["id"] == "toolu_prev"
    assert tool_use["name"] == _TOOL_NAME
    assert tool_use["input"] == {"hypothesis": ""}
    # Final user turn is a tool_result error referencing that tool_use id.
    tool_result = messages[2]["content"][0]
    assert tool_result["type"] == "tool_result"
    assert tool_result["tool_use_id"] == "toolu_prev"
    assert tool_result["is_error"] is True
    assert "hypothesis must be non-empty" in tool_result["content"]


async def test_no_tool_call_raises_llm_no_tool_call() -> None:
    """Message with no matching tool_use block raises LLMNoToolCall."""
    msg = _make_fake_message(include_tool_block=False)
    stream = _FakeStream(msg)
    client = _make_client(stream_result=stream)

    with pytest.raises(LLMNoToolCall):
        await client.diagnose_call(
            system="sys",
            user="user",
            tool_schema=_TOOL_SCHEMA,
            tool_name=_TOOL_NAME,
        )


async def test_diagnose_call_emits_llm_span(monkeypatch: pytest.MonkeyPatch) -> None:
    """The LLM call is wrapped in a tracing span (gh#64 — span_for_llm was unused)."""
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(
        "sentinel.observability.tracing._tracer",
        lambda: provider.get_tracer("sentinel"),
    )

    client = _make_client(stream_result=_FakeStream(_make_fake_message()))
    await client.diagnose_call(
        system="sys",
        user="user",
        tool_schema=_TOOL_SCHEMA,
        tool_name=_TOOL_NAME,
    )

    names = {s.name for s in exporter.get_finished_spans()}
    assert "llm.claude-sonnet-4-5" in names
