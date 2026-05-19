# sentinel/diagnosis/errors.py
"""Exception types for the diagnosis pipeline."""

from __future__ import annotations


class DiagnosisError(Exception):
    """Base class for diagnosis-layer failures."""


class DiagnosisInvalid(DiagnosisError):
    """LLM output failed schema validation twice in a row."""


class LLMError(DiagnosisError):
    """Base for LLM-transport errors raised by the client wrapper."""


class LLMTimeout(LLMError):
    """LLM call exceeded the configured timeout."""


class LLMTransport(LLMError):
    """LLM call failed with a transport error after the single retry."""


class LLMNoToolCall(LLMError):
    """LLM returned a message that did not include the required tool call."""
