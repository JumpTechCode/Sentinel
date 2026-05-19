# sentinel/diagnosis/__init__.py
"""Diagnosis agent (Work Area G).

Public surface:
- ``diagnose(incident, context, deps) -> PersistedDiagnosis``  (added in a later task)
- ``DiagnosisDeps``                                              (added in a later task)
- ``PersistedDiagnosis``
- Error types: ``DiagnosisInvalid``, ``LLMTimeout``, ``LLMTransport``, ``LLMNoToolCall``
"""

from __future__ import annotations

from sentinel.diagnosis.errors import (
    DiagnosisInvalid,
    LLMNoToolCall,
    LLMTimeout,
    LLMTransport,
)
from sentinel.diagnosis.persisted import PersistedDiagnosis

__all__ = [
    "DiagnosisInvalid",
    "LLMNoToolCall",
    "LLMTimeout",
    "LLMTransport",
    "PersistedDiagnosis",
]
