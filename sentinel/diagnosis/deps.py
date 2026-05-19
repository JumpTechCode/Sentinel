# sentinel/diagnosis/deps.py
"""DI bundle for the diagnosis agent."""

from __future__ import annotations

from dataclasses import dataclass

from sentinel.diagnosis.llm_client import AnthropicClient
from sentinel.diagnosis.prompt import PromptBundle
from sentinel.observability.llm_audit import LLMAuditLogger
from sentinel.persistence.repositories import DiagnosisRepository, IncidentRepository


@dataclass(frozen=True, slots=True)
class DiagnosisDeps:
    llm: AnthropicClient
    prompt: PromptBundle
    max_input_tokens: int
    audit_logger: LLMAuditLogger


@dataclass(frozen=True, slots=True)
class ConsumerDeps:
    """Wider deps bundle the consumer needs.

    The pure agent (``diagnose``) consumes only ``DiagnosisDeps``. The consumer
    needs additional repository handles for I/O; keeping the agent's surface
    smaller helps with testing the orchestration in isolation.
    """

    llm: AnthropicClient
    prompt: PromptBundle
    max_input_tokens: int
    incident_repo: IncidentRepository
    diagnosis_repo: DiagnosisRepository
    audit_logger: LLMAuditLogger

    def agent_deps(self) -> DiagnosisDeps:
        return DiagnosisDeps(
            llm=self.llm,
            prompt=self.prompt,
            max_input_tokens=self.max_input_tokens,
            audit_logger=self.audit_logger,
        )
