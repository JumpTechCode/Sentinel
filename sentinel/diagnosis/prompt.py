# sentinel/diagnosis/prompt.py
"""Prompt versioning: load + hash-pin the system prompt.

Also provides `serialize(incident, ctx)` which renders an `IncidentDetailResponse`
and its `IncidentContext` into a structured plaintext block for LLM consumption.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sentinel.schemas.api import IncidentDetailResponse
    from sentinel.schemas.context import IncidentContext

_LOG = logging.getLogger("sentinel.diagnosis.prompt")

_PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"


@dataclass(frozen=True, slots=True)
class PromptBundle:
    version: str
    system_text: str
    sha256_hex: str

    @classmethod
    def load(cls, version: str) -> PromptBundle:
        md = _PROMPTS_DIR / f"{version}.md"
        baseline_file = _PROMPTS_DIR / f"{version}.sha256"
        text = md.read_text(encoding="utf-8")
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if baseline_file.exists():
            baseline = baseline_file.read_text(encoding="utf-8").strip()
            if baseline != digest:
                _LOG.warning(
                    "prompt_sha_mismatch",
                    extra={
                        "version": version,
                        "expected": baseline,
                        "actual": digest,
                    },
                )
        else:
            _LOG.warning("prompt_sha_baseline_missing", extra={"version": version})
        return cls(version=version, system_text=text, sha256_hex=digest)


def serialize(incident: IncidentDetailResponse, ctx: IncidentContext) -> str:
    parts: list[str] = []
    parts.append("INCIDENT")
    parts.append(f"  service:  {incident.service}")
    parts.append(f"  severity: {incident.severity}")
    parts.append(f'  title:    "{incident.title}"')
    parts.append(f"  opened:   {incident.opened_at.isoformat()}")
    parts.append(f"  fingerprint: {incident.fingerprint}")
    parts.append("")

    _section_deploys(parts, ctx)
    _section_similar(parts, ctx)
    _section_runbooks(parts, ctx)
    _section_related(parts, ctx)
    _section_active(parts, ctx)
    _section_logs(parts, ctx)
    return "\n".join(parts)


def _hdr(title: str, status: str, *, suffix: str = "") -> str:
    extra = f" — {suffix}" if suffix else ""
    return f"{title} — status={status}{extra}"


def _section_deploys(out: list[str], ctx: IncidentContext) -> None:
    fr = ctx.recent_deploys
    out.append(_hdr("DEPLOYS (recent)", fr.status))
    for d in fr.data:
        line = f"  [{d.id}] {d.service} @ {d.deployed_at.isoformat()}"
        if d.deployed_by:
            line += f" by {d.deployed_by}"
        out.append(line)
        if d.pr_number is not None and d.pr_title:
            out.append(f'    PR #{d.pr_number} "{d.pr_title}"')
        if d.pr_diff_summary:
            out.append(f"    diff: {d.pr_diff_summary}")
    out.append("")


def _section_similar(out: list[str], ctx: IncidentContext) -> None:
    fr = ctx.similar_incidents
    out.append(_hdr("SIMILAR INCIDENTS (top by cosine)", fr.status))
    for s in fr.data:
        out.append(f"  [{s.id}] cosine={s.cosine_similarity:.2f}")
        out.append(f'    title: "{s.title}"')
        out.append(f'    root_cause: "{s.root_cause}"')
        out.append(f'    remediation: "{s.remediation}"')
    out.append("")


def _section_runbooks(out: list[str], ctx: IncidentContext) -> None:
    fr = ctx.runbooks
    out.append(_hdr("RUNBOOKS", fr.status))
    for r in fr.data:
        out.append(f"  [{r.id}] cosine={r.cosine_similarity:.2f} — {r.title}")
        out.append(f"    {r.content}")
    out.append("")


def _section_related(out: list[str], ctx: IncidentContext) -> None:
    fr = ctx.related_alerts
    out.append(_hdr("RELATED ALERTS (recent)", fr.status))
    out.extend(
        f'  [{a.id}] {a.service} {a.severity} "{a.title}" opened={a.opened_at.isoformat()}'
        for a in fr.data
    )
    out.append("")


def _section_active(out: list[str], ctx: IncidentContext) -> None:
    fr = ctx.active_alerts
    out.append(_hdr("ACTIVE ALERTS (currently open)", fr.status))
    out.extend(
        f'  [{a.id}] {a.service} {a.severity} "{a.title}" opened={a.opened_at.isoformat()}'
        for a in fr.data
    )
    out.append("")


def _section_logs(out: list[str], ctx: IncidentContext) -> None:
    fr = ctx.recent_logs
    out.append(_hdr("RECENT LOGS", fr.status))
    out.extend(
        f"  [{log_line.id}] {log_line.timestamp.isoformat()} {log_line.level} {log_line.service} :: {log_line.message}"
        for log_line in fr.data
    )
    out.append("")
