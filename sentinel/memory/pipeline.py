# sentinel/memory/pipeline.py
"""Text composition for embedding inputs.

Pure functions over MemoryIncidentRow / ResolutionData. The opened path embeds
'service title' (symmetric with how SimilarIncidentsFetcher embeds its query);
the resolved path embeds 'title\\nroot_cause\\nremediation' (the richer signal
that makes similar-incident retrieval get better with use).
"""

from __future__ import annotations

from sentinel.persistence.repositories import MemoryIncidentRow, ResolutionData


class MemoryPipeline:
    def compose_initial(self, row: MemoryIncidentRow) -> str:
        return f"{row.service} {row.title}"

    def compose_resolved(self, row: MemoryIncidentRow, resolution: ResolutionData) -> str:
        return f"{row.title}\n{resolution.root_cause}\n{resolution.remediation}"
