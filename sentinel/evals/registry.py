"""Active corpus case registry — shared singleton for fetcher override and runner.

The runner sets the active case before firing each synthetic webhook; the corpus
fetchers read it during enrichment. Both `fetcher_override.corpus_fetchers()` and
the runner reference the module-level `REGISTRY` singleton.

Not thread-safe — the eval runner is single-threaded by design (asyncio, not
threads).
"""

from __future__ import annotations

from sentinel.evals.schema import CorpusCase


class ActiveCaseRegistry:
    """Per-process holder for the currently-active corpus case.

    The runner sets the active case before POSTing each synthetic webhook;
    the fetchers read it during the enrichment fan-out. NOT thread-safe —
    the eval runner is single-threaded by design (the orchestrator fans out
    via asyncio, not threads).
    """

    def __init__(self) -> None:
        self._case: CorpusCase | None = None

    def get(self) -> CorpusCase | None:
        return self._case

    def set(self, case: CorpusCase) -> None:
        self._case = case

    def clear(self) -> None:
        self._case = None


# Module-level singleton — both fetchers and runner reference this instance.
REGISTRY = ActiveCaseRegistry()
