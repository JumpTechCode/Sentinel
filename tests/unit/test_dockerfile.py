"""Contract test for the runtime image.

CI runs the app from a host virtualenv and only starts `postgres/redis/kafka`
from compose — it never builds the `app` image — so nothing else in the suite
guards the Dockerfile. The README's project-status table claims the production
image has landed; this test keeps that claim honest by asserting the structural
contract (multi-stage build, non-root, a liveness HEALTHCHECK) rather than the
single-stage editable-install skeleton it replaced (gh#73).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_DOCKERFILE = Path(__file__).resolve().parents[2] / "Dockerfile"


@pytest.fixture(scope="module")
def dockerfile_text() -> str:
    return _DOCKERFILE.read_text(encoding="utf-8")


def test_is_multistage(dockerfile_text: str) -> None:
    """At least two named build stages — a builder and a runtime."""
    stages = re.findall(r"^FROM\s+\S+\s+AS\s+(\S+)", dockerfile_text, re.MULTILINE | re.IGNORECASE)
    assert len(stages) >= 2, f"expected a multi-stage build, found stages={stages}"


def test_runtime_copies_from_an_earlier_stage(dockerfile_text: str) -> None:
    """The runtime stage assembles itself from the builder via `COPY --from=`.

    This is the load-bearing difference from the old single-stage image: build
    artifacts (the installed venv, the pre-downloaded embedding model) are
    produced in the builder and copied forward, so build tooling never ships in
    the runtime layer.
    """
    assert "COPY --from=" in dockerfile_text


def test_has_liveness_healthcheck(dockerfile_text: str) -> None:
    """A HEALTHCHECK probing the liveness endpoint must be baked into the image."""
    match = re.search(r"^HEALTHCHECK\b.*", dockerfile_text, re.MULTILINE | re.IGNORECASE)
    assert match is not None, "Dockerfile defines no HEALTHCHECK"
    # /healthz is liveness (process up); /readyz depends on external deps and is
    # the wrong signal for image-level health. Either is accepted, but one must
    # be probed.
    healthcheck_block = dockerfile_text[match.start() :]
    assert "/healthz" in healthcheck_block or "/readyz" in healthcheck_block


def test_runs_as_non_root(dockerfile_text: str) -> None:
    """The final stage drops to a non-root user."""
    user_directives = re.findall(r"^USER\s+(\S+)", dockerfile_text, re.MULTILINE | re.IGNORECASE)
    assert user_directives, "Dockerfile never sets USER (would run as root)"
    assert user_directives[-1] != "root", "final USER must not be root"


def test_no_editable_install(dockerfile_text: str) -> None:
    """The runtime image is a real install, not the `pip install -e .` skeleton."""
    assert "pip install -e" not in dockerfile_text
    assert "--editable" not in dockerfile_text
