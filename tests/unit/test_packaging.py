"""Contract test for the wheel's non-Python data files.

setuptools packages only ``*.py`` by default. The diagnosis prompt bundle
(``sentinel/diagnosis/prompts/{version}.md`` + ``.sha256``) is read at runtime
via a package-relative path, so a non-editable install (the Docker image) must
ship those files — otherwise the app crashes at ``PromptBundle.load`` on boot.

CI runs the app from a source checkout and never builds the wheel, so nothing
else guards this. This test pins the ``package-data`` declaration to the files
it must cover. (A build-and-boot smoke of the actual image would be stronger but
needs a Docker build in CI; tracked separately.)
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import cast

_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROMPTS_DIR = _REPO_ROOT / "sentinel" / "diagnosis" / "prompts"


def _package_data() -> dict[str, list[str]]:
    cfg = tomllib.loads((_REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return cast("dict[str, list[str]]", cfg["tool"]["setuptools"]["package-data"])


def test_diagnosis_prompts_are_declared_package_data() -> None:
    patterns = _package_data().get("sentinel.diagnosis", [])
    # The versioned prompt markdown is the file whose absence crashes the image.
    assert any(
        p.startswith("prompts/") and p.endswith(".md") for p in patterns
    ), f"diagnosis prompts not declared as package-data: {patterns}"


def test_every_prompt_file_exists_to_be_packaged() -> None:
    """A declared glob with no files would silently ship nothing."""
    md_files = list(_PROMPTS_DIR.glob("*.md"))
    assert md_files, f"no prompt .md files under {_PROMPTS_DIR}"
    # The loader also reads a sibling .sha256 integrity baseline per version; if a
    # version ships a baseline it must be packaged too (declared via *.sha256).
    for md in md_files:
        baseline = md.with_suffix(".sha256")
        if baseline.exists():
            patterns = _package_data().get("sentinel.diagnosis", [])
            assert any(
                p.endswith(".sha256") for p in patterns
            ), f"{baseline.name} exists but *.sha256 is not declared package-data"
