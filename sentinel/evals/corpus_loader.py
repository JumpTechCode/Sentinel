"""Load + validate corpus YAML files.

Fail-loud philosophy: a broken YAML breaks `make evals` immediately at
import-time, not three minutes into a run. Returns frozen Pydantic models;
duplicate case IDs across the directory raise CorpusValidationError so a
fat-finger curator copy/paste doesn't silently produce a "two cases, same
metrics" bug.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from sentinel.evals.schema import CorpusCase


class CorpusValidationError(Exception):
    """Raised when a corpus file or directory fails validation.

    Carries the source path in the message so operators can find the offending
    YAML without grepping the stack trace.
    """


def load_case(path: Path) -> CorpusCase:
    """Load one YAML file as a CorpusCase. Raises FileNotFoundError on absent
    file, CorpusValidationError on any other read/parse/validation failure
    (with the path embedded in the message).
    """
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        # Preserve the missing-file contract callers may depend on.
        raise
    except (OSError, UnicodeDecodeError) as e:
        # PermissionError, IsADirectoryError, decode failures, etc. — wrap so
        # operators get the same path-embedded format as parse/validation errors.
        raise CorpusValidationError(f"{path}: cannot read — {e}") from e

    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise CorpusValidationError(f"{path}: YAML parse error — {e}") from e

    if not isinstance(raw, dict):
        raise CorpusValidationError(
            f"{path}: top-level YAML must be a mapping, got {type(raw).__name__}"
        )

    try:
        return CorpusCase.model_validate(raw)
    except ValidationError as e:
        raise CorpusValidationError(f"{path}: schema validation failed — {e}") from e


def load_corpus_dir(dir_path: Path) -> list[CorpusCase]:
    """Load all *.yaml / *.yml files under a directory; returns cases sorted by case.id.

    Non-YAML files (README, .txt, hidden) are silently skipped. Both .yaml and
    .yml extensions are accepted (Python YAML convention historically uses both
    — silently skipping .yml would surprise a curator).
    Duplicate case IDs across files raise CorpusValidationError.
    """
    yaml_files = sorted({*dir_path.glob("*.yaml"), *dir_path.glob("*.yml")})
    cases = [load_case(path) for path in yaml_files]

    # Guard against duplicate IDs — would silently produce duplicate metric rows
    # in eval_case_results and corrupt the regression baseline.
    seen: dict[str, Path] = {}
    for path, case in zip(yaml_files, cases, strict=True):
        if case.id in seen:
            raise CorpusValidationError(
                f"duplicate case id {case.id!r}: {seen[case.id]} and {path}"
            )
        seen[case.id] = path

    return sorted(cases, key=lambda c: c.id)
