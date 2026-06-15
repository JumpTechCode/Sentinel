# scripts/export_openapi.py
"""Export the FastAPI OpenAPI schema to openapi.json (publishable artifact)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from sentinel.api.app import build_app


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("openapi.json")
    schema = build_app().openapi()
    out.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out} ({len(schema['paths'])} paths)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
