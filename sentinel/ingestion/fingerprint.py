"""Title normalization + fingerprint hashing.

normalize_title strips per-incident variation (UUIDs, timestamps, hex IDs,
high-cardinality integers, URLs, bracketed context) so that successive
occurrences of the same underlying error fingerprint identically.

fingerprint is sha256(service || normalized_title || severity) with a unit
separator delimiter, so values containing colons or pipes cannot create
collisions across fields.
"""

from __future__ import annotations

import hashlib
import re

# Full RFC-4122 UUID.
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)

# ISO-8601 timestamps: 2026-05-18T14:23:01Z / 2026-05-18 14:23:01+00:00 etc.
# re.IGNORECASE covers the lowercased 't'/'z' produced by the earlier s.lower().
_TIMESTAMP_RE = re.compile(
    r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b",
    re.IGNORECASE,
)

# Kubernetes-style pod / replicaset suffixes attached to a name token:
#   web-7d4f8b69-x9k2j  →  web-
# Uses lookbehind so the leading '-' is preserved; requires at least two
# dash-delimited alphanumeric segments so plain "service-1234" is not consumed
# (the trailing integer is handled separately by _INT_RE).
_K8S_SUFFIX_RE = re.compile(
    r"(?<=-)[0-9a-z]{4,12}(?:-[0-9a-z]{4,12})+\b",
    re.IGNORECASE,
)

# Pure hex run of 6+ chars (git SHAs, trace IDs, request IDs, etc.).
_HEX_ID_RE = re.compile(r"\b[0-9a-f]{6,}\b", re.IGNORECASE)

# Integers with 3+ digits — error codes (500, 502), port numbers, counts, etc.
_INT_RE = re.compile(r"\b\d{3,}\b")

# Full URLs (stripped before hex/int passes to avoid matching fragments).
_URL_RE = re.compile(r"https?://\S+")

# Bracketed context tags: [ctx:foo], [deploy:abc123], etc.
_BRACKETED_RE = re.compile(r"\[[^\]]*\]")

# Orphaned closing delimiters left when inner content is stripped,
# e.g. "(request_id=abcdef…)" → "(request_id=)" after hex strip.
# Matches a lone ')' or ']' immediately following '=', '(', ',', or ';'.
_ORPHANED_CLOSE_RE = re.compile(r"(?<=[=(,;])\s*[)\]]")

_WS_RE = re.compile(r"\s+")


def normalize_title(s: str) -> str:
    """Strip all per-incident tokens so repeated occurrences hash identically."""
    s = s.lower()
    # URLs first — they can contain timestamps and hex inside the path.
    s = _URL_RE.sub("", s)
    # Bracketed tags before UUID/hex so bracketed hex doesn't bleed through.
    s = _BRACKETED_RE.sub("", s)
    # Timestamps before UUID/hex: date segments look like hex once split.
    s = _TIMESTAMP_RE.sub("", s)
    # UUIDs before generic hex so the dashes are consumed together.
    s = _UUID_RE.sub("", s)
    # Kubernetes pod/RS suffixes before generic hex (they contain non-hex chars
    # like 'x', 'k', 'j' so they survive the hex pass without this step).
    s = _K8S_SUFFIX_RE.sub("", s)
    # Hex IDs (git SHAs, trace/request IDs, etc.).
    s = _HEX_ID_RE.sub("", s)
    # Numeric tokens ≥ 3 digits (HTTP codes, port numbers, large counts).
    s = _INT_RE.sub("", s)
    # Clean up orphaned closing delimiters left by inner-content removal.
    s = _ORPHANED_CLOSE_RE.sub("", s)
    return _WS_RE.sub(" ", s).strip()


_SEP = "\x1f"


def fingerprint(service: str, normalized_title: str, severity: str) -> str:
    """Return sha256(service ∥ normalized_title ∥ severity) as a 64-char hex digest.

    Fields are delimited by ASCII unit-separator (0x1F) so values that contain
    colons or pipes cannot produce cross-field collisions.
    """
    payload = f"{service}{_SEP}{normalized_title}{_SEP}{severity}".encode()
    return hashlib.sha256(payload).hexdigest()
