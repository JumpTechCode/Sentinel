# 0007 — Evidence citation id format: kind-prefixed strings in context, lenient match in validator

**Status:** Accepted
**Date:** 2026-05-20

## Context

The diagnosis prompt teaches the LLM to cite supporting evidence using bracketed identifiers — `[deploy:abc123]`, `[similar:<uuid>]`, `[runbook:<uuid>]`, `[log:0]`, `[related:<uuid>]`. Each evidence reference in the structured response is then a `{kind, id, note}` triple, validated against the context by `verify_evidence` in `sentinel/diagnosis/validation.py`.

The non-obvious question is *what shape `id` takes on both sides of the verification gate*:

- **Context items** (DeployItem, LogLine, SimilarIncidentItem, etc.) carry `id` fields like `"deploy:abc123"` and `"log:0"` — i.e., the bracket form is embedded verbatim. The prompt renders these by interpolating the whole string between brackets: `f"[{d.id}] ..."` produces `[deploy:abc123]`.
- **LLM completions in practice** split on the colon. Real cassettes from `claude-sonnet-4-5` consistently emit `{"kind":"deploy","id":"abc123"}` — the bare token after the colon, not the prefixed bracket form. This is unsurprising: the model treats the bracket as a display element and reasonably hands back only the identifier portion in a structured tool input.

The validator originally bucketed context ids by kind (`bucket["deploy"] = {"deploy:abc123"}`) and required `ref.id in bucket[ref.kind]`. With the LLM emitting `"abc123"` and the bucket holding `"deploy:abc123"`, **every evidence citation registered as invented**, capping confidence at 0.4 and collapsing `evidence_quality` to zero across the entire corpus.

The eval harness surfaced this on 2026-05-20: the first full corpus run posted `evidence_quality=0.00` on all ten cases despite the LLM producing well-cited diagnoses.

## Decision

We adopt a **lenient (kind, id) match** in `verify_evidence`. A reference verifies if either of the following resolves under the declared kind:

- `ref.id in bucket[ref.kind]`  *(prefixed-id call site — what the bucket actually stores today)*
- `f"{ref.kind}:{ref.id}" in bucket[ref.kind]`  *(bare-id call site — what the LLM actually emits)*

Wrong-kind references continue to fall through to `invented` — the lenient match does **not** cross kind boundaries.

We keep the existing context `id` shape (`"deploy:abc123"`) and prompt format (`[deploy:abc123]`) unchanged. Cassettes recorded under the strict validator remain valid and now verify cleanly without re-recording.

## Why not the alternatives

**Strip the prefix from context items, change the prompt to `[deploy:abc123]` formatting from `(kind, id)` pairs separately, keep the strict validator.** This is theoretically the cleanest decomposition — kind lives in one field, identity in another, never mixed. But it requires editing the renderer in five places, regenerating every cassette ($0.45 + ~15 minutes of API time per corpus run), and rebaselining eval numbers in the README. The behavioral payoff is identical to the lenient match: every cassette ends up verifying. We chose the cheaper path.

**Train the LLM with a new prompt version that demands the prefixed form in the id field.** Could work, but it relies on instruction-following discipline against a strong learned pattern (split on `:` for tool-call ids). Even if successful, it leaves the validator brittle to the next model version and to other callers that follow the colon-splitting convention. The lenient match is contract-level, not prompt-level.

**Tighten the bucket to strip the prefix when indexing (`bucket["deploy"] = {"abc123"}`) and require bare ids.** Symmetric to the above but in the opposite direction. Same downside (cassettes must be re-recorded if any historical LLM emitted the prefixed form — and they will, intermittently). The lenient match supports both directions without re-recording.

## Consequences

- **Positive:** `evidence_quality` becomes a meaningful headline metric (recovers from 0.00 → expected ~0.7+ on corpus). No cassette re-record. No prompt churn. The validator gate keeps its load-bearing role of catching invented refs.
- **Negative:** A future reader of `verify_evidence` must understand why both branches exist. Mitigated by an explicit inline comment and this ADR. The lenient match introduces an edge case where `ref.id = "foo:bar"` (already prefixed by the caller) would match `bucket["foo"] = {"foo:foo:bar"}` if such a thing existed — but the kind enum is closed (`Literal["deploy","similar_incident","runbook","log","related_alert"]`) and context items never double-prefix, so this is theoretical only.
- **Regression coverage:** `tests/unit/diagnosis/test_validation.py` adds two new cases — bare-id-matches-prefixed-bucket (the LLM behavior we observed) and bare-id-does-NOT-match-wrong-kind-bucket (the kind boundary that must remain strict).

## Open

The lenient match exists to bridge two id shapes (bare vs prefixed). If the production prompt + context layer are ever refactored to agree on a single shape, the second branch in `verify_evidence` and `score_evidence_quality` can be removed and this ADR superseded. No commitment to do so — the cost of the extra branch is one comparison and one comment.
