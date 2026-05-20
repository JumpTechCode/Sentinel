# Sentinel eval corpus

Ten curated postmortem cases that drive the eval harness scoring (see
`plans/2026-05-20-eval-harness-design.md` §2 + §4). Each `*.yaml` file
validates against `sentinel/evals/schema.py::CorpusCase` via
`sentinel/evals/corpus_loader.py`; a malformed file breaks `make evals` at
import time.

## What's here

| File | Date | Category | Acceptable |
|---|---|---|---|
| `cloudflare-2022-06-21-bgp.yaml` | 2022-06-21 | config | config, deploy |
| `cloudflare-2019-07-02-regex.yaml` | 2019-07-02 | deploy | deploy, config |
| `github-2018-10-21-network.yaml` | 2018-10-21 | dependency | dependency, data |
| `aws-2021-12-07-useast1.yaml` | 2021-12-07 | capacity | capacity, dependency |
| `stripe-2019-07-10-db-failover.yaml` | 2019-07-10 | dependency | dependency, capacity |
| `gitlab-2017-01-31-db-deletion.yaml` | 2017-01-31 | data | data |
| `roblox-2021-10-28-consul.yaml` | 2021-10-28 | capacity | capacity, dependency |
| `slack-2021-01-04-dns.yaml` | 2021-01-04 | external | external, dependency |
| `atlassian-2022-04-04-deletion.yaml` | 2022-04-04 | data | data, deploy |
| `fastly-2021-06-08-config.yaml` | 2021-06-08 | config | config, deploy |

Distribution: **deploy×1, config×2, dependency×2, capacity×2, data×2, external×1**.

## Smoke subset

The CLI `--smoke` flag selects the first 5 cases by sorted `id`:

```
atlassian-2022-04-04-deletion
aws-2021-12-07-useast1
cloudflare-2019-07-02-regex
cloudflare-2022-06-21-bgp
fastly-2021-06-08-config
```

This is the deterministic 5-case smoke subset CI runs on every PR (per
design §7). To swap the smoke set, rename files so a different 5 are first
alphabetically.

## Adding a new case

1. Pick a public postmortem with enough technical detail to populate
   `context_seed.recent_logs` with real or paraphrased log lines.
2. Copy an existing YAML and edit. Strict fields:
   - `id` matches the filename stem (kebab-case, dated)
   - `corpus_version: 1`
   - `source_url` is the primary writeup
   - `sources_consulted` lists every URL you actually fetched
   - `notes` is your curator rationale — what makes this case good for evals
   - Every `context_seed` item carries an ID matching the `[kind]:[value]`
     convention (`deploy:<sha>`, `log:<n>`, `similar:<slug>`, `runbook:<slug>`,
     `related:<slug>`, `active:<slug>`). The diagnosis prompt + scorer's
     evidence-quality metric depend on this format.
   - `ground_truth.category` is the primary label; `acceptable_categories`
     includes the primary plus 0–2 adjacent labels for cases where two
     categories are defensible.
3. Validate: `python -c "from pathlib import Path; from sentinel.evals.corpus_loader import load_case; load_case(Path('sentinel/evals/corpus/<id>.yaml'))"`
4. Record cassettes for the new case: `make evals-record` (or run
   `python -m sentinel.evals record --corpus sentinel/evals/corpus`).
   Commit the new cassette files under `sentinel/evals/cassettes/`.
5. Re-run `make evals-baseline` to update the baseline numbers in the README.

## Sourcing discipline

Every case must cite a real public postmortem. No imagined incidents,
no internal-only sources. The point of this corpus is reproducibility —
a reviewer should be able to read the source and verify the YAML.

Paraphrased log lines are OK (full postmortems rarely include raw log
dumps); fabricated log lines are not. When in doubt, lean conservative:
omit the log line entirely rather than invent.
