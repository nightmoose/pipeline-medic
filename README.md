# Pipeline Medic

An agent that diagnoses failing **dbt** jobs orchestrated by **Airflow**, writes a fix, verifies it with `dbt build` in a sandbox, and then proposes a pull request.
It uses Meta's **Muse Spark** (Meta Model API).

Built for the **Meta Global AI Developer Hackathon** (Oct 27 – Nov 4, 2026), Indie track.

## What it does

Point it at a failing dbt job and it will:

1. **Collect** — pull the failing Airflow task, the dbt node, the error message, the model's SQL, and row/null stats from the dbt manifest and run results.
2. **Diagnose** — send that evidence to Muse Spark and get back a root cause, evidence, a confidence score, and either a unified diff patch (`action: "fix"`) or a note that this is an upstream data problem (`action: "suggest"`).
3. **Verify** — in a throwaway sandbox copy of the dbt project: apply the patch, run `dbt compile` + `dbt build`, check the patched model's row count and null rate against a clean baseline, and re-run the real Airflow DAG to confirm the original task now passes.
4. **Propose a PR** — commit the patch to a new branch (dry-run by default: nothing is pushed, `gh pr create` is never called) and write a PR body with the root cause, the diff, and the verification log.

Four seeded scenarios exercise this end to end: an upstream column rename, a schema drift that breaks a dbt contract, a bad join fan-out, and a null spike that's correctly left as a suggestion rather than a patch.

## Architecture

```
dbt_project/            the dbt project the scenarios break (duckdb-backed)
airflow/dags/            the real nightly_orders DAG (ingest -> dbt build per layer -> checks)
scenarios/<name>/        a breakage script + expected.json per scenario
pipeline_medic/
  collector.py            parses dbt manifest/run_results into failure evidence
  llm.py                  the only module that calls the Meta Model API
  diagnose.py             wires airflow_hook + collector + llm together
  verify.py               applies a patch in a sandbox and checks it fixes the build
  pr.py                   branch + commit + PR body (dry-run by default)
  report.py               renders the incident table to out/summary.html
  cli.py                  `pipeline-medic run` — ties it all together and prints the report
```

Each scenario runs against its own temporary sandbox copy of `dbt_project` with its own DuckDB file, so runs never touch the real project files or a shared database.

## How to run it

```sh
uv sync
uv run pipeline-medic run --scenario upstream_rename --mock
uv run pipeline-medic run --all --mock
```

`--mock` replaces the Meta Model API call with the recorded fixture in `tests/fixtures/llm/<scenario>.json`, so it runs offline with no API key. Drop `--mock` to call Muse Spark for real (see below). Either way, the Airflow DAG and dbt builds are real, not mocked.

Each run prints, per scenario: the failed Airflow task and dbt node, the diagnosis, the proposed fix (as a diff), the sandbox verification checks, and the dry-run pull request (branch name and a path to the generated PR body). It also writes a dark HTML incident summary to `out/summary.html`.

## Meta Model API usage

All LLM calls go through `pipeline_medic/llm.py` and nowhere else, using the `openai` SDK against:

- `base_url`: `https://api.meta.ai/v1`
- `model`: `muse-spark-1.3` (override with the `MODEL` env var)
- `api_key`: read from `MODEL_API_KEY`

Responses are cached by prompt hash under `.cache/llm/` so repeat runs don't re-spend a paid call. No other provider and no fallback model are used.

### Live run

Dropping `--mock` calls Muse Spark for real, guarded by a few safety nets in `pipeline_medic/llm.py`:

- the API client uses `max_retries=0` and a 120s timeout — no silent retries against a paid endpoint;
- a hard cap, `MAX_LLM_CALLS` (default 20), tracked in `.cache/llm/calls.json`, refuses further calls once reached (every real attempt counts, including the invalid-JSON retry);
- every real call appends a cost estimate to `.cache/llm/usage.jsonl` (tokens in/out, $1.25/M input + $4.25/M output), with no key, headers, or prompts logged.

```sh
export MODEL_API_KEY=...
uv run pipeline-medic run --all --save-artifacts out/live
```

`--save-artifacts DIR` writes each scenario's rich report, `diagnosis.json`, and verify log under `DIR/<scenario>/`, plus a copy of `summary.html`, with the API key and any `Bearer ...` token scrubbed from every file.

## Tests

```sh
uv run pytest
```

Tests use an ephemeral SQLite Airflow metadata DB and the recorded LLM fixtures — no network access or API key required.

License: MIT
