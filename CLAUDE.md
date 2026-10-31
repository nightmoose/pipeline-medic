# CLAUDE.md: Pipeline Medic
Hackathon project for the Meta Global AI Developer Hackathon (Oct 27 to Nov 4, 2026). All code is new, and nothing from before Oct 27 may be copied in.
Goal: an agent that diagnoses failing **dbt** jobs orchestrated by **Airflow**, writes a fix, verifies it with `dbt build` in a sandbox, then proposes a PR. Model: Muse Spark via the Meta Model API only.

## Hard rules
- The repo licence is MIT. Allowed dependency licences: MIT, ISC, BSD-2/3, 0BSD, Apache-2.0. Ask before adding anything GPL, LGPL, AGPL, MPL or unknown. Run `pip-licenses` after adding a dependency.
- Stack: Python 3.11, uv, dbt-core plus dbt-duckdb, apache-airflow (pinned, SQLite metadata, run via `airflow dags test`), sqlglot, openai SDK, typer, rich, jinja2, pydantic, pytest.
- LLM calls go only through `pipeline_medic/llm.py`, using the openai SDK against the Meta Model API: `base_url=https://api.meta.ai/v1`, `api_key=os.environ["MODEL_API_KEY"]` (pass it explicitly), model `muse-spark-1.3` (override with `MODEL`). No other providers and no fallback model.
  - Muse Spark always reasons, so never send `reasoning_effort: "none"`. Use `response_format` for structured JSON.
  - Never loop paid calls. Cache responses in `.cache/llm/`, keyed by prompt hash. Offline tests use recorded or mock response fixtures (`tests/fixtures/llm/`) and must not need a key.
- The PR step defaults to `--dry-run`. Never push or open a PR unless asked.
- Never commit secrets. `.env` is gitignored.
- Keep it small: under 2,000 lines of our own code.

## Layout
```
dbt_project/            real dbt project (profiles target: duckdb file per sandbox)
  models/staging, models/marts, tests (schema.yml), seeds/
airflow/dags/nightly_orders.py   real Airflow DAG: ingest -> dbt build per layer -> checks
scenarios/<name>/       breakage script + expected.json (diagnosis + contract)
pipeline_medic/{airflow_hook,collector,llm,verify,pr,report,cli}.py
templates/summary.html.j2   tests/
```

## Scenarios (each breaks the clean project in a reproducible way)
1. upstream_rename: a source column is renamed `customer_id` to `cust_id`, so the dbt model fails to compile or run.
2. schema_drift: `amount` changes from INT to VARCHAR, so a dbt contract or test fails.
3. bad_join_fanout: a join on a non-unique key causes a row explosion, so a `unique` test or row-count check fails.
4. null_spike: the upstream feed sends null emails, so a `not_null` test fails. Suggest only: this is a data problem, not a code problem.

## Done
`pipeline-medic run --all` triggers the Airflow DAG and catches the failed task. Across the four scenarios it shows 4 diagnoses, 3 patches verified by `dbt build --select <model>+` in a sandbox copy, 1 suggestion, and writes the HTML summary.
