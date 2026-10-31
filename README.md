# Pipeline Medic

An agent that diagnoses failing **dbt** jobs orchestrated by **Airflow**, writes a fix, verifies it with `dbt build` in a sandbox, and then proposes a pull request.
It uses Meta's **Muse Spark** (Meta Model API), with **Muse Spark Open** as the open-weights fallback.

Built for the **Meta Global AI Developer Hackathon** (Oct 27 – Nov 4, 2026), Indie track.

**Status:** in progress. This commit contains only scaffolding docs.

- Design mockups: `docs/mockups/`
- Build sessions: `docs/sessions/`
- Seeded failure scenarios: upstream rename, schema drift, bad join fan-out, null spike

License: MIT
