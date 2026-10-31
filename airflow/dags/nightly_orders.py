"""Nightly DAG: ingest seeds, then dbt build staging and marts, per layer."""

import os
from pathlib import Path

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT_DIR = str(REPO_ROOT / "dbt_project")
PROFILES_DIR = PROJECT_DIR


def _run_dbt(args: list[str]) -> None:
    from dbt.cli.main import dbtRunner

    os.environ.setdefault(
        "DBT_DUCKDB_PATH", str(REPO_ROOT / "dbt_project" / "dev.duckdb")
    )
    result = dbtRunner().invoke(
        [*args, "--project-dir", PROJECT_DIR, "--profiles-dir", PROFILES_DIR]
    )
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed: {result.exception}")


def ingest(**_context) -> None:
    _run_dbt(["seed"])


def build_staging(**_context) -> None:
    _run_dbt(["build", "--select", "staging.*"])


def build_marts(**_context) -> None:
    _run_dbt(["build", "--select", "marts.*"])


with DAG(
    dag_id="nightly_orders",
    description="Ingest seeds and build the orders/events dbt layers nightly.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 27, tz="UTC"),
    catchup=False,
    default_args={"retries": 0},
    tags=["pipeline-medic", "dbt"],
) as dag:
    ingest_task = PythonOperator(task_id="ingest", python_callable=ingest)

    dbt_build_staging = PythonOperator(
        task_id="dbt_build_staging", python_callable=build_staging
    )

    dbt_build_marts = PythonOperator(
        task_id="dbt_build_marts", python_callable=build_marts
    )

    ingest_task >> dbt_build_staging >> dbt_build_marts
