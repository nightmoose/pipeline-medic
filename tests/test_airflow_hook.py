"""airflow_hook.py runs the real nightly_orders DAG and finds the failed task."""

import json
from pathlib import Path

import pytest

from pipeline_medic.airflow_hook import find_failed_task, run_dag
from pipeline_medic.diagnose import build_sandbox

REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = REPO_ROOT / "scenarios"
SCENARIOS = ["upstream_rename", "schema_drift", "bad_join_fanout", "null_spike"]


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_run_dag_finds_expected_failed_task(airflow_db, tmp_path, scenario):
    expected = json.loads((SCENARIOS_DIR / scenario / "expected.json").read_text())
    project_dir = build_sandbox(scenario, tmp_path)
    duckdb_path = tmp_path / "dev.duckdb"

    dagrun, log_text = run_dag(project_dir, duckdb_path)
    failed = find_failed_task(dagrun, log_text)

    assert failed is not None
    assert failed.task_id == expected["failing_task"]
    assert failed.log


def test_run_dag_clean_project_has_no_failure(airflow_db, tmp_path):
    import shutil

    project_dir = tmp_path / "dbt_project"
    shutil.copytree(
        REPO_ROOT / "dbt_project",
        project_dir,
        ignore=shutil.ignore_patterns("target", "logs", "*.duckdb*"),
    )
    duckdb_path = tmp_path / "dev.duckdb"

    dagrun, log_text = run_dag(project_dir, duckdb_path)

    assert find_failed_task(dagrun, log_text) is None
    assert dagrun.state == "success"
