"""collector.py parses dbt's run_results.json/manifest.json for each scenario."""

import json
import os
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner

from pipeline_medic import collector
from pipeline_medic.diagnose import build_sandbox

REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = REPO_ROOT / "scenarios"
SCENARIOS = ["upstream_rename", "schema_drift", "bad_join_fanout", "null_spike"]


def _dbt_build(project_dir: Path, duckdb_path: Path) -> None:
    previous = os.environ.get("DBT_DUCKDB_PATH")
    os.environ["DBT_DUCKDB_PATH"] = str(duckdb_path)
    try:
        dbtRunner().invoke(
            [
                "--quiet",
                "build",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(project_dir),
            ]
        )
    finally:
        if previous is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = previous


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_collect_matches_expected(tmp_path, scenario):
    expected = json.loads((SCENARIOS_DIR / scenario / "expected.json").read_text())
    project_dir = build_sandbox(scenario, tmp_path)
    duckdb_path = tmp_path / "dev.duckdb"
    _dbt_build(project_dir, duckdb_path)

    context = collector.collect(project_dir, duckdb_path)

    assert context.failing_model == expected["failing_model"]
    assert context.check == expected["failing_check"]
    assert context.message
    assert context.to_prompt()


def test_collect_null_spike_stats(tmp_path):
    project_dir = build_sandbox("null_spike", tmp_path)
    duckdb_path = tmp_path / "dev.duckdb"
    _dbt_build(project_dir, duckdb_path)

    context = collector.collect(project_dir, duckdb_path)

    assert context.stats["null_rate"] > 0


def test_collect_bad_join_fanout_stats(tmp_path):
    project_dir = build_sandbox("bad_join_fanout", tmp_path)
    duckdb_path = tmp_path / "dev.duckdb"
    _dbt_build(project_dir, duckdb_path)

    context = collector.collect(project_dir, duckdb_path)

    assert context.stats["duplicate_rows"] > 0
