"""The clean dbt project builds; each seeded scenario breaks it as expected."""

import importlib.util
import json
import os
import shutil
from pathlib import Path

import pytest
from dbt.cli.main import dbtRunner

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT = REPO_ROOT / "dbt_project"
SCENARIOS_DIR = REPO_ROOT / "scenarios"
SCENARIOS = ["upstream_rename", "schema_drift", "bad_join_fanout", "null_spike"]


def _copy_project(tmp_path: Path) -> Path:
    dest = tmp_path / "dbt_project"
    shutil.copytree(
        DBT_PROJECT,
        dest,
        ignore=shutil.ignore_patterns("target", "logs", "*.duckdb*"),
    )
    return dest


def _dbt_build(project_dir: Path, duckdb_path: Path):
    previous = os.environ.get("DBT_DUCKDB_PATH")
    os.environ["DBT_DUCKDB_PATH"] = str(duckdb_path)
    try:
        return dbtRunner().invoke(
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


def _load_break_module(scenario_name: str):
    spec = importlib.util.spec_from_file_location(
        f"break_{scenario_name}", SCENARIOS_DIR / scenario_name / "break.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _failing_node_names(build_result) -> set[str]:
    if build_result.result is None:
        return set()
    return {
        r.node.name
        for r in build_result.result.results
        if r.status in ("fail", "error")
    }


def test_clean_project_builds(tmp_path):
    project_dir = _copy_project(tmp_path)
    build_result = _dbt_build(project_dir, tmp_path / "dev.duckdb")
    assert build_result.success, _failing_node_names(build_result)


@pytest.mark.parametrize("scenario_name", SCENARIOS)
def test_scenario_breaks_as_expected(tmp_path, scenario_name):
    expected = json.loads(
        (SCENARIOS_DIR / scenario_name / "expected.json").read_text()
    )

    project_dir = _copy_project(tmp_path)
    _load_break_module(scenario_name).apply(project_dir)

    build_result = _dbt_build(project_dir, tmp_path / "dev.duckdb")

    assert not build_result.success
    failing_names = _failing_node_names(build_result)
    assert any(expected["failing_model"] in name for name in failing_names), (
        failing_names
    )
