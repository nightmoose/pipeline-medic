"""cli.py wires diagnose + verify + pr together and prints a rich report."""

from pathlib import Path

from typer.testing import CliRunner

from pipeline_medic.cli import app

REPO_ROOT = Path(__file__).resolve().parents[1]
runner = CliRunner()


def test_run_mock_scenario_prints_report_sections(airflow_db):
    result = runner.invoke(app, ["run", "--scenario", "upstream_rename", "--mock"])

    assert result.exit_code == 0, result.output
    for section in ["FAILED", "DIAGNOSIS", "PROPOSED FIX", "SANDBOX VERIFY", "PULL REQUEST"]:
        assert section in result.output
    assert "cust_id" in result.output
    assert (REPO_ROOT / "out" / "summary.html").exists()
    assert (REPO_ROOT / "out" / "pr" / "upstream_rename.md").exists()


def test_run_requires_exactly_one_of_scenario_or_all(airflow_db):
    result = runner.invoke(app, ["run", "--scenario", "upstream_rename", "--all", "--mock"])

    assert result.exit_code != 0
