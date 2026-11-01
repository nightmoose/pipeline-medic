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


def test_save_artifacts_writes_report_diagnosis_and_verify_log(airflow_db, tmp_path):
    artifacts_dir = tmp_path / "artifacts"

    result = runner.invoke(
        app,
        ["run", "--scenario", "upstream_rename", "--mock", "--save-artifacts", str(artifacts_dir)],
    )

    assert result.exit_code == 0, result.output
    scenario_dir = artifacts_dir / "upstream_rename"
    report_text = (scenario_dir / "report.txt").read_text()
    assert "DIAGNOSIS" in report_text
    assert "cust_id" in report_text

    diagnosis_json = (scenario_dir / "diagnosis.json").read_text()
    assert "cust_id" in diagnosis_json

    verify_log = (scenario_dir / "verify.log").read_text()
    assert verify_log  # non-empty

    assert (artifacts_dir / "summary.html").exists()


def test_save_artifacts_scrubs_api_key_and_bearer_tokens(monkeypatch, airflow_db, tmp_path):
    monkeypatch.setenv("MODEL_API_KEY", "super-secret-key")
    from pipeline_medic import cli as cli_module

    artifacts_dir = tmp_path / "artifacts"
    scenario_dir = artifacts_dir / "upstream_rename"
    scenario_dir.mkdir(parents=True)

    cli_module._write_artifact(
        scenario_dir / "report.txt",
        "token=super-secret-key\nAuthorization: Bearer abc.def-123\n",
    )

    scrubbed = (scenario_dir / "report.txt").read_text()
    assert "super-secret-key" not in scrubbed
    assert "Bearer abc.def-123" not in scrubbed
    assert "[REDACTED]" in scrubbed
