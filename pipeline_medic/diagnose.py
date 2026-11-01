"""Run a scenario end to end: break it, run the Airflow DAG, collect the
failure, and diagnose it.

    python -m pipeline_medic.diagnose --scenario upstream_rename --mock
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

import typer

from pipeline_medic import collector, llm
from pipeline_medic.airflow_hook import FailedTask, find_failed_task, run_dag
from pipeline_medic.collector import FailureContext

REPO_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT = REPO_ROOT / "dbt_project"
SCENARIOS_DIR = REPO_ROOT / "scenarios"
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures" / "llm"

app = typer.Typer(add_completion=False)


def _load_break_module(scenario: str):
    spec = importlib.util.spec_from_file_location(
        f"break_{scenario}", SCENARIOS_DIR / scenario / "break.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_sandbox(scenario: str, sandbox_dir: Path) -> Path:
    """Copy the clean dbt project into `sandbox_dir` and apply the scenario breakage."""
    project_dir = sandbox_dir / "dbt_project"
    shutil.copytree(
        DBT_PROJECT, project_dir, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb*")
    )
    _load_break_module(scenario).apply(project_dir)
    return project_dir


@dataclass
class DiagnoseResult:
    scenario: str
    failed_task: FailedTask
    context: FailureContext
    diagnosis: llm.Diagnosis
    prompt: str = ""


def diagnose_scenario_full(scenario: str, *, mock: bool = True) -> DiagnoseResult:
    """Run `scenario` against a sandbox copy of the dbt project via the real
    Airflow DAG, collect the failure context, and return the failed task, the
    failure context, and a Diagnosis."""
    with tempfile.TemporaryDirectory() as tmp:
        sandbox_dir = Path(tmp)
        project_dir = build_sandbox(scenario, sandbox_dir)
        duckdb_path = sandbox_dir / "dev.duckdb"

        dagrun, log_text = run_dag(project_dir, duckdb_path)
        failed_task = find_failed_task(dagrun, log_text)
        if failed_task is None:
            raise RuntimeError(f"scenario {scenario!r} did not fail any Airflow task")

        context = collector.collect(project_dir, duckdb_path)
        prompt = f"Airflow task: {failed_task.task_id}\n\n{context.to_prompt()}"

        mock_response = None
        if mock:
            mock_response = json.loads((FIXTURES_DIR / f"{scenario}.json").read_text())
        diagnosis = llm.diagnose(prompt, mock_response=mock_response, scenario=scenario)
        return DiagnoseResult(
            scenario=scenario, failed_task=failed_task, context=context, diagnosis=diagnosis,
            prompt=prompt,
        )


def diagnose_scenario(scenario: str, *, mock: bool = True) -> llm.Diagnosis:
    """Run `scenario` and return just the Diagnosis. See `diagnose_scenario_full`
    for the failed task and failure context too."""
    return diagnose_scenario_full(scenario, mock=mock).diagnosis


@app.command()
def main(
    scenario: str = typer.Option(..., "--scenario", help="Scenario name under scenarios/"),
    mock: bool = typer.Option(
        False, "--mock", help="Use the recorded fixture instead of calling the Meta Model API"
    ),
) -> None:
    diagnosis = diagnose_scenario(scenario, mock=mock)
    typer.echo(diagnosis.model_dump_json(indent=2))


if __name__ == "__main__":
    app()
