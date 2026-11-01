"""`pipeline-medic run` — diagnose, verify, and propose a fix for failing dbt jobs.

    pipeline-medic run --scenario upstream_rename --mock
    pipeline-medic run --all --mock
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

from pipeline_medic import llm, diagnose, pr, verify
from pipeline_medic.report import Incident, write_summary

SCENARIOS = ["upstream_rename", "schema_drift", "bad_join_fanout", "null_spike"]
REPO_ROOT = diagnose.REPO_ROOT
PR_BODIES_DIR = REPO_ROOT / "out" / "pr"
BEARER_RE = re.compile(r"Bearer \S+")

app = typer.Typer(add_completion=False)
console = Console(record=True)


def _scrub(text: str) -> str:
    api_key = os.environ.get("MODEL_API_KEY")
    if api_key:
        text = text.replace(api_key, "[REDACTED]")
    return BEARER_RE.sub("Bearer [REDACTED]", text)


def _write_artifact(path: Path, text: str) -> None:
    path.write_text(_scrub(text))


@app.callback()
def callback() -> None:
    """Pipeline Medic: diagnose, verify, and propose fixes for failing dbt jobs."""


def _check_line(ok: bool | None, label: str) -> str:
    if ok is None:
        return f"[dim]-[/dim] {label}"
    mark = "[green]✓[/green]" if ok else "[red]✗[/red]"
    return f"{mark} {label}"


def _propose_pr(scenario: str, diagnosis, verify_result: verify.VerifyResult) -> tuple[pr.PRResult, Path]:
    PR_BODIES_DIR.mkdir(parents=True, exist_ok=True)
    body_path = PR_BODIES_DIR / f"{scenario}.md"

    if diagnosis.action == "suggest":
        result = pr.prepare_pr(REPO_ROOT, scenario, diagnosis, verify_result, open_pr=False)
        body_path.write_text(result.body)
        return result, body_path

    with tempfile.TemporaryDirectory() as tmp:
        clone_dir = Path(tmp) / "repo"
        subprocess.run(
            ["git", "clone", "--quiet", str(REPO_ROOT), str(clone_dir)],
            check=True,
            capture_output=True,
            text=True,
        )
        result = pr.prepare_pr(clone_dir, scenario, diagnosis, verify_result, open_pr=False)
        body_path.write_text(result.body)
        return result, body_path


def run_incident(
    scenario: str, *, mock: bool, number: int, save_artifacts: Path | None = None
) -> tuple[Incident, float]:
    start = time.perf_counter()
    result = diagnose.diagnose_scenario_full(scenario, mock=mock)
    elapsed = time.perf_counter() - start
    diagnosis = result.diagnosis
    context = result.context
    failed_task = result.failed_task

    console.print(
        Panel(
            f"[bold]{context.failing_model}[/bold]  (Airflow task [cyan]{failed_task.task_id}[/cyan] "
            f"· dbt node {context.unique_id})\n{context.message.strip().splitlines()[0] if context.message else context.check}",
            title="[bold red]FAILED[/bold red]",
            title_align="left",
        )
    )

    diagnosis_lines = [
        f"[dim]model: muse-spark · Meta Model API · {elapsed:.1f}s[/dim]",
        f"Root cause : {diagnosis.root_cause}",
    ]
    if diagnosis.evidence:
        diagnosis_lines.append("Evidence   : " + "; ".join(diagnosis.evidence))
    diagnosis_lines.append(f"Confidence : {diagnosis.confidence:.2f}")
    console.print(Panel("\n".join(diagnosis_lines), title="[bold blue]DIAGNOSIS[/bold blue]", title_align="left"))

    if diagnosis.action == "fix":
        fix_body = Syntax(diagnosis.patch, "diff", theme="ansi_dark", word_wrap=True)
    else:
        fix_body = "[dim]No patch: this is a data-quality problem, not a code defect.[/dim]"
    console.print(Panel(fix_body, title="[bold blue]PROPOSED FIX[/bold blue]", title_align="left"))

    verify_result = verify.verify_scenario(scenario, diagnosis)
    if not mock and diagnosis.action == "fix" and not verify_result.passed and result.prompt:
        # One self-correction pass: feed the sandbox failure back to the model.
        console.print("[yellow]Sandbox rejected the fix; retrying once with verification feedback[/yellow]")
        retry_prompt = (
            f"{result.prompt}\n\nYour previous patch:\n{diagnosis.patch}\n\n"
            f"It FAILED sandbox verification (dbt build log tail):\n{verify_result.log[-2000:]}\n"
            "Return a corrected diagnosis and patch."
        )
        diagnosis = llm.diagnose(retry_prompt, scenario=f"{scenario}:retry")
        if diagnosis.action == "fix":
            console.print(Panel(Syntax(diagnosis.patch, "diff", theme="ansi_dark", word_wrap=True),
                                title="[bold blue]PROPOSED FIX (retry)[/bold blue]", title_align="left"))
        verify_result = verify.verify_scenario(scenario, diagnosis)
    if diagnosis.action == "suggest":
        verify_lines = [_check_line(True, "no patch to verify — flagged as an upstream data issue")]
    else:
        verify_lines = [
            _check_line(verify_result.build_success, "dbt compile"),
            _check_line(verify_result.build_success, "full dbt build (sandbox duckdb)"),
            _check_line(verify_result.row_count_within_tolerance, "row/null bounds within tolerance"),
            _check_line(verify_result.build_success, "contract check (dbt build)"),
            _check_line(verify_result.dag_success, "Airflow DAG re-run"),
        ]
    console.print(Panel("\n".join(verify_lines), title="[bold blue]SANDBOX VERIFY[/bold blue]", title_align="left"))

    pr_result, body_path = _propose_pr(scenario, diagnosis, verify_result)
    if diagnosis.action == "suggest":
        pr_lines = ["[yellow]suggested · needs owner[/yellow]", f"body: {body_path}"]
        status, status_kind = "suggested · needs owner", "suggested"
    else:
        passed = verify_result.passed
        tag = "[green]verified[/green]" if passed else "[red]verification failed[/red]"
        pr_lines = [
            f"[dim]dry-run[/dim] branch {pr_result.branch} · {tag}",
            f"body: {body_path}",
        ]
        status = f"{pr_result.branch} · {'verified' if passed else 'failed'}"
        status_kind = "verified" if passed else "failed"
    console.print(Panel("\n".join(pr_lines), title="[bold blue]PULL REQUEST[/bold blue]", title_align="left"))
    console.print()

    report_text = console.export_text(clear=True)
    if save_artifacts is not None:
        scenario_dir = save_artifacts / scenario
        scenario_dir.mkdir(parents=True, exist_ok=True)
        _write_artifact(scenario_dir / "report.txt", report_text)
        _write_artifact(scenario_dir / "diagnosis.json", diagnosis.model_dump_json(indent=2))
        _write_artifact(scenario_dir / "verify.log", verify_result.log)

    failure_headline = context.message.strip().splitlines()[0] if context.message else context.check
    incident = Incident(
        number=number,
        job=context.failing_model,
        failure=failure_headline,
        root_cause=diagnosis.root_cause,
        status=status,
        status_kind=status_kind,
    )
    return incident, elapsed


@app.command()
def run(
    scenario: str = typer.Option(None, "--scenario", help="Scenario name under scenarios/"),
    all_: bool = typer.Option(False, "--all", help="Run every scenario under scenarios/"),
    mock: bool = typer.Option(
        False, "--mock", help="Use recorded fixtures instead of calling the Meta Model API"
    ),
    save_artifacts: Path = typer.Option(
        None,
        "--save-artifacts",
        help="Directory to write per-scenario report.txt, diagnosis.json, verify.log, "
        "and a copy of summary.html",
    ),
) -> None:
    if bool(scenario) == all_:
        raise typer.BadParameter("pass exactly one of --scenario <name> or --all")

    scenarios = SCENARIOS if all_ else [scenario]
    if save_artifacts is not None:
        save_artifacts.mkdir(parents=True, exist_ok=True)

    incidents: list[Incident] = []
    diagnose_seconds: list[float] = []
    for number, name in enumerate(scenarios, start=1):
        incident, elapsed = run_incident(name, mock=mock, number=number, save_artifacts=save_artifacts)
        incidents.append(incident)
        diagnose_seconds.append(elapsed)

    out_path = write_summary(incidents, diagnose_seconds)
    console.print(f"[dim]Wrote summary to {out_path}[/dim]")
    if save_artifacts is not None:
        shutil.copy(out_path, save_artifacts / "summary.html")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
