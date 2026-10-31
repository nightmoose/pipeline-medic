"""Verify a diagnosis's patch in a sandbox copy of the dbt project.

For a `fix` diagnosis: apply the patch on top of the scenario's breakage,
run `dbt compile` then `dbt build`, check row count and null rate for every
model the patch touches against a clean baseline build, and re-run the
Airflow DAG to confirm the original failure is gone.

For a `suggest` diagnosis there is no patch to apply, so verification is a
no-op that always passes.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Literal

import duckdb
from dbt.cli.main import dbtRunner
from pydantic import BaseModel, Field

from pipeline_medic import diagnose
from pipeline_medic.airflow_hook import find_failed_task, run_dag
from pipeline_medic.llm import Diagnosis

ROW_COUNT_TOLERANCE = 0.02
NULL_RATE_THRESHOLD = 0.01


class VerifyResult(BaseModel):
    scenario: str
    action: Literal["fix", "suggest"]
    applied: bool
    build_success: bool | None = None
    dag_success: bool | None = None
    model_names: list[str] = Field(default_factory=list)
    row_counts: dict[str, dict[str, int]] = Field(default_factory=dict)
    row_count_within_tolerance: bool | None = None
    null_rates: dict[str, float] = Field(default_factory=dict)
    null_rate_ok: bool | None = None
    matches_expected: bool | None = None
    log: str = ""

    @property
    def passed(self) -> bool:
        if self.action == "suggest":
            return not self.applied and self.matches_expected is not False
        return bool(
            self.applied
            and self.build_success
            and self.dag_success
            and self.row_count_within_tolerance
            and self.null_rate_ok
            and self.matches_expected is not False
        )


def parse_patch_targets(patch_text: str) -> list[str]:
    """Return the `b/...` file paths a unified diff touches, prefix stripped."""
    targets = []
    for line in patch_text.splitlines():
        if line.startswith("+++ "):
            path = line[4:].split("\t")[0].strip()
            targets.append(path[2:] if path.startswith("b/") else path)
    return targets


def apply_patch(sandbox_root: Path, patch_text: str) -> None:
    result = subprocess.run(
        ["git", "apply", "-p1"],
        cwd=sandbox_root,
        input=patch_text,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"patch failed to apply:\n{result.stdout}\n{result.stderr}")


def verify_scenario(
    scenario: str, diagnosis_result: Diagnosis, *, expected: dict | None = None
) -> VerifyResult:
    if diagnosis_result.action == "suggest":
        return VerifyResult(
            scenario=scenario,
            action="suggest",
            applied=False,
            matches_expected=_matches_expected(diagnosis_result, [], expected),
            log="No patch proposed; flagged as an upstream data-quality issue.",
        )

    touched = parse_patch_targets(diagnosis_result.patch)
    model_names = sorted(_model_names(touched))
    if not model_names:
        raise ValueError(f"patch for scenario {scenario!r} does not touch any model file")

    with (
        tempfile.TemporaryDirectory() as baseline_tmp,
        tempfile.TemporaryDirectory() as patched_tmp,
    ):
        baseline_dir = _copy_clean_project(Path(baseline_tmp))
        baseline_duckdb = Path(baseline_tmp) / "dev.duckdb"
        baseline_build = _dbt(["build"], baseline_dir, baseline_duckdb)

        patched_root = Path(patched_tmp)
        patched_dir = diagnose.build_sandbox(scenario, patched_root)
        patched_duckdb = patched_root / "dev.duckdb"
        apply_patch(patched_root, diagnosis_result.patch)

        compile_result = _dbt(["compile"], patched_dir, patched_duckdb)
        # Full build, not `--select <model>+`: some marts join two branches
        # (e.g. order_customer_detail joins fct_orders and stg_customers), so
        # a single model's ancestor/descendant closure can miss a sibling
        # branch the sandbox's empty duckdb file hasn't built yet.
        build_result = _dbt(["build"], patched_dir, patched_duckdb)
        build_success = bool(compile_result.success and build_result.success)

        row_counts: dict[str, dict[str, int]] = {}
        null_rates: dict[str, float] = {}
        row_count_ok = True
        null_rate_ok = True
        if build_success and baseline_build.success:
            for model in model_names:
                baseline_relation = _relation_name(baseline_dir, model)
                patched_relation = _relation_name(patched_dir, model)
                baseline_count = _row_count(baseline_duckdb, baseline_relation)
                patched_count = _row_count(patched_duckdb, patched_relation)
                row_counts[model] = {"baseline": baseline_count, "patched": patched_count}
                if not _within_tolerance(baseline_count, patched_count):
                    row_count_ok = False
                null_rate = _max_null_rate(patched_duckdb, patched_relation)
                null_rates[model] = null_rate
                if null_rate > NULL_RATE_THRESHOLD:
                    null_rate_ok = False
        else:
            row_count_ok = False
            null_rate_ok = False

        dag_success = None
        if build_success:
            dagrun, dag_log = run_dag(patched_dir, patched_duckdb)
            dag_success = find_failed_task(dagrun, dag_log) is None and dagrun.state == "success"

    return VerifyResult(
        scenario=scenario,
        action="fix",
        applied=True,
        build_success=build_success,
        dag_success=dag_success,
        model_names=model_names,
        row_counts=row_counts,
        row_count_within_tolerance=row_count_ok,
        null_rates=null_rates,
        null_rate_ok=null_rate_ok,
        matches_expected=_matches_expected(diagnosis_result, touched, expected),
        log=_build_log(
            model_names, compile_result, build_result, row_counts, null_rates, dag_success
        ),
    )


def _build_log(model_names, compile_result, build_result, row_counts, null_rates, dag_success) -> str:
    lines = [
        f"patched models: {', '.join(model_names)}",
        f"dbt compile: {'OK' if compile_result.success else 'FAILED'}",
        f"dbt build: {'OK' if build_result.success else 'FAILED'}",
    ]
    for model, counts in row_counts.items():
        lines.append(
            f"{model} row count: {counts['patched']} "
            f"(baseline {counts['baseline']}, tolerance {ROW_COUNT_TOLERANCE:.0%})"
        )
    for model, rate in null_rates.items():
        lines.append(f"{model} max null rate: {rate:.2%} (threshold {NULL_RATE_THRESHOLD:.0%})")
    if dag_success is None:
        lines.append("airflow dag re-run: skipped")
    else:
        lines.append(f"airflow dag re-run: {'OK' if dag_success else 'FAILED'}")
    return "\n".join(lines)


def _matches_expected(
    diagnosis_result: Diagnosis, touched: list[str], expected: dict | None
) -> bool | None:
    if expected is None:
        return None
    return (
        diagnosis_result.action == expected["action"]
        and sorted(touched) == sorted(expected["expected_patch_touches"])
    )


def _model_names(touched_paths: list[str]) -> set[str]:
    names = set()
    for raw_path in touched_paths:
        path = Path(raw_path)
        if path.suffix == ".sql" and "models" in path.parts:
            names.add(path.stem)
    return names


def _copy_clean_project(dest_root: Path) -> Path:
    project_dir = dest_root / "dbt_project"
    shutil.copytree(
        diagnose.DBT_PROJECT, project_dir, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb*")
    )
    return project_dir


def _dbt(args: list[str], project_dir: Path, duckdb_path: Path):
    previous = os.environ.get("DBT_DUCKDB_PATH")
    os.environ["DBT_DUCKDB_PATH"] = str(duckdb_path)
    try:
        return dbtRunner().invoke(
            ["--quiet", *args, "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]
        )
    finally:
        if previous is None:
            os.environ.pop("DBT_DUCKDB_PATH", None)
        else:
            os.environ["DBT_DUCKDB_PATH"] = previous


def _relation_name(project_dir: Path, model_name: str) -> str:
    manifest = json.loads((project_dir / "target" / "manifest.json").read_text())
    for node in manifest["nodes"].values():
        if node["resource_type"] == "model" and node["name"] == model_name:
            return node["relation_name"]
    raise ValueError(f"model {model_name!r} not found in manifest at {project_dir}")


def _row_count(duckdb_path: Path, relation_name: str) -> int:
    con = duckdb.connect(str(duckdb_path))
    try:
        return con.execute(f"select count(*) from {relation_name}").fetchone()[0]
    finally:
        con.close()


def _max_null_rate(duckdb_path: Path, relation_name: str) -> float:
    con = duckdb.connect(str(duckdb_path))
    try:
        row_count = con.execute(f"select count(*) from {relation_name}").fetchone()[0]
        if row_count == 0:
            return 0.0
        columns = [row[0] for row in con.execute(f"describe {relation_name}").fetchall()]
        return max(
            con.execute(
                f"select count(*) from {relation_name} where {col} is null"
            ).fetchone()[0]
            / row_count
            for col in columns
        )
    finally:
        con.close()


def _within_tolerance(baseline: int, patched: int) -> bool:
    if baseline == 0:
        return patched == 0
    return abs(patched - baseline) / baseline <= ROW_COUNT_TOLERANCE
