"""Parse dbt artifacts after a build to describe the failing node for the LLM."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb


@dataclass
class FailureContext:
    unique_id: str
    resource_type: str
    failing_model: str
    check: str
    status: str
    message: str
    compiled_sql: str | None
    raw_sql: str | None
    stats: dict[str, Any] = field(default_factory=dict)
    upstream: dict[str, str] = field(default_factory=dict)

    def to_prompt(self) -> str:
        lines = [
            f"Failing node: {self.unique_id} ({self.resource_type})",
            f"Failing model: {self.failing_model}",
            f"Check: {self.check}",
            f"Status: {self.status}",
            f"Error message:\n{self.message}",
        ]
        if self.raw_sql:
            lines.append(f"Model SQL ({self.failing_model}):\n{self.raw_sql}")
        if self.compiled_sql:
            lines.append(f"Compiled SQL that failed:\n{self.compiled_sql}")
        if self.stats:
            lines.append(f"Observed stats: {json.dumps(self.stats)}")
        for name, desc in self.upstream.items():
            lines.append(f"Upstream relation {name} (columns and sample rows, as currently loaded):\n{desc}")
        return "\n\n".join(lines)


def collect(project_dir: Path, duckdb_path: Path | None = None) -> FailureContext:
    """Read `target/run_results.json` and `target/manifest.json` under
    `project_dir` and describe the first failing node, enriched with row
    count / null rate stats queried from the duckdb file."""
    target_dir = project_dir / "target"
    run_results = json.loads((target_dir / "run_results.json").read_text())
    manifest = json.loads((target_dir / "manifest.json").read_text())

    result = _find_failing_result(run_results)
    if result is None:
        raise ValueError(f"no failing node in {target_dir / 'run_results.json'}")

    node = manifest["nodes"][result["unique_id"]]
    model_node = _model_node(node, manifest)
    check = _check_label(node, result.get("message") or "")

    if duckdb_path is None:
        duckdb_path = Path(os.environ.get("DBT_DUCKDB_PATH", project_dir / "dev.duckdb"))
    stats = _table_stats(
        duckdb_path,
        relation_name=model_node.get("relation_name") if model_node else None,
        column=node.get("column_name"),
        check_kind=(node.get("test_metadata") or {}).get("name"),
    )

    return FailureContext(
        unique_id=result["unique_id"],
        resource_type=node["resource_type"],
        failing_model=model_node["name"] if model_node else node["name"],
        check=check,
        status=result["status"],
        message=result.get("message") or "",
        compiled_sql=result.get("compiled_code"),
        raw_sql=model_node.get("raw_code") if model_node else None,
        stats=stats,
        upstream=_upstream_schemas(duckdb_path, model_node, manifest),
    )


def _upstream_schemas(duckdb_path: Path, model_node: dict | None, manifest: dict) -> dict[str, str]:
    """Columns (name: type) plus 3 sample rows for each relation the failing model
    reads, so the model sees what upstream actually looks like now."""
    if model_node is None or not Path(duckdb_path).exists():
        return {}
    out: dict[str, str] = {}
    try:
        con = duckdb.connect(str(duckdb_path))
    except duckdb.Error:
        return {}
    try:
        for dep in model_node.get("depends_on", {}).get("nodes", []):
            rel = (manifest["nodes"].get(dep) or {}).get("relation_name")
            if not rel:
                continue
            try:
                cols = con.execute(f"describe {rel}").fetchall()
                rows = con.execute(f"select * from {rel} limit 3").fetchall()
            except duckdb.Error:
                continue
            col_txt = ", ".join(f"{c[0]}: {c[1]}" for c in cols)
            row_txt = "\n".join(str(r) for r in rows)
            out[manifest["nodes"][dep]["name"]] = f"{col_txt}\n{row_txt}"
    finally:
        con.close()
    return out


def _find_failing_result(run_results: dict) -> dict | None:
    for result in run_results["results"]:
        if result["status"] in ("error", "fail"):
            return result
    return None


def _model_node(node: dict, manifest: dict) -> dict | None:
    """The model a node is about: itself if it's a model, else the model it tests."""
    if node["resource_type"] == "model":
        return node
    attached = node.get("attached_node") or next(iter(node.get("depends_on", {}).get("nodes", [])), None)
    return manifest["nodes"].get(attached) if attached else None


def _check_label(node: dict, message: str) -> str:
    if node["resource_type"] == "test":
        test_name = (node.get("test_metadata") or {}).get("name") or "test"
        column = node.get("column_name") or "?"
        return f"{test_name}_test:{column}"
    if "enforced contract" in message:
        return "contract_enforcement"
    return "compilation_error"


def _table_stats(
    duckdb_path: Path, relation_name: str | None, column: str | None, check_kind: str | None
) -> dict[str, Any]:
    if relation_name is None or not Path(duckdb_path).exists():
        return {}
    try:
        # Not read_only: dbt's own duckdb adapter may still hold the file open
        # in-process (e.g. right after a dbtRunner().invoke() call), and
        # duckdb refuses a second connection with a different configuration.
        con = duckdb.connect(str(duckdb_path))
    except duckdb.Error:
        return {}
    try:
        row_count = con.execute(f"select count(*) from {relation_name}").fetchone()[0]
        stats: dict[str, Any] = {"row_count": row_count}
        if column and check_kind == "not_null":
            null_count = con.execute(
                f"select count(*) from {relation_name} where {column} is null"
            ).fetchone()[0]
            stats["null_rate"] = null_count / row_count if row_count else 0.0
        elif column and check_kind == "unique":
            distinct_count = con.execute(
                f"select count(distinct {column}) from {relation_name}"
            ).fetchone()[0]
            stats["duplicate_rows"] = row_count - distinct_count
        return stats
    except duckdb.Error:
        return {}
    finally:
        con.close()
