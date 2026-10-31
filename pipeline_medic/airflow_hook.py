"""Run the `nightly_orders` DAG against a dbt project and find the failed task.

`dag.test()` executes every task in-process against the Airflow metadata DB
and streams task logs to stdout instead of a log file, so we capture stdout
to keep a copy of the failed task's log.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pendulum

REPO_ROOT = Path(__file__).resolve().parent.parent
DAG_PATH = REPO_ROOT / "airflow" / "dags" / "nightly_orders.py"

_env_prepared = False


def _ensure_airflow_env() -> None:
    """Point Airflow at an ephemeral SQLite metadata DB and migrate it.

    Tests set `AIRFLOW_HOME` themselves (see `tests/conftest.py`) before
    anything imports `airflow`, so this is a no-op there. Standalone CLI runs
    have no such setup, so this does it lazily on first use.
    """
    global _env_prepared
    if _env_prepared or os.environ.get("AIRFLOW_HOME"):
        _env_prepared = True
        return
    home = Path(tempfile.mkdtemp(prefix="pipeline-medic-airflow-"))
    os.environ["AIRFLOW_HOME"] = str(home)
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{home / 'airflow.db'}"
    os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "WARNING"
    from airflow.utils.db import initdb

    initdb()
    _env_prepared = True


@dataclass
class FailedTask:
    task_id: str
    try_number: int
    log: str


def _load_dag():
    spec = importlib.util.spec_from_file_location("nightly_orders", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def run_dag(project_dir: Path, duckdb_path: Path, logical_date=None):
    """Run `nightly_orders` against `project_dir`/`duckdb_path`.

    `nightly_orders.py` reads `PIPELINE_MEDIC_PROJECT_DIR` (falling back to
    the repo's real dbt project) so it can be pointed at a sandbox copy here.
    Returns `(dagrun, log_text)`.
    """
    _ensure_airflow_env()
    previous_project_dir = os.environ.get("PIPELINE_MEDIC_PROJECT_DIR")
    previous_duckdb_path = os.environ.get("DBT_DUCKDB_PATH")
    os.environ["PIPELINE_MEDIC_PROJECT_DIR"] = str(project_dir)
    os.environ["DBT_DUCKDB_PATH"] = str(duckdb_path)
    buffer = io.StringIO()
    try:
        dag = _load_dag()
        with contextlib.redirect_stdout(buffer):
            dagrun = dag.test(execution_date=logical_date or pendulum.now("UTC"))
        return dagrun, buffer.getvalue()
    finally:
        _restore_env("PIPELINE_MEDIC_PROJECT_DIR", previous_project_dir)
        _restore_env("DBT_DUCKDB_PATH", previous_duckdb_path)


def _restore_env(key: str, previous: str | None) -> None:
    if previous is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = previous


def find_failed_task(dagrun, log_text: str) -> FailedTask | None:
    """Return the task instance that actually failed, ignoring downstream skips."""
    for ti in dagrun.get_task_instances():
        if ti.state == "failed":
            return FailedTask(task_id=ti.task_id, try_number=ti.try_number, log=log_text)
    return None
