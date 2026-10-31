"""Point Airflow at an ephemeral SQLite metadata DB.

This has to happen before anything imports `airflow`, since Airflow reads
these settings at import time. `conftest.py` is imported before test modules,
so plain module-level code here runs early enough.
"""

import os
import tempfile
from pathlib import Path

_AIRFLOW_HOME = Path(tempfile.mkdtemp(prefix="pipeline-medic-airflow-"))
os.environ.setdefault("AIRFLOW_HOME", str(_AIRFLOW_HOME))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{_AIRFLOW_HOME / 'airflow.db'}"
)
os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def airflow_db():
    """Migrate the ephemeral Airflow metadata DB once per test session."""
    from airflow.utils.db import initdb

    initdb()
