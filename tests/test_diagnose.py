"""diagnose.py ties airflow_hook + collector + llm together, in --mock mode."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline_medic.diagnose import diagnose_scenario

REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = REPO_ROOT / "scenarios"
SCENARIOS = ["upstream_rename", "schema_drift", "bad_join_fanout", "null_spike"]


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_diagnose_scenario_matches_expected(airflow_db, scenario):
    expected = json.loads((SCENARIOS_DIR / scenario / "expected.json").read_text())

    diagnosis = diagnose_scenario(scenario, mock=True)

    assert diagnosis.action == expected["action"]
    assert diagnosis.root_cause == expected["root_cause"]
    if expected["action"] == "fix":
        assert diagnosis.patch
    else:
        assert diagnosis.patch == ""


def test_diagnose_cli_mock(airflow_db):
    result = subprocess.run(
        [sys.executable, "-m", "pipeline_medic.diagnose", "--scenario", "null_spike", "--mock"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["action"] == "suggest"
