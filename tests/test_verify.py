"""verify.py applies a diagnosis's patch in a sandbox and checks it fixes the build."""

import json
from pathlib import Path

import pytest

from pipeline_medic.diagnose import diagnose_scenario
from pipeline_medic.verify import verify_scenario

REPO_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = REPO_ROOT / "scenarios"
FIX_SCENARIOS = ["upstream_rename", "schema_drift", "bad_join_fanout"]


@pytest.mark.parametrize("scenario", FIX_SCENARIOS)
def test_verify_scenario_passes(airflow_db, scenario):
    expected = json.loads((SCENARIOS_DIR / scenario / "expected.json").read_text())
    diagnosis = diagnose_scenario(scenario, mock=True)

    result = verify_scenario(scenario, diagnosis, expected=expected)

    assert result.passed, result.log
    assert result.build_success
    assert result.dag_success
    assert result.row_count_within_tolerance
    assert result.null_rate_ok
    assert result.matches_expected


def test_verify_null_spike_is_a_noop(airflow_db):
    diagnosis = diagnose_scenario("null_spike", mock=True)

    result = verify_scenario("null_spike", diagnosis)

    assert result.action == "suggest"
    assert result.applied is False
    assert result.passed


def test_verify_corrupted_patch_raises(airflow_db):
    diagnosis = diagnose_scenario("upstream_rename", mock=True)
    corrupted_patch = diagnosis.patch.replace(
        "cast(order_date as date) as order_date,", "totally wrong context line"
    )
    diagnosis = diagnosis.model_copy(update={"patch": corrupted_patch})

    with pytest.raises(RuntimeError, match="patch failed to apply"):
        verify_scenario("upstream_rename", diagnosis)
