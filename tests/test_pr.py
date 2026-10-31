"""pr.py prepares a branch + commit + PR body. All git/gh calls are mocked here:
no test in this file touches the network or this repo's real git state.
"""

import subprocess

from pipeline_medic.llm import Diagnosis
from pipeline_medic.pr import prepare_pr
from pipeline_medic.verify import VerifyResult

FIX_DIAGNOSIS = Diagnosis(
    root_cause="orders.customer_id was renamed to cust_id",
    evidence=["binder error"],
    confidence=0.9,
    patch=(
        "--- a/dbt_project/models/staging/stg_orders.sql\n"
        "+++ b/dbt_project/models/staging/stg_orders.sql\n"
        "@@ -1,2 +1,2 @@\n"
        "-customer_id,\n"
        "+cust_id as customer_id,\n"
    ),
    action="fix",
)

SUGGEST_DIAGNOSIS = Diagnosis(
    root_cause="upstream nulls",
    evidence=[],
    confidence=0.8,
    patch="",
    action="suggest",
)

FIX_VERIFY_RESULT = VerifyResult(
    scenario="upstream_rename",
    action="fix",
    applied=True,
    build_success=True,
    dag_success=True,
    row_count_within_tolerance=True,
    null_rate_ok=True,
    log="dbt build: OK",
)

SUGGEST_VERIFY_RESULT = VerifyResult(scenario="null_spike", action="suggest", applied=False)


class FakeRun:
    def __init__(self):
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")


def test_dry_run_commits_locally_but_does_not_push_or_open(tmp_path):
    fake_run = FakeRun()

    result = prepare_pr(
        tmp_path, "upstream_rename", FIX_DIAGNOSIS, FIX_VERIFY_RESULT, run=fake_run
    )

    assert result.dry_run is True
    assert result.opened is False
    assert result.branch == "pipeline-medic/upstream_rename"
    assert ["git", "checkout", "-b", "pipeline-medic/upstream_rename"] in fake_run.calls
    assert any(call[:2] == ["git", "commit"] for call in fake_run.calls)
    assert not any(call[:2] == ["git", "push"] for call in fake_run.calls)
    assert not any(call[0] == "gh" for call in fake_run.calls)
    assert "Root cause" in result.body


def test_open_pr_pushes_and_calls_gh(tmp_path):
    fake_run = FakeRun()

    result = prepare_pr(
        tmp_path, "upstream_rename", FIX_DIAGNOSIS, FIX_VERIFY_RESULT, open_pr=True, run=fake_run
    )

    assert result.opened is True
    assert result.dry_run is False
    assert any(call[:2] == ["git", "push"] for call in fake_run.calls)
    assert any(call[0] == "gh" for call in fake_run.calls)


def test_suggest_diagnosis_never_touches_git(tmp_path):
    fake_run = FakeRun()

    result = prepare_pr(
        tmp_path, "null_spike", SUGGEST_DIAGNOSIS, SUGGEST_VERIFY_RESULT, run=fake_run
    )

    assert result.action == "suggest"
    assert result.opened is False
    assert result.branch is None
    assert fake_run.calls == []
    assert "data-quality" in result.body.lower()
