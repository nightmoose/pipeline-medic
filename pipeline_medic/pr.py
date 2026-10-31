"""Prepare a PR for a verified fix: branch, commit, markdown body.

Dry-run (the default) stops after a local commit on a new branch: nothing
is pushed and `gh pr create` is never invoked. Pass `open_pr=True` to also
push the branch and open the PR via the `gh` CLI.

All git/gh calls go through the injectable `run` callable so tests can
verify the exact commands without touching the network or this repo.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

from pipeline_medic.llm import Diagnosis
from pipeline_medic.verify import VerifyResult, parse_patch_targets


class PRResult(BaseModel):
    scenario: str
    action: Literal["fix", "suggest"]
    dry_run: bool
    opened: bool
    branch: str | None = None
    body: str
    commands: list[list[str]] = Field(default_factory=list)


def build_pr_body(scenario: str, diagnosis: Diagnosis, verify_result: VerifyResult) -> str:
    lines = [f"# pipeline-medic: {scenario}", "", "## Root cause", diagnosis.root_cause, ""]
    if diagnosis.evidence:
        lines.append("## Evidence")
        lines.extend(f"- {item}" for item in diagnosis.evidence)
        lines.append("")
    if diagnosis.action == "suggest":
        lines.append("## Recommendation")
        lines.append(
            "No code patch proposed: this is a data-quality problem in the upstream "
            "feed, not a defect in the dbt project."
        )
    else:
        lines.append("## Patch")
        lines.append("```diff")
        lines.append(diagnosis.patch.rstrip("\n"))
        lines.append("```")
        lines.append("")
        lines.append("## Verification")
        lines.append("```")
        lines.append(verify_result.log)
        lines.append("```")
    return "\n".join(lines) + "\n"


def prepare_pr(
    repo_root: Path,
    scenario: str,
    diagnosis: Diagnosis,
    verify_result: VerifyResult,
    *,
    open_pr: bool = False,
    run=subprocess.run,
) -> PRResult:
    body = build_pr_body(scenario, diagnosis, verify_result)

    if diagnosis.action == "suggest":
        return PRResult(
            scenario=scenario, action="suggest", dry_run=not open_pr, opened=False, body=body
        )

    commands: list[list[str]] = []

    def git(*args: str) -> None:
        argv = ["git", *args]
        commands.append(argv)
        run(argv, cwd=repo_root, check=True, capture_output=True, text=True)

    branch = f"pipeline-medic/{scenario}"
    git("checkout", "-b", branch)

    patch_file = Path(tempfile.mkstemp(suffix=".patch")[1])
    patch_file.write_text(diagnosis.patch)
    try:
        apply_argv = ["git", "apply", "-p1", str(patch_file)]
        commands.append(apply_argv)
        run(apply_argv, cwd=repo_root, check=True, capture_output=True, text=True)
    finally:
        patch_file.unlink(missing_ok=True)

    touched = parse_patch_targets(diagnosis.patch)
    git("add", *touched)
    git("commit", "-m", f"pipeline-medic: fix {scenario}\n\n{diagnosis.root_cause}")

    opened = False
    if open_pr:
        git("push", "-u", "origin", branch)
        gh_argv = [
            "gh",
            "pr",
            "create",
            "--title",
            f"pipeline-medic: fix {scenario}",
            "--body",
            body,
        ]
        commands.append(gh_argv)
        run(gh_argv, cwd=repo_root, check=True, capture_output=True, text=True)
        opened = True

    return PRResult(
        scenario=scenario,
        action="fix",
        dry_run=not open_pr,
        opened=opened,
        branch=branch,
        body=body,
        commands=commands,
    )
