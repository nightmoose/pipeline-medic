"""Real model diffs use dbt-project-relative paths and sometimes wrong hunk headers."""
from pathlib import Path

from pipeline_medic.pr import normalize_patch_paths
from pipeline_medic.verify import apply_patch


def test_normalize_patch_paths():
    out = normalize_patch_paths("diff --git a/models/x.sql b/models/x.sql\n--- a/models/x.sql\n+++ b/models/x.sql\n-models/q\n")
    assert "a/dbt_project/models/x.sql b/dbt_project/models/x.sql" in out
    assert "+++ b/dbt_project/models/x.sql" in out
    assert out.endswith("-models/q\n")


def test_apply_patch_fuzzy_wrong_hunk_header(tmp_path: Path):
    f = tmp_path / "dbt_project" / "models" / "m.sql"
    f.parent.mkdir(parents=True)
    f.write_text("-- header\n\nselect\n    id,\n    amount,\n    status\nfrom t\n")
    patch = (
        "--- a/models/m.sql\n+++ b/models/m.sql\n@@ -40,6 +40,6 @@\n"
        "     id,\n-    amount,\n+    cast(amount as integer) as amount,\n     status\n"
    )
    apply_patch(tmp_path, patch)
    assert "cast(amount as integer) as amount" in f.read_text()
