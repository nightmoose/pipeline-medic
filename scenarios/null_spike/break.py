"""Scenario: null_spike.

The upstream customers feed starts sending null emails for some customers.
This is a data problem, not a code defect, so pipeline-medic should only
suggest a fix, not apply one.
"""

import sys
from pathlib import Path


def apply(project_dir: Path) -> None:
    customers_csv = project_dir / "seeds" / "customers.csv"
    header, *rows = customers_csv.read_text().splitlines()
    columns = header.split(",")
    email_idx = columns.index("email")

    patched_rows = []
    for row in rows:
        fields = row.split(",")
        if fields[0] in ("4", "7"):
            fields[email_idx] = ""
        patched_rows.append(",".join(fields))

    customers_csv.write_text("\n".join([header, *patched_rows]) + "\n")


if __name__ == "__main__":
    apply(Path(sys.argv[1]))
