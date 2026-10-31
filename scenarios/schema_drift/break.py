"""Scenario: schema_drift.

The orders source starts sending `amount` as free-text instead of a number.
dbt's CSV type inference then loads the whole `orders.amount` seed column as
VARCHAR, which violates the INTEGER contract declared on fct_orders.
"""

import sys
from pathlib import Path


def apply(project_dir: Path) -> None:
    orders_csv = project_dir / "seeds" / "orders.csv"
    header, *rows = orders_csv.read_text().splitlines()
    columns = header.split(",")
    amount_idx = columns.index("amount")

    patched_rows = []
    for row in rows:
        fields = row.split(",")
        fields[amount_idx] = f"{fields[amount_idx]} USD"
        patched_rows.append(",".join(fields))

    orders_csv.write_text("\n".join([header, *patched_rows]) + "\n")


if __name__ == "__main__":
    apply(Path(sys.argv[1]))
