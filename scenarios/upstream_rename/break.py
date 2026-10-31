"""Scenario: upstream_rename.

The orders source renames `customer_id` to `cust_id`, so stg_orders.sql
(which still selects `customer_id`) fails at build time.
"""

import sys
from pathlib import Path


def apply(project_dir: Path) -> None:
    orders_csv = project_dir / "seeds" / "orders.csv"
    lines = orders_csv.read_text().splitlines()
    lines[0] = lines[0].replace("customer_id", "cust_id")
    orders_csv.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    apply(Path(sys.argv[1]))
