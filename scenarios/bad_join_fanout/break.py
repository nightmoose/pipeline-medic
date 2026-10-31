"""Scenario: bad_join_fanout.

The customers source sends a duplicate row for customer_id=1. The join in
order_customer_detail (fct_orders left join stg_customers on customer_id) is
not deduplicated, so that customer's orders fan out into two rows each and
the unique test on order_id fails.
"""

import sys
from pathlib import Path


def apply(project_dir: Path) -> None:
    customers_csv = project_dir / "seeds" / "customers.csv"
    lines = customers_csv.read_text().splitlines()
    duplicate = "1,alice@example.com,Alice Anderson,NA,2024-01-05"
    lines.append(duplicate)
    customers_csv.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    apply(Path(sys.argv[1]))
