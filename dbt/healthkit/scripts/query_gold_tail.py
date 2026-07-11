#!/usr/bin/env python3
"""Ad hoc smoke test: print the most recent rows of a healthkit.gold table.

Auth: SQLAlchemy's databricks dialect with auth_type=azure-cli, i.e. your
local `az login` session - no token or secret needed for this read-only,
ad hoc check.

Usage:
    python3 query_gold_tail.py [table] [limit]

    table   defaults to fct_weekly_trends
    limit   defaults to 10
"""
import sys

from sqlalchemy import create_engine, text

DATABRICKS_HOST = "adb-7405605320524740.0.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/997b45263de388bd"
CATALOG = "healthkit"
SCHEMA = "gold"

ORDER_BY_COLUMN = {
    "fct_weekly_trends": "week_start",
    "fct_daily_activity_summary": "metric_date",
    "fct_metric_freshness": "days_since_last_data",
}


def main() -> None:
    table = sys.argv[1] if len(sys.argv) > 1 else "fct_weekly_trends"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    if table not in ORDER_BY_COLUMN:
        known = ", ".join(ORDER_BY_COLUMN)
        raise SystemExit(f"Unknown table '{table}'. Known tables: {known}")

    order_by = ORDER_BY_COLUMN[table]

    engine = create_engine(
        f"databricks://token:@{DATABRICKS_HOST}"
        f"?http_path={HTTP_PATH}&catalog={CATALOG}&schema={SCHEMA}",
        connect_args={"auth_type": "azure-cli"},
    )

    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT * FROM {table} ORDER BY {order_by} DESC LIMIT :limit"),
            {"limit": limit},
        )
        cols = list(result.keys())
        rows = [list(row) for row in result.fetchall()]

    widths = [
        max(len(str(c)), *(len(str(r[i])) for r in rows)) if rows else len(str(c))
        for i, c in enumerate(cols)
    ]

    def fmt(vals):
        return "  ".join(str(v).ljust(w) for v, w in zip(vals, widths))

    print(fmt(cols))
    print(fmt(["-" * w for w in widths]))
    for r in rows:
        print(fmt(r))
    print(f"\n{len(rows)} row(s) from {CATALOG}.{SCHEMA}.{table}")


if __name__ == "__main__":
    main()
