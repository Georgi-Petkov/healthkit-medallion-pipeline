#!/usr/bin/env python3
"""Ad hoc: check how many rows the Silver Lakeflow pipeline has dropped via
its data-quality expectations (valid_metric_name, valid_date).

Shows both the latest incremental batch (Autoloader only processes new
Bronze files each run, so this is usually a small number - NOT the size of
the table) and the all-time cumulative total across every recorded batch.

Auth: SQLAlchemy's databricks dialect with auth_type=azure-cli, i.e. your
local `az login` session - no token or secret needed.

Usage:
    python3 check_dropped_rows.py
"""
import os
import subprocess

# Some local network setups (e.g. a TLS-inspecting proxy) inject a CA cert
# that macOS trusts but Python's bundled `certifi` does not, which makes
# HTTPS calls to Databricks fail with SSLCertVerificationError. Build a
# combined bundle (certifi + macOS keychain) once and point Python at it.
# Harmless no-op if your network doesn't need it.
_ca_bundle = os.path.expanduser("~/.healthkit-ca-bundle.pem")
if not os.path.exists(_ca_bundle):
    import certifi

    with open(_ca_bundle, "w") as f:
        f.write(open(certifi.where()).read())
        for keychain in (
            "/Library/Keychains/System.keychain",
            "/System/Library/Keychains/SystemRootCertificates.keychain",
            os.path.expanduser("~/Library/Keychains/login.keychain-db"),
        ):
            result = subprocess.run(
                ["security", "find-certificate", "-a", "-p", keychain],
                capture_output=True,
                text=True,
            )
            f.write(result.stdout)

os.environ["SSL_CERT_FILE"] = _ca_bundle
os.environ["REQUESTS_CA_BUNDLE"] = _ca_bundle

from sqlalchemy import create_engine, text

DATABRICKS_HOST = "adb-7405605320524740.0.azuredatabricks.net"
HTTP_PATH = "/sql/1.0/warehouses/997b45263de388bd"
PIPELINE_ID = "f7550c31-7051-4c6a-a4f2-00d095261599"

LATEST_BATCH_QUERY = f"""
WITH latest_dq_event AS (
  SELECT timestamp, details:flow_progress:data_quality:expectations as expectations
  FROM event_log('{PIPELINE_ID}')
  WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality:expectations IS NOT NULL
  ORDER BY timestamp DESC
  LIMIT 1
)
SELECT
  row_expectations.dataset,
  row_expectations.name as expectation,
  row_expectations.passed_records,
  row_expectations.failed_records as dropped_records
FROM (
  SELECT explode(from_json(
    expectations,
    "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
  )) as row_expectations
  FROM latest_dq_event
)
"""

CUMULATIVE_QUERY = f"""
WITH all_dq_events AS (
  SELECT details:flow_progress:data_quality:expectations as expectations
  FROM event_log('{PIPELINE_ID}')
  WHERE event_type = 'flow_progress'
    AND details:flow_progress:data_quality:expectations IS NOT NULL
)
SELECT
  row_expectations.name as expectation,
  count(*) as num_batches,
  SUM(row_expectations.passed_records) as cumulative_passed,
  SUM(row_expectations.failed_records) as cumulative_dropped
FROM (
  SELECT explode(from_json(
    expectations,
    "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
  )) as row_expectations
  FROM all_dq_events
)
GROUP BY row_expectations.name
"""


def main() -> None:
    engine = create_engine(
        f"databricks://token:@{DATABRICKS_HOST}?http_path={HTTP_PATH}",
        connect_args={"auth_type": "azure-cli", "enable_telemetry": False},
    )
    with engine.connect() as conn:
        latest_rows = conn.execute(text(LATEST_BATCH_QUERY)).fetchall()
        cumulative_rows = conn.execute(text(CUMULATIVE_QUERY)).fetchall()

    if not latest_rows:
        print("No data-quality expectation results found yet - has the pipeline run?")
        return

    print("Latest incremental batch (Autoloader only processes new files each")
    print("run, so this reflects the most recent sync, not the whole table):")
    for r in latest_rows:
        print(f"  {r.dataset} | {r.expectation}: {r.dropped_records} dropped / {r.passed_records} passed")
    latest_total_dropped = sum(r.dropped_records for r in latest_rows)
    print(f"  Total dropped in latest batch: {latest_total_dropped}")

    print("\nAll-time cumulative (summed across every recorded batch):")
    for r in cumulative_rows:
        print(
            f"  {r.expectation}: {r.cumulative_dropped} dropped / {r.cumulative_passed} passed "
            f"across {r.num_batches} batches"
        )
    cumulative_total_dropped = sum(r.cumulative_dropped for r in cumulative_rows)
    print(f"  Total dropped all-time: {cumulative_total_dropped}")


if __name__ == "__main__":
    main()
