#!/usr/bin/env python3
"""Ad hoc: check how many rows the Silver Lakeflow pipeline has dropped via
its data-quality expectations (valid_metric_name, valid_date).

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

QUERY = f"""
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


def main() -> None:
    engine = create_engine(
        f"databricks://token:@{DATABRICKS_HOST}?http_path={HTTP_PATH}",
        connect_args={"auth_type": "azure-cli", "enable_telemetry": False},
    )
    with engine.connect() as conn:
        rows = conn.execute(text(QUERY)).fetchall()

    if not rows:
        print("No data-quality expectation results found yet - has the pipeline run?")
        return

    for r in rows:
        print(f"{r.dataset} | {r.expectation}: {r.dropped_records} dropped / {r.passed_records} passed")

    total_dropped = sum(r.dropped_records for r in rows)
    print(f"\nTotal dropped rows (latest pipeline run): {total_dropped}")


if __name__ == "__main__":
    main()
