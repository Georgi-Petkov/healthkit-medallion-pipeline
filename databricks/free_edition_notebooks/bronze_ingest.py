# Databricks notebook source
# Bronze ingestion: Health Auto Export JSON files via Google Drive API v3 + a
# service account, landed into a Delta table. Replaces an earlier version that
# used Databricks' Unity Catalog Google Drive Beta connector -- see
# bronze_ingest_v1_retired.py for why that was abandoned (its OAuth flow never
# requested offline access, so tokens couldn't auto-refresh between scheduled
# runs). A service account key has no expiration and needs no interactive
# consent, so it doesn't have that failure mode.
#
# Credentials: a service account JSON key, stored in the healthkit-dbt secret
# scope, for a Google Cloud project with the Drive API enabled. The target
# folder is shared with the service account's email as Viewer.
#
# FOLDER_ID changed 2026-08-18: Health Auto Export turned out to be syncing
# into a different Drive folder than the one originally shared with this
# service account (a stale duplicate "Health Auto Export" folder existed from
# 2025-10-11, predating this project; the app silently reverted to writing
# into a "New Folder" under it at some point after 2026-08-02, so nothing
# ingested for two weeks despite the job succeeding daily -- 0 new files is
# a normal, non-erroring result). Repointed at the folder the app is actually
# writing to, rather than fighting the app's destination choice again.

# MAGIC %pip install google-api-python-client google-auth

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

FOLDER_ID = "1eDnSR0rDEuO4c2f2uf-ow1B3BCcvCGwY"  # Health Auto Export's actual sync target
TARGET_TABLE = "workspace.healthkit.bronze_health_export"

# COMMAND ----------

import json

from google.oauth2 import service_account
from googleapiclient.discovery import build

sa_info = json.loads(dbutils.secrets.get("healthkit-dbt", "gdrive_service_account_json"))
credentials = service_account.Credentials.from_service_account_info(
    sa_info, scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=credentials)
print(f"Reading Drive as service account: {credentials.service_account_email}")

# COMMAND ----------

# Only fetch files not already landed in Bronze -- _source_file already tracks
# this from the original Auto Loader-based ingestion, reused here so nothing
# downstream needs to change.
already_ingested = {
    row._source_file
    for row in spark.sql(f"SELECT DISTINCT _source_file FROM {TARGET_TABLE}").collect()
}
print(f"{len(already_ingested)} files already in Bronze")

# COMMAND ----------

results = drive.files().list(
    q=f"'{FOLDER_ID}' in parents and trashed=false",
    fields="files(id, name)",
    pageSize=1000,
).execute()
all_files = [f for f in results.get("files", []) if f["name"].endswith(".json")]
new_files = [f for f in all_files if f["name"] not in already_ingested]
print(f"{len(all_files)} .json files in Drive folder, {len(new_files)} new")

# COMMAND ----------

import io
from datetime import datetime, timezone

from googleapiclient.http import MediaIoBaseDownload
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("data", StringType(), True),
    StructField("_rescued_data", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_source_file", StringType(), True),
])

rows = []
for f in new_files:
    request = drive.files().get_media(fileId=f["id"])
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    raw_content = buf.getvalue().decode("utf-8")
    # The raw export file is {"data": {"metrics": [...]}} -- the pre-existing
    # rows in this table (landed by the old Auto Loader-based ingestion) store
    # only the inner "data" object as the `data` column's content, one level
    # flattened vs. the raw file. base_healthkit_metrics.sql's `payload:metrics`
    # path assumes that flattened shape. Unwrap here to match it, or new rows
    # silently fail to explode (found 2026-07-24: 3 days of real weight/metric
    # data landed in Bronze but produced zero rows in Silver until this fix).
    content = json.dumps(json.loads(raw_content)["data"])
    rows.append(Row(
        data=content,
        _rescued_data=None,
        _ingested_at=datetime.now(timezone.utc),
        _source_file=f["name"],
    ))

if rows:
    df = spark.createDataFrame(rows, schema=schema)
    df.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
    print(f"Appended {len(rows)} new files to {TARGET_TABLE}")
else:
    print("No new files to ingest -- Bronze is already up to date")
