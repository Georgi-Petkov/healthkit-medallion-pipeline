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

# Fetch files that are new OR were rewritten after we last ingested them.
# Health Auto Export overwrites same-named daily files in place (e.g. a
# backfill or a newly added metric), so skipping by name alone would
# silently miss the updated content. Silver's dedup (latest _ingested_at
# wins per metric+timestamp) resolves the re-landed rows.
last_ingested = {
    row._source_file: row.last_at
    for row in spark.sql(
        f"SELECT _source_file, MAX(_ingested_at) AS last_at FROM {TARGET_TABLE} GROUP BY _source_file"
    ).collect()
}
print(f"{len(last_ingested)} files already in Bronze")

# COMMAND ----------

from datetime import datetime, timezone

all_files, page_token = [], None
while True:
    resp = drive.files().list(
        q=f"'{FOLDER_ID}' in parents and trashed=false",
        fields="nextPageToken, files(id, name, modifiedTime)",
        pageSize=1000,
        pageToken=page_token,
    ).execute()
    all_files += [f for f in resp.get("files", []) if f["name"].endswith(".json")]
    page_token = resp.get("nextPageToken")
    if not page_token:
        break

def _modified(f):
    return datetime.fromisoformat(f["modifiedTime"].replace("Z", "+00:00"))

def _needs_ingest(f):
    prev = last_ingested.get(f["name"])
    if prev is None:
        return True
    return _modified(f) > prev.replace(tzinfo=timezone.utc)

# If Drive ever holds two files with the same name, keep only the newest.
newest_by_name = {}
for f in all_files:
    if f["name"] not in newest_by_name or _modified(f) > _modified(newest_by_name[f["name"]]):
        newest_by_name[f["name"]] = f

new_files = [f for f in newest_by_name.values() if _needs_ingest(f)]
print(f"{len(all_files)} .json files in Drive folder, {len(new_files)} new or updated")

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
