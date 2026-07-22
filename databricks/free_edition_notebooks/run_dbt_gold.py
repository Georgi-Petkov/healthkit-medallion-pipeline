# Databricks notebook source
# Scheduled dbt run: refreshes the Gold marts from whatever is currently in
# Bronze/Silver. Clones the public repo fresh each run (no persistent
# checkout to keep in sync) and runs against the same SQL Warehouse the
# local dev setup uses, via dbt/healthkit/profiles.yml + a Databricks
# secret scope (healthkit-dbt) instead of a committed .env file.

REPO_URL = "https://github.com/Georgi-Petkov/healthkit-medallion-pipeline.git"
REPO_DIR = "/tmp/healthkit-medallion-pipeline"

# COMMAND ----------

# MAGIC %sh rm -rf /tmp/healthkit-medallion-pipeline && git clone --depth 1 https://github.com/Georgi-Petkov/healthkit-medallion-pipeline.git /tmp/healthkit-medallion-pipeline

# COMMAND ----------

# MAGIC %pip install dbt-databricks

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os

os.environ["DATABRICKS_HOST"] = dbutils.secrets.get("healthkit-dbt", "databricks_host")
os.environ["DATABRICKS_HTTP_PATH"] = dbutils.secrets.get("healthkit-dbt", "databricks_http_path")
os.environ["DATABRICKS_TOKEN"] = dbutils.secrets.get("healthkit-dbt", "databricks_token")
os.environ["DBT_PROFILES_DIR"] = "/tmp/healthkit-medallion-pipeline/dbt/healthkit"

# COMMAND ----------

import subprocess

result = subprocess.run(
    ["dbt", "build"],
    cwd="/tmp/healthkit-medallion-pipeline/dbt/healthkit",
    capture_output=True,
    text=True,
)
print(result.stdout)
print(result.stderr)
if result.returncode != 0:
    raise RuntimeError(f"dbt build failed with exit code {result.returncode}")
