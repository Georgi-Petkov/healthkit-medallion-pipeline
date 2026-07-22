# Databricks notebook source
# Bronze ingestion: Health Auto Export JSON files (Google Drive) -> Delta table
# Incremental via Auto Loader (cloudFiles) so new daily files are picked up
# without reprocessing everything each run.

GDRIVE_FOLDER = "https://drive.google.com/drive/folders/1Chpk-voGl9iJySpRFXOrn47CSlmDN-fs"
CONNECTION_NAME = "autohealthexport"
TARGET_TABLE = "workspace.healthkit.bronze_health_export"
CHECKPOINT_PATH = "/Volumes/workspace/healthkit/checkpoints/bronze_health_export"
SCHEMA_LOCATION = "/Volumes/workspace/healthkit/checkpoints/bronze_health_export_schema"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.healthkit")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.healthkit.checkpoints")

# COMMAND ----------

# Diagnostic: confirm the connection actually returns files/rows on this compute
# (this is the exact query from the SQL Editor attempt, now on a notebook instead
# of a SQL Warehouse, which is what the Beta connector actually requires).
diag = spark.sql(f"""
    SELECT * FROM read_files(
      '{GDRIVE_FOLDER}',
      format => 'json',
      `databricks.connection` => '{CONNECTION_NAME}',
      pathGlobFilter => '*.json',
      multiLine => true
    )
""")
print(f"Diagnostic batch read: {diag.count()} rows")
diag.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("databricks.connection", CONNECTION_NAME)
    .option("multiLine", "true")
    .option("pathGlobFilter", "*.json")
    .load(GDRIVE_FOLDER)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_name"))
)

(
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

# COMMAND ----------

result = spark.sql(f"SELECT COUNT(*) AS row_count, COUNT(DISTINCT _source_file) AS file_count FROM {TARGET_TABLE}")
result.show()
