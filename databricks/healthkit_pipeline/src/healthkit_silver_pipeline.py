"""Bronze -> Silver Lakeflow Declarative Pipeline for HealthKit metrics.

Reads raw Health Auto Export JSON payloads from ADLS Gen2 (bronze) via
Autoloader, flattens each metric's datapoints into rows, and writes a
deduped current-state table plus one SCD Type 2 history table (step_count)
via Auto CDC.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

BRONZE_PATH = "abfss://bronze@healthkitdatalake.dfs.core.windows.net/raw/healthkit/"
BRONZE_SCHEMA_LOCATION = "abfss://bronze@healthkitdatalake.dfs.core.windows.net/_autoloader_schema/healthkit_metrics"


@dp.table(
    name="bronze_healthkit_raw",
    comment="Raw Health Auto Export JSON payloads, one row per file, ingested via Autoloader.",
)
def bronze_healthkit_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", BRONZE_SCHEMA_LOCATION)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(BRONZE_PATH)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("file_modification_time", F.col("_metadata.file_modification_time"))
    )


@dp.table(
    name="silver_healthkit_metrics_staging",
    comment="Flattened HealthKit metric datapoints: one row per (metric, datapoint) across all Bronze files.",
)
@dp.expect_or_drop("valid_metric_name", "metric_name IS NOT NULL")
@dp.expect_or_drop("valid_date", "date IS NOT NULL")
def silver_healthkit_metrics_staging():
    bronze = dp.read_stream("bronze_healthkit_raw")

    metrics = bronze.select(
        "source_file",
        "file_modification_time",
        F.explode("data.metrics").alias("metric"),
    )

    datapoints = metrics.select(
        "source_file",
        "file_modification_time",
        F.col("metric.name").alias("metric_name"),
        F.col("metric.units").alias("units"),
        F.explode("metric.data").alias("point"),
    )

    return datapoints.select(
        "metric_name",
        "units",
        F.to_timestamp(F.col("point.date"), "yyyy-MM-dd HH:mm:ss xx").alias("date"),
        F.col("point.qty").cast("double").alias("value_qty"),
        F.col("point.avg").cast("double").alias("value_avg"),
        F.col("point.min").cast("double").alias("value_min"),
        F.col("point.max").cast("double").alias("value_max"),
        F.to_json("point").alias("value_raw"),
        "source_file",
        "file_modification_time",
        F.current_timestamp().alias("ingested_at"),
    )


# --- Current-state table: latest value wins per (metric_name, date). ---
# sequence_by is a composite struct: primarily file_modification_time (a
# re-sent/overlapping export only overwrites a row if it's from a file
# modified more recently than what's already there, so re-running an
# overlapping backfill export is safe), with coalesce(value_qty, value_avg)
# as a tie-break for the rare case where two Bronze files land in the same
# second - e.g. Health Auto Export's batched sync occasionally reports the
# still-accumulating current hour twice, once per simultaneous POST, with
# a slightly different value each time. Since these are all
# monotonically-accumulating-within-the-hour metrics (steps, kcal burned,
# minutes, etc.), the higher value is always the more complete one, never
# a conflicting independent measurement - so ties resolve deterministically
# to MAX instead of an arbitrary (and previously inconsistent) pick.
dp.create_streaming_table(
    name="healthkit_metrics",
    comment="Deduped, current-value HealthKit metric datapoints. One row per (metric_name, date).",
)

dp.create_auto_cdc_flow(
    target="healthkit_metrics",
    source="silver_healthkit_metrics_staging",
    keys=["metric_name", "date"],
    sequence_by=F.struct(
        F.col("file_modification_time"),
        F.coalesce(F.col("value_qty"), F.col("value_avg")),
    ),
    stored_as_scd_type="1",
)


# --- SCD Type 2 history demo for step_count via Auto CDC. ---
# Keeps every distinct value_qty ever seen for a given date, with
# __START_AT / __END_AT marking the validity window of each version -
# useful when HealthKit retroactively revises a past day's step count.
@dp.view(name="step_count_changes")
def step_count_changes():
    return dp.read_stream("silver_healthkit_metrics_staging").filter(
        "metric_name = 'step_count'"
    )


dp.create_streaming_table(
    name="step_count_history",
    comment="SCD Type 2 history of step_count datapoints, maintained via Auto CDC.",
)

dp.create_auto_cdc_flow(
    target="step_count_history",
    source="step_count_changes",
    keys=["metric_name", "date"],
    sequence_by="file_modification_time",
    stored_as_scd_type="2",
    track_history_column_list=["value_qty"],
)
