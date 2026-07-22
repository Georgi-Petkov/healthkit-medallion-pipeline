-- Bronze -> Silver: explode Apple Health Auto Export's nested JSON
-- ({"metrics": [{"name", "units", "data": [reading, ...]}, ...]}) into one row
-- per (metric, reading). Uses Databricks' native VARIANT type rather than a
-- rigid from_json schema, since readings are heterogeneously shaped --
-- most are {source, date, qty}, but sleep_analysis has no qty at all
-- (core/rem/deep/awake/totalSleep/inBedStart/inBedEnd/... instead).
-- value_raw preserves the full original reading so anything not captured
-- by the generic value_qty/avg/min/max columns is still recoverable downstream.

with bronze_parsed as (

    select
        _source_file,
        _ingested_at,
        parse_json(data) as payload
    from {{ source('healthkit_bronze', 'bronze_health_export') }}

),

metrics_exploded as (

    select
        b._source_file,
        b._ingested_at,
        m.value:name::string as metric_name,
        m.value:units::string as units,
        m.value:data as readings
    from bronze_parsed b,
    lateral variant_explode(b.payload:metrics) as m

),

readings_exploded as (

    select
        me._source_file,
        me._ingested_at,
        me.metric_name,
        me.units,
        r.value as reading
    from metrics_exploded me,
    lateral variant_explode(me.readings) as r

),

deduped as (

    select
        metric_name,
        units,
        try_to_timestamp(reading:date::string, 'yyyy-MM-dd HH:mm:ss XX') as date,
        try_cast(reading:qty::string as double) as value_qty,
        try_cast(coalesce(reading:Avg::string, reading:avg::string) as double) as value_avg,
        try_cast(coalesce(reading:Min::string, reading:min::string) as double) as value_min,
        try_cast(coalesce(reading:Max::string, reading:max::string) as double) as value_max,
        to_json(reading) as value_raw,
        _source_file as source_file,
        _ingested_at as ingested_at
    from readings_exploded
    -- The same (metric_name, date) reading can land twice if the user re-runs
    -- an overlapping Health Auto Export backfill -- two different Bronze
    -- files, same underlying datapoint. _ingested_at is current_timestamp()
    -- at Auto Loader processing time (see databricks/free_edition_notebooks/
    -- bronze_ingest.py), so "most recently processed wins" is the same
    -- recency semantics the original Lakeflow pipeline's Auto CDC
    -- (sequence_by file_modification_time) used, just keyed on ingestion
    -- order instead of file mtime.
    qualify row_number() over (
        partition by metric_name, try_to_timestamp(reading:date::string, 'yyyy-MM-dd HH:mm:ss XX')
        order by _ingested_at desc
    ) = 1

)

select * from deduped
