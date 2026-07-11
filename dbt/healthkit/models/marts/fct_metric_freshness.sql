-- Per-metric data quality/pipeline health: coverage, latest data date, and
-- how stale each metric is relative to today - useful for monitoring whether
-- the ingest+pipeline is still healthy for every metric HealthKit reports.
with metrics as (

    select * from {{ ref('stg_healthkit_metrics') }}

),

per_metric as (

    select
        metric_name,
        count(*) as datapoint_count,
        min(metric_date) as first_date,
        max(metric_date) as last_date,
        max(ingested_at) as last_ingested_at,
        datediff(day, min(metric_date), max(metric_date)) + 1 as calendar_days_span,
        count(distinct metric_date) as days_with_data
    from metrics
    group by metric_name

)

select
    metric_name,
    datapoint_count,
    first_date,
    last_date,
    last_ingested_at,
    days_with_data,
    calendar_days_span,
    round(days_with_data / nullif(calendar_days_span, 0) * 100, 1) as coverage_pct,
    datediff(day, last_date, current_date()) as days_since_last_data
from per_metric
order by days_since_last_data desc, metric_name
