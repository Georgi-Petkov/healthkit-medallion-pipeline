with source as (

    select * from {{ ref('base_healthkit_metrics') }}

),

cleaned as (

    select
        metric_name,
        lower(units) as units,
        -- metric_date must reflect the LOCAL calendar day (matching what
        -- the phone shows), not the UTC day. `date` here is already a UTC
        -- instant (the local offset was consumed during parsing in the
        -- Lakeflow pipeline), so casting it to a date truncates by session
        -- timezone (UTC) - which silently reassigns hours near local
        -- midnight to the wrong day, every day, for anyone not in UTC.
        -- value_raw still has the original "yyyy-MM-dd HH:mm:ss +offset"
        -- string from HealthKit, so pull the local date straight from its
        -- first 10 characters instead - correct regardless of warehouse
        -- session timezone or which offset a given reading was taken in.
        to_date(substring(get_json_object(value_raw, '$.date'), 1, 10)) as metric_date,
        date as metric_timestamp,
        value_qty,
        value_avg,
        value_min,
        value_max,
        coalesce(value_qty, value_avg) as value,
        -- Health Auto Export emits one extra datapoint per day, per metric,
        -- timestamped at exactly local 00:00:00 - a daily-granularity
        -- rollup (the full day's total/average), alongside the genuine
        -- intraday breakdown points (arbitrary sync times like 08:07:39).
        -- Downstream daily aggregations must prefer this rollup value over
        -- summing/averaging it together with the intraday points, or every
        -- cumulative metric (steps, active energy, etc.) gets silently
        -- double-counted. See fct_daily_activity_summary.
        substring(get_json_object(value_raw, '$.date'), 12, 8) = '00:00:00' as is_daily_rollup,
        source_file,
        ingested_at

    from source
    where metric_name is not null
      and date is not null

)

select * from cleaned
