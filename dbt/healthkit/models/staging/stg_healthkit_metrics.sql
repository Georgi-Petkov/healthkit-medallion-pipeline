with source as (

    select * from {{ source('healthkit_silver', 'healthkit_metrics') }}

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
        source_file,
        ingested_at

    from source
    where metric_name is not null
      and date is not null

)

select * from cleaned
