with source as (

    select * from {{ source('healthkit_silver', 'healthkit_metrics') }}

),

cleaned as (

    select
        metric_name,
        lower(units) as units,
        cast(date as date) as metric_date,
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
