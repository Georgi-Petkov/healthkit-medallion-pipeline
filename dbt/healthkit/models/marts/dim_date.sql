-- Calendar dimension: one row per day, from the Monday of the first week with
-- data through the Sunday of the last week with data. Padding out to whole
-- ISO weeks (not just min/max metric_date) means the weekly facts, whose
-- week_start is a Monday that can fall before the first datapoint, still
-- resolve to a dim_date row. The natural date is the key: every fact here
-- already carries a real calendar date, so a surrogate key would only add a
-- lookup without buying anything.
with bounds as (

    select
        cast(date_trunc('week', min(metric_date)) as date) as first_day,
        date_add(cast(date_trunc('week', max(metric_date)) as date), 6) as last_day
    from {{ ref('stg_healthkit_metrics') }}

),

spine as (

    select explode(sequence(first_day, last_day, interval 1 day)) as date_day
    from bounds

)

select
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    day(date_day) as day_of_month,
    -- Spark's weekday() is 0 = Monday .. 6 = Sunday; shift to ISO 1..7.
    weekday(date_day) + 1 as iso_day_of_week,
    date_format(date_day, 'EEEE') as day_name,
    weekday(date_day) >= 5 as is_weekend,
    cast(date_trunc('week', date_day) as date) as week_start_date,
    -- Same ISO-year rule and label format as fct_weekly_weight_trends.year_week
    -- (ISO year = year of that week's Thursday), so the two join cleanly.
    year(date_trunc('week', date_day) + interval 3 days) as iso_year,
    weekofyear(date_day) as iso_week,
    lpad(cast(year(date_trunc('week', date_day) + interval 3 days) as string), 4, '0')
        || '-W' || lpad(cast(weekofyear(date_day) as string), 2, '0') as year_week
from spine
order by date_day
