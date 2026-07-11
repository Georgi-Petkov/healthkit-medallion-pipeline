-- Weekly weight & body fat trend. Mirrors the logic from the original
-- WeightReport analysis notebook: sparse scale readings are forward-filled
-- to a full daily series *before* averaging per ISO week (so a week with
-- only one reading isn't diluted by nulls, and single-day noise like water
-- weight gets smoothed out), then week-over-week change is computed.
with daily_readings as (

    select
        metric_date,
        max(case when metric_name = 'weight_body_mass' then value end) as weight_kg,
        max(case when metric_name = 'body_fat_percentage' then value end) as body_fat_pct
    from {{ ref('stg_healthkit_metrics') }}
    where metric_name in ('weight_body_mass', 'body_fat_percentage')
    group by metric_date

),

bounds as (

    select min(metric_date) as min_date, max(metric_date) as max_date
    from daily_readings

),

date_spine as (

    select explode(sequence(min_date, max_date, interval 1 day)) as date
    from bounds

),

spine_with_readings as (

    select
        date_spine.date,
        daily_readings.weight_kg,
        daily_readings.body_fat_pct
    from date_spine
    left join daily_readings on daily_readings.metric_date = date_spine.date

),

forward_filled as (

    select
        date,
        last_value(weight_kg) ignore nulls over (
            order by date rows between unbounded preceding and current row
        ) as weight_kg_filled,
        last_value(body_fat_pct) ignore nulls over (
            order by date rows between unbounded preceding and current row
        ) as body_fat_pct_filled
    from spine_with_readings

),

-- The earliest days in the export have no prior reading to forward-fill
-- from. The source notebook manually seeds the first 3 such days with
-- known values from an earlier, pre-HealthKit weight record - replicated
-- here verbatim for fidelity with that analysis rather than left null.
-- body_fat has no equivalent seed in the source notebook, so its leading
-- gap is intentionally left unfilled (stays null).
null_weight_ranks as (

    select date, row_number() over (order by date) as null_rank
    from forward_filled
    where weight_kg_filled is null

),

leading_gap_patched as (

    select
        f.date,
        case
            when r.null_rank = 1 then 90.70
            when r.null_rank = 2 then 90.30
            when r.null_rank = 3 then 89.70
            else f.weight_kg_filled
        end as weight_kg,
        f.body_fat_pct_filled as body_fat_pct
    from forward_filled f
    left join null_weight_ranks r on r.date = f.date

),

with_iso_week as (

    select
        date,
        weight_kg,
        body_fat_pct,
        weekofyear(date) as iso_week,
        -- ISO year is the year of the Thursday in that ISO week (the
        -- standard ISO 8601 rule for resolving year-boundary weeks).
        year(date_trunc('week', date) + interval 3 days) as iso_year
    from leading_gap_patched

),

weekly as (

    select
        iso_year,
        iso_week,
        max(date) as week_end_date,
        avg(weight_kg) as weight_kg,
        avg(body_fat_pct) as body_fat_pct
    from with_iso_week
    group by iso_year, iso_week

),

with_trend as (

    select
        iso_year,
        iso_week,
        week_end_date,
        weight_kg,
        body_fat_pct,
        weight_kg - lag(weight_kg) over (order by week_end_date) as weekly_weight_change_kg,
        body_fat_pct - lag(body_fat_pct) over (order by week_end_date) as weekly_body_fat_change_pct
    from weekly

)

select
    week_end_date,
    lpad(cast(iso_year as string), 4, '0') || '-W' || lpad(cast(iso_week as string), 2, '0') as year_week,
    round(weight_kg, 2) as weight_kg,
    round(weekly_weight_change_kg, 2) as weekly_weight_change_kg,
    round(body_fat_pct, 2) as body_fat_pct,
    round(weekly_body_fat_change_pct, 2) as weekly_body_fat_change_pct
from with_trend
order by week_end_date
