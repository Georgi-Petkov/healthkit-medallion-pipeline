-- Weekly weight & body fat trend, mirroring the logic from the original
-- WeightReport analysis notebook: average whatever real scale readings
-- exist within each ISO week (a week with only 3 real readings averages
-- exactly those 3 - no forward-filled/padded values are included), so
-- single-day noise like water weight gets smoothed without ever fabricating
-- data for days that haven't been weighed in yet. Then compute
-- week-over-week change on those weekly averages, plus HTML-formatted
-- colored-arrow display columns replicating the notebook's annotate_loss().
with daily_readings as (

    select
        metric_date,
        max(case when metric_name = 'weight_body_mass' then value end) as weight_kg,
        max(case when metric_name = 'body_fat_percentage' then value end) as body_fat_pct
    from {{ ref('stg_healthkit_metrics') }}
    where metric_name in ('weight_body_mass', 'body_fat_percentage')
    group by metric_date

),

first_weight_date as (

    select min(metric_date) as min_date
    from daily_readings
    where weight_kg is not null

),

-- The 3 calendar days immediately before the first real weight reading have
-- no prior value to reference, so the source notebook seeds them with fixed
-- values from an earlier, pre-HealthKit weight record - replicated here
-- verbatim for fidelity with that analysis. body_fat has no equivalent seed
-- in the source notebook, so it's left null for these days.
leading_seed_days as (

    select
        date_add(f.min_date, -offsets.n) as metric_date,
        case offsets.n
            when 3 then 90.70
            when 2 then 90.30
            when 1 then 89.70
        end as weight_kg,
        cast(null as double) as body_fat_pct
    from first_weight_date f
    cross join (select explode(sequence(1, 3)) as n) offsets

),

combined_readings as (

    select
        coalesce(d.metric_date, s.metric_date) as metric_date,
        coalesce(d.weight_kg, s.weight_kg) as weight_kg,
        coalesce(d.body_fat_pct, s.body_fat_pct) as body_fat_pct
    from daily_readings d
    full outer join leading_seed_days s on d.metric_date = s.metric_date

),

with_iso_week as (

    select
        metric_date,
        weight_kg,
        body_fat_pct,
        weekofyear(metric_date) as iso_week,
        -- ISO year is the year of the Thursday in that ISO week (the
        -- standard ISO 8601 rule for resolving year-boundary weeks).
        year(date_trunc('week', metric_date) + interval 3 days) as iso_year
    from combined_readings

),

weekly as (

    select
        iso_year,
        iso_week,
        max(metric_date) as week_end_date,
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
        body_fat_pct - lag(body_fat_pct) over (order by week_end_date) as weekly_body_fat_change_pp
    from weekly

)

select
    week_end_date,
    lpad(cast(iso_year as string), 4, '0') || '-W' || lpad(cast(iso_week as string), 2, '0') as year_week,
    round(weight_kg, 2) as weight_kg,
    round(weekly_weight_change_kg, 2) as weekly_weight_change_kg,
    round(body_fat_pct, 2) as body_fat_pct,
    round(weekly_body_fat_change_pp, 2) as weekly_body_fat_change_pp,
    {{ color_change('weekly_weight_change_kg') }} as weekly_weight_change_display,
    {{ color_change('weekly_body_fat_change_pp') }} as weekly_body_fat_change_display
from with_trend
order by week_end_date
