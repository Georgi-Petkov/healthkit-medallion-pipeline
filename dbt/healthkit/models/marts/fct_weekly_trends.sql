-- Week-over-week trend for core activity metrics: weekly totals/averages plus
-- absolute and percent change vs. the prior week, via window functions.
with daily as (

    select * from {{ ref('fct_daily_activity_summary') }}

),

weekly as (

    select
        date_trunc('week', metric_date) as week_start,
        sum(total_steps) as total_steps,
        sum(active_energy_kcal) as active_energy_kcal,
        sum(exercise_minutes) as exercise_minutes,
        avg(avg_heart_rate_bpm) as avg_heart_rate_bpm,
        avg(resting_heart_rate_bpm) as avg_resting_heart_rate_bpm,
        count(*) as days_with_data
    from daily
    group by date_trunc('week', metric_date)

),

with_trend as (

    select
        week_start,
        total_steps,
        active_energy_kcal,
        exercise_minutes,
        avg_heart_rate_bpm,
        avg_resting_heart_rate_bpm,
        days_with_data,
        lag(total_steps) over (order by week_start) as prev_week_total_steps,
        lag(active_energy_kcal) over (order by week_start) as prev_week_active_energy_kcal,
        lag(avg_resting_heart_rate_bpm) over (order by week_start) as prev_week_resting_heart_rate_bpm
    from weekly

)

select
    week_start,
    total_steps,
    active_energy_kcal,
    exercise_minutes,
    avg_heart_rate_bpm,
    avg_resting_heart_rate_bpm,
    days_with_data,
    total_steps - prev_week_total_steps as steps_change_vs_prev_week,
    round(
        try_divide(total_steps - prev_week_total_steps, prev_week_total_steps) * 100, 1
    ) as steps_pct_change_vs_prev_week,
    round(
        try_divide(active_energy_kcal - prev_week_active_energy_kcal, prev_week_active_energy_kcal) * 100, 1
    ) as active_energy_pct_change_vs_prev_week,
    round(avg_resting_heart_rate_bpm - prev_week_resting_heart_rate_bpm, 1) as resting_hr_change_vs_prev_week
from with_trend
order by week_start
