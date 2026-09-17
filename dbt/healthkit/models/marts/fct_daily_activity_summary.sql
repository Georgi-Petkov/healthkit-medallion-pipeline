-- One row per day, pivoting key activity/vitals metrics into columns for easy dashboarding.
with metrics as (

    select * from {{ ref('stg_healthkit_metrics') }}

),

-- Resolve one value per (metric_name, metric_date) before pivoting: always
-- sum/average the intraday points, and never trust an is_daily_rollup row
-- as a shortcut daily total. is_daily_rollup flags rows timestamped at
-- exactly local midnight, which were originally assumed to be Health Auto
-- Export's authoritative end-of-day figure - verified false (2026-09-17):
-- checked against the full history, every metric, every midnight-timestamped
-- row is a near-zero fraction (0-0.4%) of that day's real total, with the
-- exact same JSON shape as an ordinary sample. There is no evidence Health
-- Auto Export sends a genuine daily aggregate at this export cadence; the
-- midnight row is most likely a day-boundary reset artifact from how
-- HealthKit tracks these cumulative quantity types, not a rollup. sum() is
-- used for cumulative metrics and avg() for rate/point-in-time metrics,
-- excluding the midnight row from both (its value is real but negligible,
-- and this way it can never masquerade as the day's total again).
daily_metric_totals as (

    select
        metric_name,
        metric_date,
        case
            when metric_name in (
                'step_count', 'active_energy', 'apple_exercise_time',
                'apple_stand_time', 'flights_climbed', 'walking_running_distance'
            )
                then sum(case when not is_daily_rollup then value end)
            else avg(case when not is_daily_rollup then value end)
        end as daily_value
    from metrics
    group by metric_name, metric_date

),

daily as (

    select
        metric_date,
        max(case when metric_name = 'step_count' then daily_value end) as total_steps,
        max(case when metric_name = 'active_energy' then daily_value end) as active_energy_kcal,
        max(case when metric_name = 'apple_exercise_time' then daily_value end) as exercise_minutes,
        max(case when metric_name = 'apple_stand_time' then daily_value end) as stand_minutes,
        max(case when metric_name = 'flights_climbed' then daily_value end) as flights_climbed,
        max(case when metric_name = 'walking_running_distance' then daily_value end) as distance_km,
        max(case when metric_name = 'heart_rate' then daily_value end) as avg_heart_rate_bpm,
        max(case when metric_name = 'resting_heart_rate' then daily_value end) as resting_heart_rate_bpm,
        max(case when metric_name = 'heart_rate_variability' then daily_value end) as avg_hrv_ms,
        max(case when metric_name = 'weight_body_mass' then daily_value end) as weight_kg,
        count(distinct metric_name) as metrics_present
    from daily_metric_totals
    group by metric_date

)

select * from daily
order by metric_date
