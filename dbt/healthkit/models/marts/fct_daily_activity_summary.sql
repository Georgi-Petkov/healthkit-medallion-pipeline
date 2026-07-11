-- One row per day, pivoting key activity/vitals metrics into columns for easy dashboarding.
with metrics as (

    select * from {{ ref('stg_healthkit_metrics') }}

),

daily as (

    select
        metric_date,
        sum(case when metric_name = 'step_count' then value end) as total_steps,
        sum(case when metric_name = 'active_energy' then value end) as active_energy_kcal,
        sum(case when metric_name = 'apple_exercise_time' then value end) as exercise_minutes,
        sum(case when metric_name = 'apple_stand_time' then value end) as stand_minutes,
        sum(case when metric_name = 'flights_climbed' then value end) as flights_climbed,
        sum(case when metric_name = 'walking_running_distance' then value end) as distance_km,
        avg(case when metric_name = 'heart_rate' then value end) as avg_heart_rate_bpm,
        avg(case when metric_name = 'resting_heart_rate' then value end) as resting_heart_rate_bpm,
        avg(case when metric_name = 'heart_rate_variability' then value end) as avg_hrv_ms,
        avg(case when metric_name = 'weight_body_mass' then value end) as weight_kg,
        count(distinct metric_name) as metrics_present
    from metrics
    group by metric_date

)

select * from daily
order by metric_date
