-- For any (metric_name, metric_date) where Health Auto Export sent a daily
-- rollup datapoint, fct_daily_activity_summary's value for that metric/day
-- must exactly equal the rollup's own raw value - never a sum/combination
-- with the intraday breakdown points. This is a direct, magnitude-blind
-- check on the exact invariant broken by the bug fixed in 3e672c2 (rollup +
-- intraday summed together, roughly doubling every daily figure). Because
-- it never judges plausibility, it can never flag a real value no matter
-- how extreme a day was - it only catches the aggregation logic doing the
-- wrong thing.

with rollups as (

    select metric_name, metric_date, value as rollup_value
    from {{ ref('stg_healthkit_metrics') }}
    where is_daily_rollup

),

mart_values as (

    select
        metric_date,
        total_steps, active_energy_kcal, exercise_minutes,
        stand_minutes, flights_climbed, distance_km
    from {{ ref('fct_daily_activity_summary') }}

),

unpivoted as (

    select metric_date, 'step_count' as metric_name, total_steps as mart_value from mart_values
    union all
    select metric_date, 'active_energy', active_energy_kcal from mart_values
    union all
    select metric_date, 'apple_exercise_time', exercise_minutes from mart_values
    union all
    select metric_date, 'apple_stand_time', stand_minutes from mart_values
    union all
    select metric_date, 'flights_climbed', flights_climbed from mart_values
    union all
    select metric_date, 'walking_running_distance', distance_km from mart_values

)

select r.metric_name, r.metric_date, r.rollup_value, u.mart_value
from rollups r
join unpivoted u using (metric_name, metric_date)
where abs(r.rollup_value - u.mart_value) > 0.01
