-- is_daily_rollup rows are never a genuine daily total in this data - verified
-- empirically (2026-09-17) across the full history: every metric, every
-- midnight-timestamped row is a near-zero fraction (0-0.4%) of that day's
-- real intraday sum, with the exact same JSON shape as an ordinary sample.
-- See fct_daily_activity_summary.sql for the full explanation. This is the
-- inverse of the invariant this test used to check (assert_daily_totals_
-- match_rollup_when_present, which encoded the since-disproven assumption
-- that rollups were authoritative and got fixed 3e672c2 by preferring them).
--
-- This guards the corrected behavior going forward: whenever a rollup row
-- and intraday rows both exist for a (metric_name, metric_date), the mart's
-- value must match the intraday sum, never the rollup's raw value - so a
-- regression back to trusting the rollup shows up here immediately.

with rollups as (

    select metric_name, metric_date, value as rollup_value
    from {{ ref('stg_healthkit_metrics') }}
    where is_daily_rollup

),

intraday_totals as (

    select metric_name, metric_date, sum(value) as intraday_sum
    from {{ ref('stg_healthkit_metrics') }}
    where not is_daily_rollup
      and metric_name in (
          'step_count', 'active_energy', 'apple_exercise_time',
          'apple_stand_time', 'flights_climbed', 'walking_running_distance'
      )
    group by metric_name, metric_date

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

select r.metric_name, r.metric_date, r.rollup_value, i.intraday_sum, u.mart_value
from rollups r
join intraday_totals i using (metric_name, metric_date)
join unpivoted u using (metric_name, metric_date)
where abs(u.mart_value - r.rollup_value) < 0.01   -- the mart matched the rollup...
  and abs(u.mart_value - i.intraday_sum) > 0.01    -- ...instead of the real intraday sum
