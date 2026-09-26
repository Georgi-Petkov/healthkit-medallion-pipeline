-- One row per HealthKit metric: human-readable name and category (defined
-- here) plus the unit it is reported in (taken from the data). Driven from
-- the observed metrics, not from the definitions list, so a metric that
-- appears in Silver before it is described here shows up with a null
-- display_name/metric_category and trips the not_null tests instead of
-- silently vanishing from the dimension.
with observed as (

    select
        metric_name,
        -- Each metric is reported in a single unit today (verified against
        -- the full history), so max() is just a way to pick that one value.
        max(units) as unit
    from {{ ref('stg_healthkit_metrics') }}
    group by metric_name

),

definitions as (

    select * from values
        ('heart_rate',                       'Heart Rate',                       'cardiovascular'),
        ('resting_heart_rate',               'Resting Heart Rate',               'cardiovascular'),
        ('heart_rate_variability',           'Heart Rate Variability',           'cardiovascular'),
        ('cardio_recovery',                  'Cardio Recovery',                  'cardiovascular'),
        ('vo2_max',                          'VO2 Max',                          'cardiovascular'),
        ('blood_pressure',                   'Blood Pressure',                   'cardiovascular'),
        ('step_count',                       'Steps',                            'activity'),
        ('active_energy',                    'Active Energy',                    'activity'),
        ('apple_exercise_time',              'Exercise Time',                    'activity'),
        ('apple_stand_time',                 'Stand Time',                       'activity'),
        ('flights_climbed',                  'Flights Climbed',                  'activity'),
        ('walking_running_distance',         'Walking + Running Distance',       'activity'),
        ('walking_speed',                    'Walking Speed',                    'mobility'),
        ('running_speed',                    'Running Speed',                    'mobility'),
        ('running_power',                    'Running Power',                    'mobility'),
        ('six_minute_walking_test_distance', 'Six-Minute Walk Test Distance',    'mobility'),
        ('sleep_analysis',                   'Sleep Analysis',                   'sleep'),
        ('weight_body_mass',                 'Body Weight',                      'body_composition'),
        ('body_mass_index',                  'Body Mass Index',                  'body_composition'),
        ('body_fat_percentage',              'Body Fat Percentage',              'body_composition'),
        ('lean_body_mass',                   'Lean Body Mass',                   'body_composition'),
        ('waist_circumference',              'Waist Circumference',              'body_composition'),
        ('height',                           'Height',                           'body_composition'),
        ('time_in_daylight',                 'Time in Daylight',                 'environment')
        as t(metric_name, display_name, metric_category)

)

select
    o.metric_name,
    d.display_name,
    d.metric_category,
    o.unit
from observed o
left join definitions d on o.metric_name = d.metric_name
order by o.metric_name
