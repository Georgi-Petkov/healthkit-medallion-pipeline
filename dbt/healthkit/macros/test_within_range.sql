{% test within_range(model, column_name, min_value=none, max_value=none) %}

with validation as (
    select {{ column_name }} as value_to_check
    from {{ model }}
    where {{ column_name }} is not null
),

validation_errors as (
    select value_to_check
    from validation
    where
        {%- if min_value is not none %}
        value_to_check < {{ min_value }}
        {%- endif %}
        {%- if min_value is not none and max_value is not none %}
        or
        {%- endif %}
        {%- if max_value is not none %}
        value_to_check > {{ max_value }}
        {%- endif %}
)

select * from validation_errors

{% endtest %}
