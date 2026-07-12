{% macro color_change(column_name, decimals=2) %}
    concat(
        round({{ column_name }}, {{ decimals }}),
        ' ',
        case
            when {{ column_name }} < 0 then '<span style="color:red;">&#x25BC;</span>'
            when {{ column_name }} > 0 then '<span style="color:green;">&#x25B2;</span>'
            when {{ column_name }} = 0 then '<span style="color:orange;">&#x2014;</span>'
            else ''
        end
    )
{% endmacro %}
