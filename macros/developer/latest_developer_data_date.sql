{% macro latest_developer_data_date() -%}
    -- Get the date of the most recent Sunday and then subtract 14 days
    DATEADD('day', 
        -14 - ((DAYOFWEEKISO(CURRENT_DATE()) % 7)), 
        CURRENT_DATE()
    )
{%- endmacro %}
