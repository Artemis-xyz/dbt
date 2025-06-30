{% macro latest_developer_data_date() -%}
    DATEADD('day', 
        -7 - ((DAYOFWEEKISO(CURRENT_DATE()) % 7) + 7), 
        CURRENT_DATE()
    )
{%- endmacro %}
