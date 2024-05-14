{% macro get_rolling_active_address_metrics(chain) %}
    select
        date,
        mau,
        wau
    from {{ref("fact_" ~ chain ~ "_rolling_active_addresses")}}
{% endmacro %}