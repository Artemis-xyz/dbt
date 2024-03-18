{% macro get_issuance_metrics(chain) %}
    select date, chain, issuance, circulating_supply
    from {{ ref("fact_" ~ chain ~ "_issuance_circulating_supply_silver") }}
{% endmacro %}
