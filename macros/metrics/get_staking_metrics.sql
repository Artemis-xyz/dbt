{% macro get_staking_metrics(chain) %}
    select date, chain, total_staked_native, total_staked_usd
    from {{ ref("fact_" ~ chain ~ "_amount_staked_silver") }}
{% endmacro %}
