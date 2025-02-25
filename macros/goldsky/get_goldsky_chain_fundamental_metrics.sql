{% macro get_goldsky_chain_fundamental_metrics(chain) %}
    select
        to_timestamp(block_timestamp)::date as date
        , count (distinct from_address) as daa
        , count(*) as txns
        , sum(gas * gas_price) / 1e18 as fees_native
    from {{ ref("fact_" ~ chain ~ "_transactions") }}
    group by to_timestamp(block_timestamp)::date
{% endmacro %}
