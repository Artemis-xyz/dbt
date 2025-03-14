{% macro get_goldsky_chain_fundamental_metrics(chain) %}
    {% if chain == "bob" %}
        select
            to_timestamp(block_timestamp)::date as date
            , count (distinct from_address) as daa
            , count(*) as txns
            , sum((receipt_gas_used * effective_gas_price) + l1_fee) / 1e18 as fees_native
        from {{ ref("fact_" ~ chain ~ "_transactions") }}
        where gas > 0
        group by to_timestamp(block_timestamp)::date
    {% else %}
        select
            to_timestamp(block_timestamp)::date as date
            , count (distinct from_address) as daa
            , count(*) as txns
            , sum((receipt_effective_gas_price * receipt_gas_used) + receipt_l1_fee) / 1e18 as fees_native
        from {{ ref("fact_" ~ chain ~ "_transactions") }}
        where gas > 0
        group by to_timestamp(block_timestamp)::date
    {% endif %}
{% endmacro %}
