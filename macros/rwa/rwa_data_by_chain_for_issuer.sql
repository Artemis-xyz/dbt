{% macro rwa_data_by_chain_for_issuer(issuer_id) %}
    select
        date
        , chain
        , symbol
        , issuer_friendly_name
        , avg(price) as price
        , sum(rwa_supply_usd) as tokenized_mcap
        , sum(net_rwa_supply_usd_change) as tokenized_mcap_change
        , sum(rwa_supply_native) as tokenized_supply
        , sum(net_rwa_supply_native_change) as tokenized_supply_change
    from {{ ref( "agg_rwa_by_product_and_chain") }}
    where issuer_id = '{{ issuer_id }}'
    group by 1, 2, 3, 4
{% endmacro %}
