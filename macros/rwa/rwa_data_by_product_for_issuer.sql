{% macro rwa_data_by_product_for_issuer(issuer) %}
    select
        date
        , '{{ issuer }}' as issuer
        , chain
        , symbol
        , sum(rwa_supply_usd) as rwa_supply_usd
        , sum(net_rwa_supply_usd_change) as net_rwa_supply_usd_change
        , sum(rwa_supply_native) as rwa_supply_native
        , sum(net_rwa_supply_native_change) as net_rwa_supply_native_change
    from {{ ref( "agg_rwa_by_product_and_chain") }}
    where issuer = '{{ issuer }}'
    group by 1, 2, 3, 4
{% endmacro %}
