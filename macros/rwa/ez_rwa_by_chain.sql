{% macro ez_rwa_by_chain(issuer_id) %}

select
    date
    , chain
    , issuer_friendly_name
    , avg(price) as price
    , sum(rwa_supply_usd) as tokenized_mcap
    , sum(net_rwa_supply_usd_change) as tokenized_mcap_change
    , sum(rwa_supply_native) as tokenized_supply
    , sum(net_rwa_supply_native_change) as tokenized_supply_change

    -- Standardized Metrics
    , sum(rwa_supply_usd) as tokenized_market_cap
    , sum(net_rwa_supply_usd_change) as tokenized_market_cap_net_change
    , sum(rwa_supply_native) as tokenized_market_cap_native
    , sum(net_rwa_supply_native_change) as tokenized_market_cap_native_net_change
from {{ ref( "agg_rwa_by_product_and_chain") }}
where issuer_id = '{{ issuer_id }}'
group by 1, 2, 3

{% endmacro %}
