{% macro chainlink_direct_operator_rewards_daily(chain) %}
with 
prices as ({{get_coingecko_price_with_latest("chainlink")}})
select
    date(block_timestamp) as date
    , '{{chain}}' as chain
    , sum(decoded_log:"payment"::number / 1e18) as token_amount
    , sum(decoded_log:"payment"::number / 1e18 * p.price) as usd_amount
from {{ chain }}_flipside.core.ez_decoded_event_logs e
left join prices p on p.date = date(block_timestamp)
where topics[0]::string = '0xd8d7ecc4800d25fa53ce0372f13a416d98907a7ef3d8d3bdd79cf4fe75529c65'
    and block_timestamp > date('2023-08-01')
group by 1
{% endmacro %}