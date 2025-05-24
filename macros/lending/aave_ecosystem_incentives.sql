{% macro aave_v3_ecosystem_incentives(chain, contract_address, protocol) %}
with
event_logs as(
    select 
        block_timestamp
        , decoded_log:amount::float as amount
        , decoded_log:reward::string as asset
    from {{chain}}_flipside.core.ez_decoded_event_logs
    where contract_address = lower('{{contract_address}}')
        and event_name = 'RewardsClaimed'
)
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset
        , amount / pow(10, decimals) as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join {{chain}}_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour
        and lower(asset) = lower(token_address)
)
select
    date
    , '{{chain}}' as chain
    , '{{protocol}}' as protocol
    , asset as token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4
{% endmacro %}

{% macro aave_v2_ecosystem_incentives(chain, contract_address, protocol) %}
with
event_logs as (
    select 
        block_timestamp
        , case 
            when '{{chain}}' = 'ethereum' then '0x4da27a545c0c5B758a6BA100e3a049001de870f5' 
            when '{{chain}}' = 'avalanche' then '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
            else '0x63a72806098Bd3D9520cC43356dD78afe5D386D9'
        end as asset
        , decoded_log:amount::float as amount
    from ethereum_flipside.core.ez_decoded_event_logs
    where contract_address = lower('{{contract_address}}')
        and event_name = 'RewardsClaimed'
)
, prices as ({{get_coingecko_price_with_latest('aave')}})
, event_logs_priced as (
    select 
        block_timestamp::date as date
        , asset as token_address
        , amount
        , amount / 1E18 as amount_nominal
        , amount_nominal * price as amount_usd
    from  event_logs
    left join prices on block_timestamp::date = date
)
select
    date
    , '{{chain}}' as chain
    , '{{protocol}}' as protocol
    , token_address
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from event_logs_priced
group by 1, 4
{% endmacro %}