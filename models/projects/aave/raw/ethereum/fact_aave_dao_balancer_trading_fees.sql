{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_dao_balancer_trading_fees",
    )
}}

with
swaps as (
    select 
        block_timestamp
        , decoded_log:tokenIn::string as token_address
        , decoded_log:tokenAmountIn::float * 0.001 as amount
    from ethereum_flipside.core.ez_decoded_event_logs 
    where contract_address = lower('0xC697051d1C6296C24aE3bceF39acA743861D9A81') 
        and event_name = 'LOG_SWAP'
)
, swap_revenue as (
    select
        block_timestamp::date as date
        , swaps.token_address
        , coalesce(amount / pow(10, decimals), 0) as amount_nominal
        , coalesce(amount_nominal * price, 0) as amount_usd
    from swaps
    left join ethereum_flipside.price.ez_prices_hourly p
        on date_trunc(hour, block_timestamp) = hour 
        and lower(swaps.token_address) = lower(p.token_address)
)
select
    date
    , token_address
    , 'AAVE DAO' as protocol
    , 'ethereum' as chain
    , sum(coalesce(amount_nominal, 0)) as trading_fees_nominal
    , sum(coalesce(amount_usd, 0)) as trading_fees_usd
from swap_revenue 
where date < to_date(sysdate())
group by 1, 2
order by 1 desc