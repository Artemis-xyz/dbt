{{
    config(
        materialized="table",
        snowflake_warehouse="CHAINLINK",
        database="chainlink",
        schema="raw",
        alias="fact_ethereum_ccip_reward_daily"
    )
}}

with
    token_meta as (
        select
            token_contract
            , token_symbol
        from {{ref('dim_chainlink_ethereum_ccip_token_meta')}}
    )
    , eth_price as ({{ get_coingecko_price_with_latest('ethereum') }})
    , link_price as ({{ get_coingecko_price_with_latest('chainlink') }})
    , token_usd_daily AS (
        select 
            date as date_start
            , 'WETH' as symbol
            , price as usd_amount
        from eth_price

        union all

        select 
            date as date_start
            , 'LINK' as symbol
            , price as usd_amount
        from link_price
    )
    , ccip_reward_daily AS (
        select
            ccip_send_requested_daily.date_start
            , cast(date_trunc('month', ccip_send_requested_daily.date_start) as date) as date_month
            , sum(ccip_send_requested_daily.fee_amount) as token_amount
            , sum((ccip_send_requested_daily.fee_amount * tud.usd_amount)) as usd_amount
            , ccip_send_requested_daily.token as token
        from {{ref('fact_chainlink_ethereum_ccip_send_requested_daily')}} ccip_send_requested_daily
        left join token_usd_daily tud ON tud.date_start = ccip_send_requested_daily.date_start AND tud.symbol = ccip_send_requested_daily.token
        group by 1, 5
    )

select
    'ethereum' as chain,
    date_start,
    date_month,
    token_amount,
    usd_amount,
    token
from ccip_reward_daily
order by 2, 6