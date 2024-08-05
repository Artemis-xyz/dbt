{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_dao_safety_incentives",
    )
}}

with 
    logs as (
        select 
            block_timestamp
            , decoded_log:amount::float / 1E18 as amount_nominal
        from ethereum_flipside.core.ez_decoded_event_logs 
        where contract_address = lower('0x4da27a545c0c5B758a6BA100e3a049001de870f5')
            and event_name = 'RewardsClaimed'
    )
    , prices as ({{get_coingecko_price_with_latest('aave')}})
    , priced_logs as (
        select
            block_timestamp::date as date
            , '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9' as token_address
            , amount_nominal
            , amount_nominal * price as amount_usd
        from logs
        left join price on block_timestamp::date = date
    )
select
    date
    , token_address
    , 'AAVE DAO' as protocol
    , 'ethereum' as chain
    , sum(coalesce(amount_nominal, 0)) as amount_nominal
    , sum(coalesce(amount_usd, 0)) as amount_usd
from priced_logs
group by 1, 2