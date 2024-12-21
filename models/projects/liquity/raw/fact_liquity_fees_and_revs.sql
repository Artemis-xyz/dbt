{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_fees_and_revs'
    )
}}

WITH lusd AS (
    select
        block_timestamp::date as date,
        'LUSD' as token,
        decoded_log:_LUSDFee / 1e18 as revenue_native,
        decoded_log:_LUSDFee / 1e18 as revenue_usd
    from
        ethereum_flipside.core.ez_decoded_event_logs
    where
        1 = 1
        AND contract_address = lower('0x24179cd81c9e782a4096035f7ec97fb8b783e007')
        AND event_name = 'LUSDBorrowingFeePaid'
),
eth AS (
    select
        e.block_timestamp::date as date,
        'ETH' as token,
        e.decoded_log:_ETHFee / 1e18 as revenue_native,
        e.decoded_log:_ETHFee / 1e18 * p.price as revenue_usd
    from
        ethereum_flipside.core.ez_decoded_event_logs e
        left join ethereum_flipside.price.ez_prices_hourly p on p.hour = e.block_timestamp::date and is_native
    where
        1 = 1
        AND contract_address = lower('0xa39739ef8b0231dbfa0dcda07d7e29faabcf4bb2')
        AND event_name = 'Redemption'
)
SELECT
    date,
    'ethereum' as chain,
    token,
    sum(revenue_native) as revenue_native,
    sum(revenue_usd) as revenue_usd
FROM
    lusd 
FULL JOIN eth using(date, token)
GROUP BY
    1,
    2,
    3