{{
    config(
        materialized="table",
        snowflake_warehouse="UNISWAP_SM",
        database="uniswap",
        schema="raw",
        alias="fact_uniswap_token_incentives",
    )
}}


with prices as (
    SELECT date(hour) as date, AVG(price) as price
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE lower(token_address) = lower('0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984')
    GROUP BY 1
)

, logs as (
    SELECT
        date(l.block_timestamp) as date,
        'UNI' as token,
        sum(l.decoded_log:reward::number / 1e18) as reward_native
    FROM
        ethereum_flipside.core.fact_decoded_event_logs l
    WHERE
        contract_address in (
            lower('0xca35e32e7926b96a9988f61d510e038108d8068e')
            , lower('0xa1484c3aa22a66c62b77e0ae78e15258bd0cb711')
            , lower('0x7fba4b8dc5e7616e59622806932dbea72537a56b')
            , lower('0x6c3e4cb2e96b01f4b866965a91ed4437839a121a')
        )
        and event_name = 'RewardPaid'
    GROUP BY
        1,
        2
)
SELECT
    p.date
    , token
    , coalesce(l.reward_native, 0) as token_incentives_native
    , coalesce(reward_native * p.price, 0) as token_incentives_usd
FROM prices p
left join logs l on p.date = l.date