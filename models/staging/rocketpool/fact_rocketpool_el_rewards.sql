{{ config(
    materialized="table",
    snowflake_warehouse="ROCKETPOOL"
    )
}}

with dim_fee_distributors as (
    SELECT
        distinct(DECODED_LOG:_address::STRING) as fee_distributor_address
    FROM ethereum_flipside.core.ez_decoded_event_logs
    WHERE event_name = 'ProxyCreated'
    AND contract_address = lower('0xe228017f77B3E0785e794e4c0a8A6b935bB4037C') -- NodeDistributor Factory
)
, prices AS (
    SELECT
        hour,
        price
    FROM
        ethereum_flipside.price.ez_prices_hourly
    WHERE
        is_native = True
)
SELECT
    l.block_timestamp::date as date
    , SUM(DECODED_LOG:_userAmount::NUMBER / 1e18) as el_fees_to_users_eth
    , SUM(DECODED_LOG:_userAmount::NUMBER / 1e18 * p.price) as el_fees_to_users_usd
    , SUM(DECODED_LOG:_nodeAmount::NUMBER / 1e18) as el_fees_to_nodes_eth
    , SUM(DECODED_LOG:_nodeAmount::NUMBER / 1e18 * p.price) as el_fees_to_nodes_usd
FROM ethereum_flipside.core.ez_decoded_event_logs l
LEFT JOIN prices p on p.hour = date_trunc('hour', l.block_timestamp)
WHERE contract_address in (SELECT fee_distributor_address FROM dim_fee_distributors)
AND event_name = 'FeesDistributed'
GROUP BY 1