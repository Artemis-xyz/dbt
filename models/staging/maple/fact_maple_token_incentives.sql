{{
    config(
        materialized='table',
        snowflake_warehouse='MAPLE'
    )
}}

with rewards_contracts as (
    SELECT
        decoded_log:mplRewards as rewards_contract_address
    FROM
        {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }}
    where
        event_name = 'MplRewardsCreated'
)
, mpl_prices as (
    SELECT
        hour,
        price,
        token_address,
        symbol
    FROM
        {{ source('ETHEREUM_FLIPSIDE', 'ez_prices_hourly') }}
    WHERE token_address = lower('0x33349b282065b0284d756f0577fb39c158f935e6')
)
SELECT
    block_timestamp,
    symbol as token,
    decoded_log:reward as incentive_native,
    decoded_log:reward * p.price as incentive_usd
FROM
    {{ source('ETHEREUM_FLIPSIDE', 'ez_decoded_event_logs') }} l
LEFT JOIN mpl_prices p ON p.hour = DATE_TRUNC('hour', l.block_timestamp)
WHERE contract_address in (SELECT rewards_contract_address FROM rewards_contracts)
AND event_name = 'RewardPaid'