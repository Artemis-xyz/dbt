{{
    config(
        materialized="table",
        snowflake_warehouse="FRAX",
    )
}}

SELECT 
    block_timestamp,
    transaction_hash,
    decoded_log:rewardAmount::number / 1e18 / 0.9 as yield_generated_native,
    p.price,
    decoded_log:rewardAmount::number / 1e18 / 0.9 * p.price as yield_generated
FROM
    {{ ref("fact_ethereum_decoded_events") }}
LEFT JOIN {{ source("ETHEREUM_FLIPSIDE_PRICE", "ez_prices_hourly") }} p ON p.hour = block_timestamp::date AND p.token_address = lower('0xac3E018457B222d93114458476f3E3416Abbe38F') 
WHERE TRUE
AND contract_address = lower('0xac3E018457B222d93114458476f3E3416Abbe38F')
AND event_name = 'NewRewardsCycle'