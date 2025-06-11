{{ config(materialized="table") }}


WITH weekly_rewards AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        SUM(decoded_log:"amount"::FLOAT) / 1e18 AS total_reward
    FROM base_flipside.core.ez_decoded_event_logs 
    WHERE 
        CONTRACT_ADDRESS = LOWER('0x16613524e02ad97eDfeF371bC883F2F5d6C480A5') 
        AND EVENT_NAME = 'NotifyReward'
    GROUP BY 1
),
aero_prices AS (
    SELECT
        DATE_TRUNC('day', hour) AS day,
        AVG(price) AS aero_price
    FROM base_flipside.price.ez_prices_hourly
    WHERE token_address = LOWER('0x940181a94a35a4569e4529a3cdfb74e38fd98631')
    GROUP BY 1
)
SELECT
    r.day,
    r.total_reward,
    p.aero_price,
    r.total_reward * p.aero_price AS usd_value
FROM weekly_rewards r
LEFT JOIN aero_prices p ON r.day = p.day
ORDER BY r.day