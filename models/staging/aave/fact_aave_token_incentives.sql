{{
    config(
        materialized='table',
        snowflake_warehouse='Analytics_XL',
    )
}}


WITH all_rewards AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(decoded_log:"amount"::FLOAT) / 1e18 AS reward_amount,
        'stkAAVE' AS module_type
    FROM ethereum_flipside.core.ez_decoded_event_logs 
    WHERE contract_address = lower('0x4da27a545c0c5B758a6BA100e3a049001de870f5') 
    AND EVENT_NAME = 'RewardsClaimed'
    GROUP BY day

    UNION ALL

    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(decoded_log:"amount"::FLOAT) / 1e18 AS reward_amount,
        'stkAAVEwstETHBPTv2' AS module_type
    FROM ethereum_flipside.core.ez_decoded_event_logs 
    WHERE contract_address = lower('0x9eDA81C21C273a82BE9Bbc19B6A6182212068101') 
    AND EVENT_NAME = 'RewardsClaimed'
    GROUP BY day

    UNION ALL

    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(decoded_log:"amount"::FLOAT) / 1e18 AS reward_amount,
        'stkGHO' AS module_type
    FROM ethereum_flipside.core.ez_decoded_event_logs 
    WHERE contract_address = lower('0x1a88Df1cFe15Af22B3c4c783D4e6F7F9e0C1885d') 
    AND EVENT_NAME = 'RewardsClaimed'
    GROUP BY day

    UNION ALL

    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(decoded_log:"amount"::FLOAT) / 1e18 AS reward_amount,
        'stkABPT' AS module_type
    FROM ethereum_flipside.core.ez_decoded_event_logs 
    WHERE contract_address = lower('0xa1116930326D21fB917d5A27F1E9943A9595fb47') 
    AND EVENT_NAME = 'RewardsClaimed'
    GROUP BY day

    UNION ALL

    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(decoded_log:"amount"::FLOAT) / 1e18 AS reward_amount,
        'ETH_rewards' AS module_type
    FROM ethereum_flipside.core.ez_decoded_event_logs 
    WHERE contract_address = lower('0xd784927Ff2f95ba542BfC824c8a8a98F3495f6b5') 
    AND EVENT_NAME = 'RewardsClaimed'
    GROUP BY day
),
aave_prices AS (
    SELECT
        DATE_TRUNC('day', hour) AS day,
        AVG(price) AS aave_price
    FROM ethereum_flipside.price.ez_prices_hourly
    WHERE token_address = LOWER('0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9')
    GROUP BY day
),
final AS (
    SELECT
        r.day,
        SUM(r.reward_amount) AS total_reward_amount,
        AVG(p.aave_price) AS aave_price,
        SUM(r.reward_amount * p.aave_price) AS total_usd_value
    FROM all_rewards r
    JOIN aave_prices p ON r.day = p.day
    GROUP BY r.day
)
SELECT * FROM final
ORDER BY day