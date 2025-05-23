{{ config(materialized="table", snowflake_warehouse="SYNTHETIX") }}

WITH all_rewards AS (
    -- Optimism V2
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        SUM(decoded_log:"value"::FLOAT) / 1e18 AS total_reward,
        'optimism' AS chain
    FROM optimism_flipside.core.ez_decoded_event_logs 
    WHERE 
        CONTRACT_ADDRESS = LOWER('0x47eE58801C1AC44e54FF2651aE50525c5cfc66d0') 
        AND EVENT_NAME IN ('Vested', 'VestingEntryCreated')
    GROUP BY 1

    UNION ALL

    -- Ethereum V2
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        SUM(decoded_log:"value"::FLOAT) / 1e18 AS total_reward,
        'ethereum_v2' AS chain
    FROM ethereum_flipside.core.ez_decoded_event_logs 
    WHERE 
        CONTRACT_ADDRESS IN (
                            LOWER('0xDA4eF8520b1A57D7d63f1E249606D1A459698876'),
                            LOWER('0xb671F2210B1F6621A2607EA63E6B2DC3e2464d1F'),
                            LOWER('0xFAd53Cc9480634563E8ec71E8e693Ffd07981d38'),
                            LOWER('0xAc86855865CbF31c8f9FBB68C749AD5Bd72802e3'))
        AND EVENT_NAME = 'VestingEntryCreated'
        AND tx_hash NOT IN (
                '0x9091f96f711593c065db08f5c4284860298f0fb6d9a8d54efd066caea60ff940',
                '0xd1a24fc0cc67a96c8e8935a5b4659d19a1dcb537ae34ddd6d45f91a3b00d321c',
                '0x1365c91fdc281f2291c000f46b15e412c674e7563c946a364ae874ed88a52402',
                '0xe438bef36211266c79f2d15eb31ea53425ea2e6df85a2b2623199059c3712ea8',
                '0xc6d3ce01b82a69a75d5a200bbe63b010591a49156e3bd13764b43d449511cdf9',
                '0x1c20147329400885ccc5d57ceae3c3477644ea14f5672cdd642989ce6972e562',
                '0xc206f82ac5710bc764865d7aef678b1d5cfd496489a14596b6587bd5db151d77',
                '0x99730f1ce685d19548bfd22fb89df7102e2620a27d3ecf7ef67282060621b50e'
            )
        AND block_timestamp NOT BETWEEN TIMESTAMP '2022-05-24 02:45:00' AND TIMESTAMP '2022-05-24 03:15:00'
    GROUP BY 1
    
),
snx_prices AS (
    SELECT
        DATE_TRUNC('day', hour) AS day,
        AVG(price) AS snx_price
    FROM (
        SELECT hour, price, token_address
        FROM optimism_flipside.price.ez_prices_hourly
        WHERE token_address = LOWER('0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4')

        UNION ALL

        SELECT hour, price, token_address
        FROM ethereum_flipside.price.ez_prices_hourly
        WHERE token_address = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f')
    )
    GROUP BY 1
)
SELECT
    r.day,
    r.chain,
    r.total_reward,
    p.snx_price,
    r.total_reward * p.snx_price as usd_value
FROM all_rewards r
LEFT JOIN snx_prices p ON r.day = p.day
ORDER BY r.day, r.chain