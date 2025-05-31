{{ config(materialized="table") }}

WITH perp_v2_staking_rewards AS (
    SELECT
        DATE(block_timestamp) AS date,
        'optimism' AS chain,
        SUM(
            TRY_CAST(decoded_log:"amount"::STRING AS FLOAT) / 1e18 * eph.price
        ) AS token_incentives
    FROM optimism_flipside.core.ez_decoded_event_logs AS tt
    JOIN optimism_flipside.price.ez_prices_hourly AS eph
        ON LOWER(eph.symbol) = LOWER('PERP') AND eph.hour = DATE_TRUNC('hour', tt.block_timestamp)
    WHERE LOWER(contract_address) IN ( 
        LOWER('0xe6410Ef478F3BA4A9d983d205226ACC6cC794b07'),
        LOWER('0x5Ebe7ee72ab56f82194e61D95d5A0a32CdF722E1')
    )
    AND event_name = 'Claimed'
    GROUP BY date, chain
),

perp_v1_token_incentives AS (
    SELECT
        DATE(block_timestamp) AS date,
        'ethereum' AS chain,
        SUM(
            TRY_CAST(decoded_log:"_balance"::STRING AS FLOAT) / 1e18 * eph.price
        ) AS token_incentives
    FROM ethereum_flipside.core.ez_decoded_event_logs AS tt
    JOIN ethereum_flipside.price.ez_prices_hourly AS eph
        ON LOWER(eph.symbol) = LOWER('PERP') AND eph.hour = DATE_TRUNC('hour', tt.block_timestamp)
    WHERE LOWER(contract_address) IN (
        LOWER('0x49a4B8431Fc24BE4b22Fb07D1683E2c52bC56088'),
        LOWER('0xc2a9e84D77f4B534F049b593C282c5c91F24808A')
    )
    AND event_name = 'Claimed'
    GROUP BY date, chain
),

pool_party AS (
    SELECT
        DATE(block_timestamp) AS date,
        'optimism' AS chain,
        SUM(
            TRY_CAST(decoded_log:"_balance"::STRING AS FLOAT) / 1e18 * eph.price
        ) AS token_incentives
    FROM optimism_flipside.core.ez_decoded_event_logs AS tt
    JOIN optimism_flipside.price.ez_prices_hourly AS eph
        ON LOWER(eph.symbol) = LOWER('PERP') AND eph.hour = DATE_TRUNC('hour', tt.block_timestamp)
    WHERE LOWER(contract_address) = LOWER('0x3230Cbb08C64d0804BE5b7f4cE43834291490a91')
      AND event_name = 'Claimed'
    GROUP BY date, chain
)

SELECT
    date,
    chain,
    SUM(token_incentives) AS total_token_incentives
FROM (
    SELECT * FROM perp_v2_staking_rewards
    UNION ALL
    SELECT * FROM perp_v1_token_incentives
    UNION ALL
    SELECT * FROM pool_party
) AS combined
GROUP BY date, chain
ORDER BY date