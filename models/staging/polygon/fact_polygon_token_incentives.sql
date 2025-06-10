{{
    config(
        materialized="table",
    )
}}


WITH polygon_block_rewards AS (
    SELECT
        DATE(tt.block_timestamp) AS date,
        SUM(TRY_CAST(tt.decoded_log:"reward"::STRING AS FLOAT) / 1e18 * eph.price) AS token_incentives
    FROM ethereum_flipside.core.ez_decoded_event_logs AS tt
    JOIN ethereum_flipside.price.ez_prices_hourly AS eph
        ON eph.token_address = LOWER('0x7D1AfA7B718fb893dB30A3aBc0Cfc608AaCfeBB0')
        AND eph.hour = DATE_TRUNC('hour', tt.block_timestamp)
    WHERE LOWER(tt.contract_address) = LOWER('0x86e4dc95c7fbdbf52e33d563bbdb00823894c287')
      AND tt.event_name = 'NewHeaderBlock'
      AND MOD(TRY_CAST(tt.decoded_log:"reward"::STRING AS FLOAT) / 1e18, 1) != 0
      GROUP BY date
)

SELECT * FROM polygon_block_rewards