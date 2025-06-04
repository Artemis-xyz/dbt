{{ config(
    materialized= "table",
) }}

WITH trading_rewards AS (
    SELECT
        DATE(tt.block_timestamp) AS date,
        TX_HASH,
        TRY_CAST(tt.decoded_log:"amount"::STRING AS FLOAT) / 1e18 as amount,
        TRY_CAST(tt.decoded_log:"amount"::STRING AS FLOAT) / 1e18 * eph.price AS token_incentives
    FROM optimism_flipside.core.ez_decoded_event_logs AS tt
    JOIN optimism_flipside.price.ez_prices_hourly AS eph
        ON eph.token_address = LOWER('0x920Cf626a271321C151D027030D5d08aF699456b')
        AND eph.hour = DATE_TRUNC('hour', tt.block_timestamp)
    WHERE LOWER(tt.contract_address) IN (
        LOWER('0xf486A72E8c8143ACd9F65A104A16990fDb38be14'),
        LOWER('0x2787cc20e5ecb4bf1bfb79eae284201027683179')
    )
      AND tt.event_name = 'Claimed'
      AND MOD(TRY_CAST(tt.decoded_log:"amount"::STRING AS FLOAT) / 1e18, 1) != 0
)

SELECT * FROM trading_rewards