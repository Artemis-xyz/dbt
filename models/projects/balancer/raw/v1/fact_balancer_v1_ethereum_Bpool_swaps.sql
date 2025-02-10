{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='raw',
    )
}}

SELECT
    t1.event_name,
    t1.origin_from_address,
    t1.origin_to_address,
    t2.pool as pool,
    t1.tx_hash,
    t1.event_index,
    t1.block_number,
    t1.decoded_log:caller::STRING AS caller,              -- Extracts caller from JSON
    TRY_CAST(t1.decoded_log:tokenAmountIn::STRING AS NUMBER) AS tokenAmountIn, -- Gracefully handle invalid values
    TRY_CAST(t1.decoded_log:tokenAmountOut::STRING AS NUMBER) AS tokenAmountOut,
   -- t1.decoded_log:tokenAmountIn::NUMBER AS tokenAmountIn, -- bug, number larger than 38 decimals
   -- t1.decoded_log:tokenAmountOut::NUMBER AS tokenAmountOut, 
    t1.decoded_log:tokenIn::STRING AS tokenIn,            -- Extracts tokenIn from JSON
    t1.decoded_log:tokenOut::STRING AS tokenOut,           -- Extracts tokenOut from JSON
    t1.block_timestamp,
    trunc(t1.block_timestamp, 'hour') as hour,
FROM {{ source("ETHEREUM_FLIPSIDE", "ez_decoded_event_logs")}} t1
INNER JOIN {{ ref('fact_balancer_v1_ethereum_Bpools') }} t2 ON lower(t1.contract_address) = lower(t2.pool)
WHERE t1.event_name = 'LOG_SWAP'
