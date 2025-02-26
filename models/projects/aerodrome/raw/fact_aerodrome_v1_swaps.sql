{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_v1_swaps'
    )
}}

WITH aerodrome_pools AS (
    SELECT 
        DECODED_LOG:pool::string as pool_address,
        DECODED_LOG:token0::string as token0_address,
        DECODED_LOG:token1::string as token1_address,
        DECODED_LOG:stable::boolean as is_stable_pool
    FROM BASE_FLIPSIDE.core.ez_decoded_event_logs 
    WHERE CONTRACT_ADDRESS ILIKE '0x420DD381b31aEf6683db6B902084cB0FFECe40Da'
    AND EVENT_NAME ILIKE 'PoolCreated'
),
pool_fees AS (
   WITH custom_fees AS (
       SELECT 
           DECODED_LOG:pool::string AS pool,
           DECODED_LOG:fee::integer / 100000 AS fee_rate,
           block_timestamp,
           ROW_NUMBER() OVER (PARTITION BY DECODED_LOG:pool::string ORDER BY block_timestamp DESC) as rn
       FROM BASE_FLIPSIDE.core.ez_decoded_event_logs
       WHERE LOWER(contract_address) = '0x420dd381b31aef6683db6b902084cb0ffece40da'
       AND event_name = 'SetCustomFee'
   )
   SELECT 
       p.pool_address as pool,
       p.is_stable_pool,
       COALESCE(
           cf.fee_rate,
           CASE 
               WHEN p.is_stable_pool THEN 0.0005  
               ELSE 0.003                 
           END
       ) as fee_rate,
       cf.block_timestamp,
       cf.rn
   FROM aerodrome_pools p
   LEFT JOIN custom_fees cf 
       ON p.pool_address = cf.pool 
       AND (cf.rn = 1 OR cf.rn IS NULL)
),
swap_events AS (
    SELECT 
        e.BLOCK_TIMESTAMP,
        'Aerodrome' as app,
        'DeFi' as category,
        'Base' as chain,
        '1' as version,
        e.TX_HASH,
        e.ORIGIN_FROM_ADDRESS as sender, 
        DECODED_LOG:to::string as recipient,
        e.CONTRACT_ADDRESS as pool_address,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0In::string AS DECIMAL(38,0)) > 0 THEN p.token0_address
            ELSE p.token1_address
        END as token_in_address,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0Out::string AS DECIMAL(38,0)) > 0 THEN p.token0_address
            ELSE p.token1_address
        END as token_out_address,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0In::string AS DECIMAL(38,0)) > 0 THEN DECODED_LOG:amount0In::string
            ELSE DECODED_LOG:amount1In::string
        END as amount_in_native,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0Out::string AS DECIMAL(38,0)) > 0 THEN DECODED_LOG:amount0Out::string
            ELSE DECODED_LOG:amount1Out::string
        END as amount_out_native,
        f.fee_rate as swap_fee_pct,
        f.is_stable_pool
    FROM BASE_FLIPSIDE.core.ez_decoded_event_logs e
    INNER JOIN aerodrome_pools p
        ON e.CONTRACT_ADDRESS = p.pool_address
    LEFT JOIN pool_fees f 
        ON p.pool_address = f.pool 
    WHERE e.EVENT_NAME ILIKE 'Swap'
),
prices_and_decimals AS (
    SELECT 
        se.*,
        tin.DECIMALS as token_in_decimals,
        tin.PRICE as token_in_price,
        tout.DECIMALS as token_out_decimals,
        tout.PRICE as token_out_price
    FROM swap_events se
    LEFT JOIN base_flipside.price.ez_prices_hourly tin 
        ON se.token_in_address = tin.TOKEN_ADDRESS
        AND DATE_TRUNC('hour', se.BLOCK_TIMESTAMP) = tin.HOUR
        AND tin.blockchain = 'base'
    LEFT JOIN base_flipside.price.ez_prices_hourly tout
        ON se.token_out_address = tout.TOKEN_ADDRESS
        AND DATE_TRUNC('hour', se.BLOCK_TIMESTAMP) = tout.HOUR
        AND tout.blockchain = 'base'
)
SELECT 
    pd.BLOCK_TIMESTAMP,
    pd.app,
    pd.category,
    pd.chain,
    pd.version,
    pd.TX_HASH,
    pd.sender,
    pd.recipient,
    pd.pool_address,
    pd.token_in_address,
    pd.token_out_address,
    pd.amount_in_native / POW(10, COALESCE(pd.token_in_decimals, 18)) AS amount_in_native,
    TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price AS amount_in_usd,
    pd.amount_out_native / POW(10, COALESCE(pd.token_out_decimals, 18)) AS amount_out_native,
    TRY_CAST(pd.amount_out_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_out_decimals, 18)) * pd.token_out_price as amount_out_usd,
    pd.swap_fee_pct,
    (TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct) as fee_usd,
    (TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct * 0.1667) as revenue,
    (TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct * 0.8333) as supply_side_revenue_usd
FROM prices_and_decimals pd
WHERE pd.token_in_decimals IS NOT NULL 
AND pd.token_out_decimals IS NOT NULL
AND TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price < 1e9 -- No swaps above 1B
ORDER BY pd.BLOCK_TIMESTAMP DESC