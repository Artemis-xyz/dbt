{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_v2_swaps'
    )
}}

WITH pools AS (
    SELECT
        pool_address,
        token0_address,
        token1_address,
        tick_spacing
    FROM {{ ref('fact_aerodrome_v2_pools') }}
),
pool_fees AS (
   WITH default_fees AS (
       SELECT
           pool_address,
           CASE 
               WHEN tick_spacing = 1 THEN 0.0001    -- CL1: 1bp
               WHEN tick_spacing = 10 THEN 0.0005   -- CL10: 5bps 
               WHEN tick_spacing = 50 THEN 0.0005   -- CL50: 5bps
               WHEN tick_spacing = 100 THEN 0.0005  -- CL100: 5bps
               WHEN tick_spacing = 200 THEN 0.003   -- CL200: 30bps
               WHEN tick_spacing = 2000 THEN 0.01   -- CL2000: 100bps
               ELSE 0.003                          -- Default
           END as default_fee_rate
       FROM pools
   ),
   custom_fees AS (
       SELECT 
           LOWER(DECODED_LOG:pool::string) as pool, 
           DECODED_LOG:fee::integer / 1e6 as fee_rate,
           block_timestamp,
           ROW_NUMBER() OVER (PARTITION BY DECODED_LOG:pool::string ORDER BY block_timestamp DESC) as rn
       FROM BASE_FLIPSIDE.core.ez_decoded_event_logs
       WHERE CONTRACT_ADDRESS = LOWER('0xf4171b0953b52fa55462e4d76eca1845db69af00')
       AND EVENT_NAME = 'SetCustomFee'
   )
   SELECT 
       df.pool_address,
       CASE
           WHEN cf.fee_rate IS NOT NULL THEN cf.fee_rate  
           ELSE df.default_fee_rate                       
       END as fee_rate,       
       cf.block_timestamp,
       cf.rn
   FROM default_fees df
   LEFT JOIN custom_fees cf 
       ON df.pool_address = cf.pool 
       AND cf.rn = 1
),
swap_events AS (
    SELECT 
        e.BLOCK_TIMESTAMP,
        'Aerodrome' as app,
        'DeFi' as category,
        'Base' as chain,
        '2' as version,
        e.TX_HASH,
        e.ORIGIN_FROM_ADDRESS as sender, 
        DECODED_LOG:recipient::string as recipient,
        e.CONTRACT_ADDRESS as pool_address,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0)) < 0 THEN p.token1_address
            ELSE p.token0_address
        END as token_in_address,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0)) > 0 THEN p.token1_address
            ELSE p.token0_address
        END as token_out_address,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0)) > 0 
            THEN ABS(TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0)))
            ELSE ABS(TRY_CAST(DECODED_LOG:amount1::string AS DECIMAL(38,0)))
        END as amount_in_native,
        CASE 
            WHEN TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0)) < 0 
            THEN ABS(TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0)))
            ELSE ABS(TRY_CAST(DECODED_LOG:amount1::string AS DECIMAL(38,0)))
        END as amount_out_native,
        COALESCE(f.fee_rate, 0.003) as swap_fee_pct
    FROM BASE_FLIPSIDE.core.ez_decoded_event_logs e
    INNER JOIN pools p
        ON e.CONTRACT_ADDRESS = p.pool_address
    LEFT JOIN pool_fees f 
        ON p.pool_address = f.pool_address 
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
    TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price AS   amount_in_usd,
    pd.amount_out_native / POW(10, COALESCE(pd.token_out_decimals, 18)) AS amount_out_native,
    TRY_CAST(pd.amount_out_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_out_decimals, 18)) * pd.token_out_price as amount_out_usd,
    pd.swap_fee_pct,
    (TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct) as fee_usd,
    (TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE( pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct * 0.1667) as revenue,
    (TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price * pd.swap_fee_pct * 0.8333) as supply_side_revenue_usd
FROM prices_and_decimals pd
WHERE pd.token_in_decimals IS NOT NULL 
AND pd.token_out_decimals IS NOT NULL
AND TRY_CAST(pd.amount_in_native AS DECIMAL(38,0)) / POW(10, COALESCE(pd.token_in_decimals, 18)) * pd.token_in_price < 1e9 -- No swaps above 1B
ORDER BY pd.BLOCK_TIMESTAMP DESC