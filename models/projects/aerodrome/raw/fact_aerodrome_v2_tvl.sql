{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_v2_tvl'
    )
}}

WITH pools AS (
   SELECT
       DECODED_LOG:pool::string as pool,
       DECODED_LOG:token0::string as token0,
       DECODED_LOG:token1::string as token1
   FROM BASE_FLIPSIDE.core.ez_decoded_event_logs
   WHERE CONTRACT_ADDRESS = LOWER('0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A')
   AND EVENT_NAME = 'PoolCreated'
),
mint_and_burn_liquidity AS (
   SELECT
       TRUNC(block_timestamp, 'day') as date,
       contract_address as pool,
       event_name,
       CASE 
           WHEN event_name = 'Mint' THEN TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0))
           WHEN event_name = 'Burn' THEN -TRY_CAST(DECODED_LOG:amount0::string AS DECIMAL(38,0))
       END as token0_amount,
       CASE 
           WHEN event_name = 'Mint' THEN TRY_CAST(DECODED_LOG:amount1::string AS DECIMAL(38,0))
           WHEN event_name = 'Burn' THEN -TRY_CAST(DECODED_LOG:amount1::string AS DECIMAL(38,0))
       END as token1_amount
   FROM BASE_FLIPSIDE.core.ez_decoded_event_logs
   WHERE contract_address IN (SELECT pool FROM pools)
   AND event_name IN ('Mint', 'Burn')
),
adjusted_mint_and_burn_liquidity AS (
   SELECT
       t1.date,
       t1.pool,
       t1.event_name,
       p.token0,
       p.token1,
       t1.token0_amount / POWER(10, COALESCE(t2.decimals, 18)) as token0_amount_adj,
       t1.token1_amount / POWER(10, COALESCE(t3.decimals, 18)) as token1_amount_adj
   FROM mint_and_burn_liquidity t1
   JOIN pools p ON t1.pool = p.pool
   LEFT JOIN BASE_FLIPSIDE.core.dim_contracts t2 ON p.token0 = t2.address
   LEFT JOIN BASE_FLIPSIDE.core.dim_contracts t3 ON p.token1 = t3.address
   WHERE t2.decimals != 0 AND t3.decimals != 0
),
token_changes_per_pool_per_day AS (
   SELECT
       date,
       pool,
       token0,
       SUM(token0_amount_adj) as token0_amount_per_day,
       token1,
       SUM(token1_amount_adj) as token1_amount_per_day
   FROM adjusted_mint_and_burn_liquidity
   GROUP BY date, pool, token0, token1
),
token_cumulative_per_day AS (
   SELECT
       date,
       pool,
       token0,
       token0_amount_per_day,
       SUM(token0_amount_per_day) OVER (
           PARTITION BY pool ORDER BY date
       ) as token0_cumulative,
       token1,
       token1_amount_per_day,
       SUM(token1_amount_per_day) OVER (
           PARTITION BY pool ORDER BY date
       ) as token1_cumulative
   FROM token_changes_per_pool_per_day
),
average_token_price_per_day AS (
   SELECT 
       TRUNC(hour, 'day') as date,
       token_address,
       AVG(price) as price
   FROM base_flipside.price.ez_prices_hourly
   WHERE blockchain = 'base'
   GROUP BY date, token_address
)
, all_data AS (
SELECT
        t1.date,
        pool,
        token0,
        COALESCE(t2.price, 0) as token0_price,
        token0_amount_per_day,
        token0_cumulative,
        COALESCE(token0_amount_per_day * token0_price, 0) as token0_daily_usd,
        COALESCE(token0_cumulative * token0_price, 0) as token0_cumulative_usd,
        token1,
        COALESCE(t3.price, 0) as token1_price,
        token1_amount_per_day,
        token1_cumulative,
        COALESCE(token1_amount_per_day * token1_price, 0) as token1_daily_usd,
        COALESCE(token1_cumulative * token1_price, 0) as token1_cumulative_usd
    FROM token_cumulative_per_day t1
    LEFT JOIN average_token_price_per_day t2 
        ON t1.date = t2.date AND LOWER(t1.token0) = LOWER(t2.token_address)
    LEFT JOIN average_token_price_per_day t3 
        ON t1.date = t3.date AND LOWER(t1.token1) = LOWER(t3.token_address)
    HAVING ABS(
            LN(ABS(COALESCE(NULLIF(token0_cumulative_usd, 0), 1))) / LN(10)
            - LN(ABS(COALESCE(NULLIF(token1_cumulative_usd, 0), 1))) / LN(10)
        ) < 1
)

SELECT
    date,
    chain,
    version,
    pool_address,
    token0 as token_address,
    token0_cumulative as token_balance,
    token0_cumulative_usd as token_balance_usd
FROM all_data
UNION ALL
SELECT
    date,
    chain,
    version,
    pool_address,
    token1 as token_address,
    token1_cumulative as token_balance,
    token1_cumulative_usd as token_balance_usd
FROM all_data