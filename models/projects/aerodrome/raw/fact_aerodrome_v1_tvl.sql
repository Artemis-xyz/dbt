{{
    config(
        materialized='table',
        snowflake_warehouse='ANALYTICS_XL',
        database='AERODROME',
        schema='raw',
        alias='fact_v1_tvl'
    )
}}

WITH pools AS (
    SELECT 
        DECODED_LOG:pool::string AS pool,
        DECODED_LOG:token0::string AS token0,
        DECODED_LOG:token1::string AS token1
    FROM BASE_FLIPSIDE.core.ez_decoded_event_logs 
    WHERE LOWER(contract_address) = '0x420dd381b31aef6683db6b902084cb0ffece40da'
      AND event_name = 'PoolCreated'
),

daily_reserves AS (
    SELECT 
        block_timestamp::date as date,
        CONTRACT_ADDRESS as pool_address,
        TRY_TO_NUMBER(DECODED_LOG:reserve0::string) as token0_reserve,
        TRY_TO_NUMBER(DECODED_LOG:reserve1::string) as token1_reserve,
        ROW_NUMBER() OVER (
            PARTITION BY CONTRACT_ADDRESS, DATE_TRUNC('day', block_timestamp)
            ORDER BY block_timestamp DESC
        ) as rn
    FROM BASE_FLIPSIDE.core.ez_decoded_event_logs
    WHERE CONTRACT_ADDRESS IN (SELECT pool FROM pools)
    AND EVENT_NAME = 'Sync'
)
,all_data AS (
    SELECT 
        r.date,
        'base' as chain,
        'v1' as version,
        p.pool as pool_address,
        p.token0,
        t0.symbol as token0_symbol,
        p.token1,
        t1.symbol as token1_symbol,
        r.token0_reserve / POW(10, COALESCE(t0.DECIMALS, 18)) as token0_balance,
        r.token1_reserve / POW(10, COALESCE(t1.DECIMALS, 18)) as token1_balance,
        (token0_balance * COALESCE(t0.price, 0)) as token0_usd,
        (token1_balance * COALESCE(t1.price, 0)) as token1_usd,
        (r.token0_reserve / POW(10, COALESCE(t0.DECIMALS, 18))) * COALESCE(t0.price, 0) +
        (r.token1_reserve / POW(10, COALESCE(t1.DECIMALS, 18))) * COALESCE(t1.price, 0) as tvl_usd
    FROM pools p
    LEFT JOIN daily_reserves r 
        ON p.pool = r.pool_address
    LEFT JOIN base_flipside.price.ez_prices_hourly t0 ON r.date = t0.hour AND p.token0 = t0.token_address
    LEFT JOIN base_flipside.price.ez_prices_hourly t1 ON r.date = t1.hour AND p.token1 = t1.token_address
    WHERE r.rn = 1
    AND ((r.token0_reserve / POW(10, COALESCE(t0.DECIMALS, 18))) * COALESCE(t0.price, 0)) +
        ((r.token1_reserve / POW(10, COALESCE(t1.DECIMALS, 18))) * COALESCE(t1.price, 0)) < 1e11
)

SELECT
    date,
    chain,
    version,
    pool_address,
    token0 as token_address,
    token0_symbol as token_symbol,
    token0_balance as token_balance,
    token0_usd as token_balance_usd
FROM all_data
UNION ALL
SELECT
    date,
    chain,
    version,
    pool_address,
    token1 as token_address,
    token1_symbol as token_symbol,
    token1_balance as token_balance,
    token1_usd as token_balance_usd
FROM all_data