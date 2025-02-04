{{
    config(
        materialized = 'table',
        snowflake_warehouse = 'CONVEX',
        database = 'CONVEX',
        schema = 'raw',
        alias = 'fact_convex_lp_token_tvl_by_pool'
    )
}}

WITH deposits AS (
  SELECT 
    block_number,
    DATE_TRUNC('day', block_timestamp) as date,
    decoded_log:poolid::integer as pool_id,
    decoded_log:amount::decimal as amount,
    'deposit' as type
  FROM ethereum_flipside.core.ez_decoded_event_logs
  WHERE contract_address = lower('0xF403C135812408BFbE8713b5A23a04b3D48AAE31')
    AND event_name = 'Deposited'
),

withdrawals AS (
  SELECT 
    block_number,
    DATE_TRUNC('day', block_timestamp) as date,
    decoded_log:poolid::integer as pool_id,
    decoded_log:amount::decimal as amount,
    'withdraw' as type
  FROM ethereum_flipside.core.ez_decoded_event_logs
  WHERE contract_address = lower('0xF403C135812408BFbE8713b5A23a04b3D48AAE31')
    AND event_name = 'Withdrawn'
),

combined_flows AS (
  SELECT 
    block_number,
    date,
    pool_id,
    CASE WHEN type = 'deposit' THEN amount ELSE -amount END as amount
  FROM (
    SELECT * FROM deposits 
    UNION ALL 
    SELECT * FROM withdrawals
  )
),

-- Get daily net changes per pool
daily_pool_changes AS (
  SELECT 
    date,
    pool_id,
    SUM(amount) as net_change
  FROM combined_flows
  GROUP BY 1, 2
),

-- Create date spine with all pool combinations
date_pool_spine AS (
  SELECT
    d.date,
    p.pid as pool_id,
    p.lptoken
  FROM (
    SELECT DISTINCT date FROM pc_dbt_db.prod.dim_date_spine
    WHERE date >= (SELECT MIN(date) FROM combined_flows)
    AND date <= CURRENT_DATE
  ) d
  CROSS JOIN convex.prod_raw.fact_convex_pools p
),

-- Calculate running balances for all dates
pool_balances AS (
  SELECT 
    s.date,
    s.pool_id,
    s.lptoken,
    COALESCE(dpc.net_change, 0) as daily_change,
    SUM(COALESCE(dpc.net_change, 0)) OVER (
      PARTITION BY s.pool_id 
      ORDER BY s.date
      ROWS UNBOUNDED PRECEDING
    ) as balance
  FROM date_pool_spine s
  LEFT JOIN daily_pool_changes dpc ON s.date = dpc.date AND s.pool_id = dpc.pool_id
),

-- Get daily average prices
daily_prices AS (
  SELECT
    DATE_TRUNC('day', hour) as date,
    token_address,
    decimals,
    AVG(price) as avg_price
  FROM ethereum_flipside.price.ez_prices_hourly
  WHERE token_address IN (SELECT DISTINCT LOWER(lptoken) FROM convex.prod_raw.fact_convex_pools)
  GROUP BY 1, 2, 3
)

-- Final TVL calculation
SELECT 
  pb.date,
  pb.pool_id,
  pb.lptoken,
  pb.balance / pow(10, coalesce(decimals, 18)) as token_balance,
  dp.avg_price as token_price,
  pb.balance * COALESCE(dp.avg_price, 0) / pow(10, coalesce(decimals, 18)) as usd_value
FROM pool_balances pb
LEFT JOIN daily_prices dp ON pb.date = dp.date 
  AND LOWER(pb.lptoken) = dp.token_address
WHERE pb.balance > 0 -- Only show pools with balance
ORDER BY pb.date, pb.pool_id