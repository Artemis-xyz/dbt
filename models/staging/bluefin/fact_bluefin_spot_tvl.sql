{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

WITH date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    WHERE date >= (SELECT MIN(date) FROM {{ ref('fact_raw_bluefin_spot_swaps') }})
        AND date < to_date(sysdate())
), 

all_pools AS (
    SELECT DISTINCT
        pool_address, 
        symbol_a,
        symbol_b
    FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
), 

date_pool_matrix AS (
    SELECT d.date, p.pool_address, p.symbol_a, p.symbol_b
    FROM date_spine d
    CROSS JOIN all_pools p
), 

vault_balances AS (
  SELECT
      date,
      timestamp,
      pool_address,
      symbol_a,
      symbol_b,
      vault_a_amount_native,
      vault_b_amount_native,
      vault_a_amount_usd,
      vault_b_amount_usd,
      COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0) AS pool_tvl
  FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
),

latest_snapshot_per_day AS (
  SELECT
    dpm.date,
    dpm.pool_address,
    dpm.symbol_a,
    dpm.symbol_b,
    vb.vault_a_amount_native,
    vb.vault_b_amount_native,
    vb.vault_a_amount_usd,
    vb.vault_b_amount_usd,
    vb.pool_tvl,
    ROW_NUMBER() OVER (
      PARTITION BY dpm.date, dpm.pool_address
      ORDER BY vb.timestamp DESC
    ) AS rn
  FROM date_pool_matrix dpm
  LEFT JOIN vault_balances vb
    ON vb.pool_address = dpm.pool_address AND vb.date <= dpm.date
)

SELECT
  date,
  pool_address,
  symbol_a,
  symbol_b,
  vault_a_amount_native,
  vault_b_amount_native,
  vault_a_amount_usd,
  vault_b_amount_usd,
  pool_tvl
FROM latest_snapshot_per_day
WHERE rn = 1


/*
WITH vault_balances AS (
    SELECT
        date,
        timestamp,
        pool_address,
        symbol_a,
        symbol_b,
        vault_a_amount_native,
        vault_b_amount_native,
        vault_a_amount_usd,
        vault_b_amount_usd,
        COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0) AS pool_tvl
    FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
), 

partitioned_vault_balances AS (
    SELECT
        date,
        pool_address,
        symbol_a,
        symbol_b,
        vault_a_amount_native,
        vault_b_amount_native,
        vault_a_amount_usd,
        vault_b_amount_usd,
        pool_tvl,
        ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY timestamp DESC) AS rn
    FROM vault_balances
)

SELECT
    date,
    pool_address,
    symbol_a,
    symbol_b,
    vault_a_amount_native,
    vault_b_amount_native,
    vault_a_amount_usd,
    vault_b_amount_usd,
    pool_tvl
FROM partitioned_vault_balances 
WHERE rn = 1
*/