{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="ANALYTICS_XL",
    )
}}

WITH daily_price AS (
  SELECT
    DATE_TRUNC('day', hour) AS date,
    token_address AS quote_mint,
    AVG(price) AS avg_usd_price,
    MAX(decimals) AS decimals
  FROM 
    {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly')}}
  WHERE 
    token_address = 'So11111111111111111111111111111111111111112' -- SOL token address
  GROUP BY 
    1, 2
),

sol_revenue AS (
  SELECT
    DATE(BLOCK_TIMESTAMP) AS date,
    SUM(BALANCE - PRE_BALANCE) AS sol_revenue
  FROM
   {{ source('SOLANA_FLIPSIDE', 'fact_sol_balances')}}
  WHERE
    OWNER = '5YET3YapxD6to6rqPqTWB3R9pSbURy6yduuUtoZkzoPX'
  GROUP BY
    1
)

SELECT
  sr.date,
  sr.sol_revenue,
  dp.avg_usd_price,
  sr.sol_revenue * dp.avg_usd_price AS usd_revenue
FROM
  sol_revenue sr
LEFT JOIN
  daily_price dp
ON
  sr.date = dp.date
ORDER BY
  sr.date DESC
limit 10
