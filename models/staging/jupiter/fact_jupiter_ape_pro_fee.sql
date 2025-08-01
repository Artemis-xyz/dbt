{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="ANALYTICS_XL"
    )
}}

WITH raw_revenue AS (
  SELECT
    BLOCK_TIMESTAMP,
    DATE(BLOCK_TIMESTAMP) AS date,
    BALANCE - PRE_BALANCE AS sol_delta
  FROM
    {{ source('SOLANA_FLIPSIDE', 'fact_sol_balances')}}

  WHERE
    OWNER = '5YET3YapxD6to6rqPqTWB3R9pSbURy6yduuUtoZkzoPX'
    {% if is_incremental() %}
    AND BLOCK_TIMESTAMP > (
      SELECT DATEADD('day', -3, MAX(date)) FROM {{ this }}
    )
    {% endif %}
),

joined_with_price AS (
  SELECT
    r.date,
    r.BLOCK_TIMESTAMP,
    r.sol_delta,
    p.price AS usd_price
  FROM
    raw_revenue r
  LEFT JOIN
    {{ source('SOLANA_FLIPSIDE_PRICE', 'ez_prices_hourly')}} p
    ON r.BLOCK_TIMESTAMP >= p.hour
    AND r.BLOCK_TIMESTAMP < DATEADD('hour', 1, p.hour)
    AND p.token_address = 'So11111111111111111111111111111111111111112' -- SOL token
)

SELECT
  date,
  SUM(sol_delta) AS sol_revenue,
  SUM(sol_delta * usd_price) AS usd_revenue
FROM
  joined_with_price
GROUP BY
  date
ORDER BY
  date DESC
