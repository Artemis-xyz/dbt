{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

WITH v1_tvl AS (
    SELECT * FROM {{ ref('fact_maple_v1_tvl') }}
),

v2_tvl AS (
    SELECT * FROM {{ ref('fact_maple_tvl') }}
),

agg_tvl AS (
    SELECT 
        date, 
        pool_name, 
        outstanding_usd as tvl 
    FROM v1_tvl
    UNION ALL
    SELECT 
        date, 
        pool_name, 
        tvl 
    FROM v2_tvl
)

SELECT *
FROM agg_tvl
-- WHERE
--  pool_name NOT IN ('Orthogonal Credit USDC1')
ORDER BY date DESC, pool_name