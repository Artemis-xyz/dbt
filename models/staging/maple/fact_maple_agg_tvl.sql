{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}
WITH agg_tvl AS (
    SELECT 
        date, 
        pool_name, 
        asset,
        outstanding_usd as tvl,
        outstanding_usd as tvl_native,
        null as outstanding_supply
    FROM {{ ref('fact_maple_v1_tvl') }}
    UNION ALL
    SELECT 
        date, 
        pool_name, 
        asset,
        tvl,
        tvl_native,
        outstanding as outstanding_supply
    FROM {{ ref('fact_maple_v2_tvl') }}
)

SELECT
    date,
    pool_name,
    asset,
    sum(tvl) as tvl,
    sum(tvl_native) as tvl_native,
    sum(outstanding_supply) as outstanding_supply
FROM agg_tvl
GROUP BY 1, 2, 3
-- WHERE
--  pool_name NOT IN ('Orthogonal Credit USDC1')
ORDER BY date DESC, pool_name