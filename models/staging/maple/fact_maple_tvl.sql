-- fact_maple_tvl.sql
{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}


WITH onchain_pools AS (
    SELECT date, last_ts as timestamp, pool_name, outstanding, assets as cash, total_assets 
    FROM {{ ref('fact_maple_pool_state_daily') }}
),

-- offchain_data AS (
--     SELECT *
--     FROM {{ ref('fact_maple_offchain_data') }}
-- ),

combined_data AS (
    SELECT
        oc.date,
        oc.timestamp,
        oc.pool_name,
        oc.outstanding,
        oc.total_assets,
        -- COALESCE(od.collateral, 0) as collateral,
        oc.total_assets 
            -- + COALESCE(od.collateral, 0) 
            as tvl
    FROM
        onchain_pools oc
    -- LEFT JOIN 
        -- offchain_data od ON od.date = oc.date AND od.pool_name = oc.pool_name

    -- UNION ALL

    -- SELECT
    --     date,
    --     timestamp,
    --     pool_name,
    --     outstanding,
    --     total_assets,
    --     collateral,
    --     tvl
    -- FROM
    --     offchain_data
    -- WHERE pool_name IN ('Altcoin Lending', 'Maple Solana')
)

SELECT * 
FROM combined_data
WHERE date > DATE('2023-03-01') 
  AND date <= CURRENT_DATE 
  AND pool_name NOT IN ('Orthogonal Credit USDC1')
ORDER BY date DESC, pool_name