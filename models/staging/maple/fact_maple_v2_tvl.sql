-- fact_maple_tvl.sql
{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}


WITH onchain_pools AS (
    SELECT 
        date, 
        pool_name, 
        asset, 
        outstanding, 
        outstanding as outstanding_native, 
        assets as cash, 
        total_assets, 
        total_assets as total_assets_native
    FROM {{ ref('fact_maple_pool_state_daily') }}
),

offchain_data AS (
    SELECT
        date,
        pool_name,
        asset,
        collateral,
        collat_native
    FROM {{ ref('fact_maple_offchain_data') }}
),

combined_data AS (
    SELECT
        date,
        pool_name,
        asset,
        outstanding,
        outstanding as outstanding_native,
        total_assets as tvl,
        total_assets as tvl_native
    FROM
        onchain_pools

    UNION ALL

    SELECT
        date,
        pool_name,
        asset,
        NULL as outstanding,
        NULL as outstanding_native,
        collateral as tvl,
        collat_native as tvl_native
    FROM
        offchain_data
)

SELECT * 
FROM combined_data
WHERE date > DATE('2023-03-01') 
  AND date <= CURRENT_DATE 
ORDER BY date DESC, pool_name