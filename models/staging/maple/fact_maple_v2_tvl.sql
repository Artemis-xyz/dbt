-- fact_maple_tvl.sql
{{ 
    config(
        materialized='table', 
        snowflake_warehouse='MAPLE',
    )
}}

SELECT 1 as dummy

-- WITH onchain_pools AS (
--     SELECT 
--         date, 
--         pool_name, 
--         asset, 
--         outstanding, 
--         outstanding as outstanding_native, 
--         assets as cash, 
--         total_assets, 
--         total_assets as total_assets_native
--     FROM {{ ref('fact_maple_pool_state_daily') }}
-- ),

-- offchain_data AS (
--     SELECT
--         date,
--         pool_name,
--         asset,
--         NULL as total_assets,
--         NULL as total_assets_native,
--         collateral,
--         collat_native
--     FROM {{ ref('fact_maple_offchain_data') }}
--     WHERE pool_name <> ('Altcoin Lending')
--     UNION ALL
--     SELECT
--         date,
--         pool_name,
--         asset,
--         total_assets,
--         total_assets_native,
--         NULL as collateral,
--         NULL as collat_native
--     FROM {{ ref('fact_maple_offchain_data') }}
--     WHERE pool_name = ('Altcoin Lending')
-- ),

-- combined_data AS (
--     SELECT
--         date,
--         pool_name,
--         asset,
--         coalesce(outstanding, 0) as outstanding,
--         coalesce(outstanding, 0) as outstanding_native,
--         coalesce(total_assets, 0) as tvl,
--         coalesce(total_assets, 0) as tvl_native
--     FROM
--         onchain_pools

--     UNION ALL

--     SELECT
--         date,
--         pool_name,
--         asset,
--         0 as outstanding,
--         0 as outstanding_native,
--         coalesce(collateral, 0) + coalesce(total_assets, 0) as tvl,
--         coalesce(collat_native, 0) + coalesce(total_assets_native, 0) as tvl_native
--     FROM
--         offchain_data
-- )

-- SELECT * 
-- FROM combined_data
-- WHERE 
-- -- date > DATE('2023-03-01') 
-- --   AND
--    date <= CURRENT_DATE 
-- ORDER BY date DESC, pool_name