{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="dim_ilk_list_labeled"
    )
}}

-- This table should contain your ilk mappings
-- You may need to adjust this based on your specific ilk categorizations
SELECT 
    ilk,
    begin_dt,
    end_dt,
    asset_code,
    equity_code
FROM {{ ref('dim_ilk_list_manual_input') }}

UNION ALL

SELECT
    ilk,
    CAST(NULL AS DATE) AS begin_dt,
    CAST(NULL AS DATE) AS end_dt,
    CASE
        WHEN ilk LIKE 'ETH-%' THEN 11110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A' THEN 11120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 11130
        WHEN ilk LIKE 'GUNI%' THEN 11140
        WHEN ilk LIKE 'UNIV2%' THEN 11141
        WHEN ilk LIKE 'DIRECT%' THEN 11210
        WHEN ilk LIKE 'RWA%' THEN 12310
        WHEN ilk LIKE 'PSM%' THEN 13410
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A') THEN 11510
        ELSE 11199
    END AS asset_code,
    CASE
        WHEN ilk LIKE 'ETH-%' THEN 31110
        WHEN ilk LIKE 'WBTC-%' OR ilk = 'RENBTC-A'  THEN 31120
        WHEN ilk LIKE 'WSTETH-%' OR ilk LIKE 'RETH-%' OR ilk = 'CRVV1ETHSTETH-A' THEN 31130
        WHEN ilk LIKE 'GUNI%' THEN 31140
        WHEN ilk LIKE 'UNIV2%' THEN 31141
        WHEN ilk LIKE 'DIRECT%' THEN 31160
        WHEN ilk LIKE 'RWA%' THEN 31170
        WHEN ilk LIKE 'PSM%' THEN NULL
        WHEN ilk IN ('USDC-A','USDC-B', 'USDT-A', 'TUSD-A','GUSD-A','PAXUSD-A') THEN 31190
        ELSE 31150
    END AS equity_code
FROM {{ ref('fact_ilk_list') }}
WHERE ilk NOT IN (SELECT ilk FROM {{ ref('dim_ilk_list_manual_input') }})
AND ilk <> 'TELEPORT-FW-A'