{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_final"
    )
}}

-- Non-M2M entries
SELECT
    code,
    ts,
    hash,
    value,
    token,
    descriptor,
    ilk,
    CASE WHEN descriptor = 'MKR Vest Creates/Yanks' THEN 0 ELSE dai_value END AS dai_value,
    CASE WHEN descriptor = 'MKR Vest Creates/Yanks' THEN 0 ELSE eth_value END AS eth_value,
    DATE(ts) AS dt
FROM {{ ref('fact_with_prices') }}
WHERE SUBSTRING(CAST(code AS VARCHAR), -4) <> '9999'

UNION ALL

-- M2M entries
SELECT
    code,
    ts,
    hash,
    NULL AS value,
    token,
    descriptor,
    ilk,
    incremental_dai_m2m AS dai_value,
    incremental_eth_m2m AS eth_value,
    DATE(ts) AS dt
FROM {{ ref('fact_incremental_m2m') }}

-- Final filter
WHERE (COALESCE(value, 0) <> 0 OR dai_value <> 0 OR eth_value <> 0)