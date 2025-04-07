{{
    config(
        materialized="table",
        snowflake_warehouse="MAKER",
        database="maker",
        schema="raw",
        alias="fact_incremental_m2m"
    )
}}

SELECT
    *,
    dai_m2m - COALESCE(LAG(dai_m2m) OVER (PARTITION BY SUBSTRING(CAST(code AS VARCHAR), 1, 1), token ORDER BY ts), 0) AS incremental_dai_m2m,
    eth_m2m - COALESCE(LAG(eth_m2m) OVER (PARTITION BY SUBSTRING(CAST(code AS VARCHAR), 1, 1), token ORDER BY ts), 0) AS incremental_eth_m2m
FROM {{ ref('fact_cumulative_sums') }}
WHERE cumulative_ale_token_value > 0
    AND SUBSTRING(CAST(code AS VARCHAR), -4) = '9999'