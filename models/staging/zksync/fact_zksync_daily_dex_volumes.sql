{{
    config(
        materialized="table",
        snowflake_warehouse="ZKSYNC",
    )
}}

WITH dex_volumes AS (
    {{ dune_dex_volumes("zksync") }}
), 

adjusted_dex_volumes AS (
    {{ adjusted_dune_dex_volumes("zksync") }}
)

SELECT 
    dex_volumes.date,
    dex_volumes.daily_volume,
    adjusted_dex_volumes.daily_volume_adjusted
FROM dex_volumes
JOIN adjusted_dex_volumes
    ON dex_volumes.date = adjusted_dex_volumes.date