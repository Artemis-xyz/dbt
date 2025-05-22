{{
    config(
        materialized="table",
        snowflake_warehouse="BLAST",
    )
}}

WITH dex_volumes AS (
    {{ dune_dex_volumes("blast") }}
), 

adjusted_dex_volumes AS (
    {{ adjusted_dune_dex_volumes("blast") }}
)

SELECT 
    dex_volumes.date,
    dex_volumes.daily_volume,
    adjusted_dex_volumes.daily_volume_adjusted
FROM dex_volumes
JOIN adjusted_dex_volumes
    ON dex_volumes.date = adjusted_dex_volumes.date