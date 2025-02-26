{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_metrics'
    )
}}

SELECT 
    date,
    SUM(daily_volume_usd) as daily_volume_usd,
    SUM(cumulative_volume_usd) as cumulative_volume_usd,
    SUM(daily_fees_usd) as daily_fees_usd,
    SUM(cumulative_fees_usd) as cumulative_fees_usd,
    SUM(unique_traders) as unique_traders,
    SUM(total_swaps) as total_swaps
FROM (
    SELECT * FROM aerodrome.prod_raw.fact_v1_metrics
    UNION ALL
    SELECT * FROM aerodrome.prod_raw.fact_v2_metrics
)
GROUP BY date
ORDER BY date DESC