{{ config(materialized="table") }}

WITH fact_drift_daily_perp_data AS (
    SELECT
        date
        , sum(latest_excess_pnl) as latest_excess_pnl
    FROM
        {{ref('fact_drift_daily_perp_data')}}
    GROUP BY 1
)
SELECT
    date
    , latest_excess_pnl - LAG(latest_excess_pnl) OVER (ORDER BY date) as total_revenue
    , latest_excess_pnl
FROM 
    fact_drift_daily_perp_data
