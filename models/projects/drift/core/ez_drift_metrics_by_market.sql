{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics_by_market",
    )
}}

SELECT 
    date,
    market,
    'drift' AS app,
    'DeFi' AS category,
    'solana' AS chain,
    daily_average_fill_price
FROM {{ ref("fact_drift_prediction_markets") }}
