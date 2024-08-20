{{
    config(
        materialized="table",
        snowflake_warehouse="DRIFT",
        database="drift",
        schema="core",
        alias="ez_metrics",
    )
}}

SELECT 
    date,
    'drift' AS app,
    'DeFi' AS category,
    "'TRUMP-WIN-2024-BET'" AS trump_prediction_market_100k_buy_order_price,
    "'KAMALA-POPULAR-VOTE-2024-BET'" AS kamala_prediction_market_100k_buy_order_price
FROM {{ ref("fact_drift_prediction_markets") }}
PIVOT(SUM(PREDICTION_MARKET_100K_BUY_ORDER_PRICE) FOR market IN (ANY ORDER BY market))
