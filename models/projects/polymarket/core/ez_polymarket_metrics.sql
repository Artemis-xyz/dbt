{{
    config(
        materialized="table",
        snowflake_warehouse="POLYMARKET",
        database="polymarket",
        schema="core",
        alias="ez_metrics",
    )
}}

SELECT 
    date,
    'polymarket' AS app,
    'DeFi' AS category,
    "'21742633143463906290569050155826241533067272736897614950488156847949938836455'" AS trump_prediction_market_100k_buy_order_price,
    "'21271000291843361249209065706097167029083067325856089903026951915683588703117'" AS kamala_prediction_market_100k_buy_order_price
FROM {{ ref("fact_polymarket_prediction_markets") }}
PIVOT(SUM(PREDICTION_MARKET_100K_BUY_ORDER_PRICE) FOR market IN (ANY ORDER BY market))
