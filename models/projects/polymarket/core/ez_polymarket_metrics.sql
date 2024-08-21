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
    trump_prediction_market_100k_buy_order_price,
    kamala_prediction_market_100k_buy_order_price,
    trump_prediction_market_100k_sell_order_price,
    kamala_prediction_market_100k_sell_order_price
FROM {{ ref("fact_polymarket_prediction_markets") }}