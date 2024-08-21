{{ config(materialized="table") }}

WITH buys AS (
    SELECT 
        DATEADD('DAY', -1, DATE_TRUNC('DAY', extraction_date)::date) AS date,
        value:market::varchar as market,
        AVG(value:average_fill_price) as prediction_market_100k_buy_order_price
    FROM {{ source("PROD_LANDING", "raw_drift_fill_price_by_market") }},
    lateral flatten(input => parse_json(source_json))
    GROUP BY 
        1, 2
), sells AS (
    SELECT 
        DATEADD('DAY', -1, DATE_TRUNC('DAY', extraction_date)::date) AS date,
        value:market::varchar as market,
        AVG(value:average_fill_price) as prediction_market_100k_sell_order_price
    FROM {{ source("PROD_LANDING", "raw_drift_sell_fill_price_by_market") }},
    lateral flatten(input => parse_json(source_json))
    GROUP BY 
        1, 2
), pivoted_buys AS (
    SELECT 
        date,
        "'TRUMP-WIN-2024-BET'" AS trump_prediction_market_100k_buy_order_price,
        "'KAMALA-POPULAR-VOTE-2024-BET'" AS kamala_prediction_market_100k_buy_order_price
    FROM buys
    PIVOT(SUM(PREDICTION_MARKET_100K_BUY_ORDER_PRICE) FOR market IN (ANY ORDER BY market))
    
), pivoted_sells AS (
    SELECT 
        date,
        "'TRUMP-WIN-2024-BET'" AS trump_prediction_market_100k_sell_order_price,
        "'KAMALA-POPULAR-VOTE-2024-BET'" AS kamala_prediction_market_100k_sell_order_price
    FROM sells
    PIVOT(SUM(PREDICTION_MARKET_100K_SELL_ORDER_PRICE) FOR market IN (ANY ORDER BY market))
)
SELECT 
    pivoted_buys.date,
    trump_prediction_market_100k_buy_order_price,
    kamala_prediction_market_100k_buy_order_price,
    trump_prediction_market_100k_sell_order_price,
    kamala_prediction_market_100k_sell_order_price
FROM pivoted_buys
LEFT JOIN pivoted_sells 
    ON pivoted_buys.date = pivoted_sells.date