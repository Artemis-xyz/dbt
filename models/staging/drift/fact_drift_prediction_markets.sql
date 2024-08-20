{{ config(materialized="table") }}

SELECT 
    DATEADD('DAY', -1, DATE_TRUNC('DAY', extraction_date)::date) AS date,
    value:market::varchar as market,
    AVG(value:average_fill_price) as prediction_market_100k_buy_order_price
FROM {{ source("PROD_LANDING", "raw_drift_fill_price_by_market") }},
lateral flatten(input => parse_json(source_json))
GROUP BY 
    1, 2
