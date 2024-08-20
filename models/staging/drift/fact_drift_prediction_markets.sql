{{ config(materialized="table") }}

SELECT 
    DATE_TRUNC('DAY', extraction_date)::date AS date,
    value:market as market,
    AVG(value:average_fill_price) as daily_average_fill_price
FROM {{ source("PROD_LANDING", "raw_drift_fill_price_by_market") }},
lateral flatten(input => parse_json(source_json))
GROUP BY 
    1, 2
