{{
    config(
        materialized="table",
        snowflake_warehouse="outerlands"
    )
}}

WITH 
    price_data AS (
        SELECT
            date,
            coingecko_id,
            shifted_token_price_usd as price,
            LAG(shifted_token_price_usd) OVER (PARTITION BY coingecko_id ORDER BY date) as previous_price
        FROM
            pc_dbt_db.prod.fact_coingecko_token_date_adjusted_gold
        WHERE
            coingecko_id IN (select coingecko_id from {{ source('SIGMA', 'dim_outerlands_fundamental_index_assets') }})
    )
SELECT
    date,
    coingecko_id,
    price,
    previous_price,
    CASE
        WHEN previous_price IS NOT NULL AND previous_price != 0
        THEN (price - previous_price) / previous_price * 100
        ELSE NULL
    END AS daily_percent_change
FROM
    price_data
ORDER BY
    coingecko_id,
    date