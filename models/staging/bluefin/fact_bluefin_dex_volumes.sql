{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

SELECT
    date, 
    SUM(GREATEST((COALESCE(amount_a_swapped, 0) * COALESCE(price_a, 0)), (COALESCE(amount_b_swapped, 0) * COALESCE(price_b, 0)))) AS volume
FROM {{ ref('fact_bluefin_dex_swaps') }}
GROUP BY 1
