{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

SELECT
    date, 
    pool_address,
    symbol_a,
    symbol_b, 
    SUM(amount_a_swapped_native) AS amount_a_swapped_native,
    SUM(amount_b_swapped_native) AS amount_b_swapped_native,
    SUM(amount_a_swapped_usd) AS amount_a_swapped_usd,
    SUM(amount_b_swapped_usd) AS amount_b_swapped_usd,
    SUM(GREATEST(COALESCE(amount_a_swapped_usd, 0), COALESCE(amount_b_swapped_usd, 0))) AS volume_usd
FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
GROUP BY 1, 2, 3, 4
