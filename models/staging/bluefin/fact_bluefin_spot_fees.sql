{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

SELECT
    date,
    pool_address,
    fee_symbol,
    symbol_a,
    symbol_b,
    SUM(fee_amount_native) AS fee_amount_native, 
    SUM(fee_amount_usd) AS fee_amount_usd
FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
GROUP BY 1, 2, 3, 4, 5