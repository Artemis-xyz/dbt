{{config(
    materialized = 'table',
    database = 'momentum'
)}}

SELECT
    date,
    pool_address,
    fee_symbol,
    symbol_a,
    symbol_b,
    SUM(fee_amount_native) AS fees_native, 
    SUM(fee_amount_usd) AS fees_usd, 
FROM {{ ref('fact_raw_momentum_spot_swaps') }}
GROUP BY 1, 2, 3, 4, 5