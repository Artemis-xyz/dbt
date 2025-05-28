{{config(
    materialized = 'table',
    database = 'momentum'
)}}

SELECT
    date,
    pool_address,
    fee_symbol AS token_sold, 
    CASE
        WHEN lower(fee_symbol) = lower(symbol_a) THEN symbol_b
        WHEN lower(fee_symbol) = lower(symbol_b) THEN symbol_a
    END AS token_bought, 
    COUNT(DISTINCT sender) AS dau, 
    COUNT(DISTINCT transaction_digest) AS txns
FROM {{ ref('fact_raw_momentum_spot_swaps') }}
GROUP BY 1, 2, 3, 4