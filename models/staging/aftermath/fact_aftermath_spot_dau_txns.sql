{{config(
    materialized = 'table',
    database = 'aftermath'
)}}

WITH daily_dau_txns AS (
    SELECT
        date, 
        COUNT(DISTINCT sender) AS daily_dau, 
        COUNT(DISTINCT transaction_digest) AS daily_txns
    FROM {{ ref('fact_raw_aftermath_spot_swaps') }}
    GROUP BY 1
)

SELECT
    date,
    pool_address,
    fee_symbol AS token_sold, 
    CASE
        WHEN lower(fee_symbol) = lower(symbol_a) THEN symbol_b
        WHEN lower(fee_symbol) = lower(symbol_b) THEN symbol_a
    END AS token_bought, 
    daily_dau, 
    daily_txns, 
    COUNT(DISTINCT sender) AS pool_dau, 
    COUNT(DISTINCT transaction_digest) AS pool_txns
FROM {{ ref('fact_raw_aftermath_spot_swaps') }}
INNER JOIN daily_dau_txns USING(date)
GROUP BY 1, 2, 3, 4, 5, 6