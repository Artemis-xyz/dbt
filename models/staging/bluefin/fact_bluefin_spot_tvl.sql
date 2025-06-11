{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

WITH vault_balances AS (
    SELECT
        date, 
        pool_address, 
        symbol_a,
        symbol_b,
        vault_a_amount_native,
        vault_b_amount_native,
        vault_a_amount_usd, 
        vault_b_amount_usd,
        COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0) AS tvl,
        ROW_NUMBER() OVER (
            PARTITION BY date, pool_address 
            ORDER BY timestamp DESC
        ) AS rn
    FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
)

SELECT 
    date,
    pool_address,
    symbol_a,
    symbol_b,
    SUM(vault_a_amount_native) AS vault_a_amount_native,
    SUM(vault_b_amount_native) AS vault_b_amount_native,
    SUM(vault_a_amount_usd) AS vault_a_amount_usd,
    SUM(vault_b_amount_usd) AS vault_b_amount_usd,
    SUM(tvl) AS tvl
FROM vault_balances
WHERE rn = 1
GROUP BY 1, 2, 3, 4