{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

WITH vault_balances AS (
    SELECT
        date,
        timestamp,
        pool_address,
        symbol_a,
        symbol_b,
        vault_a_amount_native,
        vault_b_amount_native,
        vault_a_amount_usd,
        vault_b_amount_usd,
        COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0) AS pool_tvl
    FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
), 

partitioned_vault_balances AS (
    SELECT
        date,
        pool_address,
        symbol_a,
        symbol_b,
        vault_a_amount_native,
        vault_b_amount_native,
        vault_a_amount_usd,
        vault_b_amount_usd,
        pool_tvl,
        ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY timestamp DESC) AS rn
    FROM vault_balances
)

SELECT
    date,
    pool_address,
    symbol_a,
    symbol_b,
    vault_a_amount_native,
    vault_b_amount_native,
    vault_a_amount_usd,
    vault_b_amount_usd,
    pool_tvl
FROM partitioned_vault_balances 
WHERE rn = 1