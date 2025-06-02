{{config(
    materialized = 'table',
    database = 'cetus'
)}}

WITH vault_balances AS (
    SELECT 
        date, 
        pool_address, 
        symbol_a,
        symbol_b,
        CASE 
            WHEN date = '2024-11-12' THEN NULL
            ELSE vault_a_amount_usd
        END AS vault_a_amount_usd, 
        CASE 
            WHEN date = '2024-11-12' THEN NULL
            ELSE vault_b_amount_usd
        END AS vault_b_amount_usd,
        CASE 
            WHEN date = '2024-11-12' THEN NULL
            ELSE vault_a_amount_native
        END AS vault_a_amount_native,
        CASE 
            WHEN date = '2024-11-12' THEN NULL
            ELSE vault_b_amount_native
        END AS vault_b_amount_native,
        ROW_NUMBER() OVER (
            PARTITION BY date, pool_address 
            ORDER BY timestamp DESC
        ) AS rn    
    FROM {{ ref('fact_raw_cetus_spot_swaps') }}
    QUALIFY rn = 1
), 

filled_forward AS (
    SELECT 
        date,
        pool_address,
        symbol_a,
        symbol_b,
        -- forward fill the vault_a_amount_usd per pool
        LAST_VALUE(vault_a_amount_usd IGNORE NULLS) OVER (
            PARTITION BY pool_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_vault_a_amount_usd,
        -- forward fill the vault_b_amount_usd per pool
        LAST_VALUE(vault_b_amount_usd IGNORE NULLS) OVER (
            PARTITION BY pool_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_vault_b_amount_usd,
        -- forward fill the vault_a_amount_native per pool
        LAST_VALUE(vault_a_amount_native IGNORE NULLS) OVER (
            PARTITION BY pool_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_vault_a_amount_native,
        -- forward fill the vault_b_amount_native per pool
        LAST_VALUE(vault_b_amount_native IGNORE NULLS) OVER (
            PARTITION BY pool_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_vault_b_amount_native
    FROM vault_balances
)

SELECT 
    date,
    pool_address,
    symbol_a,
    symbol_b,
    filled_vault_a_amount_usd AS vault_a_amount_usd,
    filled_vault_b_amount_usd AS vault_b_amount_usd,
    filled_vault_a_amount_native AS vault_a_amount_native,
    filled_vault_b_amount_native AS vault_b_amount_native,
    COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0) AS tvl,
    COALESCE(vault_a_amount_native, 0) + COALESCE(vault_b_amount_native, 0) AS tvl_native
FROM filled_forward
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
