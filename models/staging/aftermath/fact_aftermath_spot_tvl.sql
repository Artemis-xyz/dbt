{{config(
    materialized = 'table',
    database = 'aftermath'
)}}

SELECT 
    date, 
    pool_address, 
    symbol_a,
    symbol_b,
    vault_a_amount_native,
    vault_b_amount_native,
    vault_a_amount_usd, 
    vault_b_amount_usd,
    (COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0)) AS tvl, 
    ROW_NUMBER() OVER (
        PARTITION BY date, pool_address 
        ORDER BY date DESC
    ) AS rn    
FROM {{ ref('fact_raw_aftermath_spot_swaps') }}
QUALIFY rn = 1