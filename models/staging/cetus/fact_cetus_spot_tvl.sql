{{config(
    materialized = 'table',
    database = 'cetus'
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
    CASE
        WHEN date = '2024-11-12' AND pool_address = '0xa528b26eae41bcfca488a9feaa3dca614b2a1d9b9b5c78c256918ced051d4c50' THEN 2 * vault_b_amount_usd
            -- This was a pricing issue on this day, so just had to approximate the TVL for this particular pool
        ELSE (COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0)) 
    END AS tvl  
FROM {{ ref('fact_raw_cetus_spot_swaps') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, pool_address ORDER BY date DESC, pool_address DESC) = 1