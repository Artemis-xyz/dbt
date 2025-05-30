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
        vault_a_amount_usd, 
        vault_b_amount_usd,
        CASE 
            WHEN date = '2024-11-12' THEN NULL
            ELSE (vault_a_amount_usd + vault_b_amount_usd)
        END AS tvl,
        ROW_NUMBER() OVER (
            PARTITION BY date, pool_address 
            ORDER BY timestamp DESC
        ) AS rn    
    FROM {{ ref('fact_raw_cetus_spot_swaps') }}
),

latest_per_day AS (
    SELECT *
    FROM vault_balances
    WHERE rn = 1
),

filled_forward AS (
    SELECT 
        date,
        pool_address,
        symbol_a,
        symbol_b,
        vault_a_amount_usd,
        vault_b_amount_usd,
        -- forward fill the TVL per pool
        LAST_VALUE(tvl IGNORE NULLS) OVER (
            PARTITION BY pool_address
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS filled_tvl
    FROM latest_per_day
)

SELECT 
    date,
    pool_address,
    symbol_a,
    symbol_b,
    vault_a_amount_usd,
    vault_b_amount_usd,
    SUM(COALESCE(filled_tvl, 0)) AS tvl
FROM filled_forward
GROUP BY 1, 2, 3, 4, 5, 6
