{{config(
    materialized = 'table',
    database = 'cetus'
)}}

-- Step 1: Get raw vault balances
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
        CASE
            WHEN date = '2024-11-12' THEN NULL
            ELSE COALESCE(vault_a_amount_usd, 0) + COALESCE(vault_b_amount_usd, 0) 
        END AS pool_tvl, 
        ROW_NUMBER() OVER (PARTITION BY date, pool_address ORDER BY timestamp DESC) AS rn
    FROM {{ ref('fact_raw_cetus_spot_swaps') }}
),

-- Step 2: Get first seen date for each pool
pool_first_seen AS (
    SELECT 
        pool_address,
        symbol_a,
        symbol_b,
        MIN(date) AS first_seen
    FROM {{ ref('fact_raw_cetus_spot_swaps') }}
    GROUP BY 1, 2, 3
),

-- Step 3: Build pool × date matrix only from first_seen onward
pool_date_spine AS (
    SELECT
        d.date,
        p.pool_address,
        p.symbol_a,
        p.symbol_b
    FROM {{ ref('dim_date_spine') }} d
    JOIN pool_first_seen p ON d.date >= p.first_seen
    WHERE d.date < TO_DATE(SYSDATE())
),

-- Step 4: Join to raw vault balances (sparse)
sparse_balances AS (
    SELECT
        vb.date,
        vb.pool_address,
        vb.symbol_a,
        vb.symbol_b,
        vb.vault_a_amount_native,
        vb.vault_b_amount_native,
        vb.vault_a_amount_usd,
        vb.vault_b_amount_usd,
        vb.pool_tvl
    FROM vault_balances vb
    WHERE vb.rn = 1
),

-- Step 5: Build dense matrix and apply fill forward
dense_matrix AS (
    SELECT
        spine.date,
        spine.pool_address,
        spine.symbol_a,
        spine.symbol_b,
        sb.vault_a_amount_native,
        sb.vault_b_amount_native,
        sb.vault_a_amount_usd,
        sb.vault_b_amount_usd,
        sb.pool_tvl
    FROM pool_date_spine spine
    LEFT JOIN sparse_balances sb
      ON sb.pool_address = spine.pool_address
     AND sb.date = spine.date
),

fill_forward AS (
    SELECT
        date,
        pool_address,
        symbol_a,
        symbol_b,

        LAST_VALUE(vault_a_amount_native IGNORE NULLS) OVER (
            PARTITION BY pool_address ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vault_a_amount_native,

        LAST_VALUE(vault_b_amount_native IGNORE NULLS) OVER (
            PARTITION BY pool_address ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vault_b_amount_native,

        LAST_VALUE(vault_a_amount_usd IGNORE NULLS) OVER (
            PARTITION BY pool_address ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vault_a_amount_usd,

        LAST_VALUE(vault_b_amount_usd IGNORE NULLS) OVER (
            PARTITION BY pool_address ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS vault_b_amount_usd,

        LAST_VALUE(pool_tvl IGNORE NULLS) OVER (
            PARTITION BY pool_address ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS pool_tvl
    FROM dense_matrix
)

-- Step 6: Final output
SELECT *
FROM fill_forward
WHERE pool_tvl IS NOT NULL
ORDER BY date DESC, pool_address
