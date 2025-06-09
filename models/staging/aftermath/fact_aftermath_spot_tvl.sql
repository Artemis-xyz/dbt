{{config(
    materialized = 'table',
    database = 'aftermath'
)}}

WITH raw_balances AS (
    SELECT
        date,
        pool_address,
        symbol_a,
        symbol_b,
        vault_a_amount_usd,
        vault_b_amount_usd,
        vault_a_amount_native,
        vault_b_amount_native
    FROM {{ ref('fact_raw_aftermath_spot_swaps') }}
),

-- Step 1: Get the full set of dates per pool from when it first appears onward
pool_date_spine AS (
    SELECT DISTINCT
        rb.pool_address,
        d.date
    FROM raw_balances rb
    JOIN {{ ref('dim_date_spine') }} d
      ON d.date >= (
        SELECT MIN(date) FROM raw_balances r2 WHERE r2.pool_address = rb.pool_address
      )
    WHERE d.date < to_date(sysdate())
),

-- Step 2: Left join raw balances to spine (only actual needed pool-date combinations)
pool_balances_expanded AS (
    SELECT
        ds.date,
        ds.pool_address,
        rb.symbol_a,
        rb.symbol_b,
        rb.vault_a_amount_usd,
        rb.vault_b_amount_usd,
        rb.vault_a_amount_native,
        rb.vault_b_amount_native
    FROM pool_date_spine ds
    LEFT JOIN raw_balances rb
      ON ds.date = rb.date
     AND ds.pool_address = rb.pool_address
),

-- Step 3: Forward fill vault balances using window functions
forward_filled AS (
    SELECT
        date,
        pool_address,
        symbol_a,
        symbol_b,

        COALESCE(
            LAST_VALUE(vault_a_amount_usd IGNORE NULLS) OVER (
                PARTITION BY pool_address ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS vault_a_amount_usd,

        COALESCE(
            LAST_VALUE(vault_b_amount_usd IGNORE NULLS) OVER (
                PARTITION BY pool_address ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS vault_b_amount_usd, 

        COALESCE(
            LAST_VALUE(vault_a_amount_native IGNORE NULLS) OVER (
                PARTITION BY pool_address ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS vault_a_amount_native,

        COALESCE(
            LAST_VALUE(vault_b_amount_native IGNORE NULLS) OVER (
                PARTITION BY pool_address ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ),
            0
        ) AS vault_b_amount_native,
    FROM pool_balances_expanded
)

-- Step 4: Calculate TVL
SELECT
    date,
    pool_address,
    symbol_a,
    symbol_b,
    vault_a_amount_native,
    vault_b_amount_native,
    vault_a_amount_usd,
    vault_b_amount_usd,
    vault_a_amount_usd + vault_b_amount_usd AS tvl, 
    ROW_NUMBER() OVER (
        PARTITION BY date, pool_address 
        ORDER BY date DESC
    ) AS rn
FROM forward_filled
QUALIFY rn = 1
ORDER BY date DESC, pool_address
