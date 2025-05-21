{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

WITH vault_balances AS (
    SELECT
        date,
        pool_address,
        (COALESCE(vault_a_amount, 0) * COALESCE(price_a, 0)) AS vault_a_tvl,
        (COALESCE(vault_b_amount, 0) * COALESCE(price_b, 0)) AS vault_b_tvl,
        (COALESCE(vault_a_amount, 0) * COALESCE(price_a, 0)) + (COALESCE(vault_b_amount, 0) * COALESCE(price_b, 0)) AS pool_tvl
    FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
    GROUP BY 1, 2
), 

partitioned_vault_balances AS (
    SELECT
        date,
        ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY date DESC) AS rn,
    FROM vault_balances
)

SELECT * FROM partitioned_vault_balances