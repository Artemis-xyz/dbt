{{config(
    materialized = 'table',
    database = 'cetus'
)}}

WITH unfiltered_data AS (
    SELECT
        date, 
        transaction_digest,
        pool_address,
        symbol_a,
        symbol_b, 
        SUM(COALESCE(amount_a_swapped_native, 0)) AS amount_a_swapped_native,
        SUM(COALESCE(amount_b_swapped_native, 0)) AS amount_b_swapped_native,
        SUM(COALESCE(amount_a_swapped_usd, 0)) AS amount_a_swapped_usd,
        SUM(COALESCE(amount_b_swapped_usd, 0)) AS amount_b_swapped_usd,
        SUM(GREATEST(COALESCE(amount_a_swapped_usd, 0), COALESCE(amount_b_swapped_usd, 0))) AS volume_usd
    FROM {{ ref('fact_raw_cetus_spot_swaps') }}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT *
FROM unfiltered_data
WHERE amount_a_swapped_usd / NULLIF(amount_b_swapped_usd, 0) BETWEEN 0.5 AND 2.5


