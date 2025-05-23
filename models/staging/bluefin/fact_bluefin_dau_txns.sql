{{config(
    materialized = 'table',
    database = 'bluefin'
)}}

SELECT
    date,
    COUNT(DISTINCT sender) AS dau, 
    COUNT(DISTINCT transaction_digest) AS txns
FROM {{ ref('fact_raw_bluefin_spot_swaps') }}
GROUP BY 1