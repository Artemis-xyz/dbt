{{
    config(
        materialized='table',
        unique_key='date',
        snowflake_warehouse='PYTH',
    )
}}

with agg as (
    SELECT date, dau, txns FROM {{ ref('fact_pyth_v1_solana_txns_dau') }}
    UNION ALL
    SELECT date, dau, txns FROM {{ ref('fact_pyth_v2_solana_txns_dau') }}
)

SELECT
    date,
    SUM(txns) AS txns,
    SUM(dau) AS dau
FROM agg
GROUP BY 1
