{{
    config(
        materialized='table',
        snowflake_warehouse='PYTH',
        database='pyth',
        schema='core',
        alias='ez_metrics_by_chain',
    )
}}

with 
    dau_txns as (
        SELECT * FROM {{ ref('fact_pyth_txns_dau') }}
    )
    , date_spine as (
        SELECT
            date
        FROM {{ ref('dim_date_spine') }}
        WHERE date between (SELECT min(date) FROM dau_txns) and to_date(sysdate())
    )

SELECT
    date_spine.date
    , 'solana' as chain

    --Old Metrics needed for compatibility
    , dau_txns.dau
    , dau_txns.txns
    , '0' as fees

    --Standardized Metrics

    --Usage Metrics
    , dau_txns.txns as oracle_txns
    , dau_txns.dau as oracle_dau

    --Cash Flow Metrics
    , 0 as oracle_fees
    , 0 as ecosystem_revenue
FROM date_spine
LEFT JOIN dau_txns USING (date)