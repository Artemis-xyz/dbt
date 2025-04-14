{{
    config(
        materialized='table',
        snowflake_warehouse='PYTH',
        database='pyth',
        schema='core',
        alias='ez_metrics',
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
    ),
    market_metrics as (
        {{ get_coingecko_metrics("pyth") }}
    )

SELECT
    date_spine.date,

    --Old Metrics needed for compatibility
    dau_txns.dau,
    dau_txns.txns,
    '0' as fees,

    --Standardized Metrics

    --Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    --Usage Metrics
    , dau_txns.txns as oracle_txns
    , dau_txns.dau as oracle_dau

    --Cash Flow Metrics
    , 0 as oracle_fees
    , 0 as gross_protocol_revenue

    --Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
FROM date_spine
LEFT JOIN dau_txns ON date_spine.date = dau_txns.date
LEFT JOIN market_metrics ON date_spine.date = market_metrics.date