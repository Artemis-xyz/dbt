{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN',
        database='BANANAGUN',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with metrics as (
    SELECT *
    FROM {{ ref('fact_bananagun_all_metrics') }}
)

SELECT
    date
    , chain
    , coalesce(metrics.trading_volume, 0) as trading_volume

    --Standardized Metrics

    -- Aggregator Metrics
    , coalesce(metrics.dau, 0) AS aggregator_dau
    , coalesce(metrics.daily_txns, 0) AS aggregator_txns
    , coalesce(metrics.fees_usd, 0) AS aggregator_revenue
    , coalesce(metrics.trading_volume, 0) AS aggregator_volume

    -- Cash Flow Metrics
    , coalesce(metrics.fees_usd, 0) AS gross_protocol_revenue
    , coalesce(metrics.fees_usd, 0) * 0.6 AS treasury_cash_flow
    , coalesce(metrics.fees_usd, 0) * 0.4 AS token_cash_flow
FROM metrics
ORDER BY date DESC