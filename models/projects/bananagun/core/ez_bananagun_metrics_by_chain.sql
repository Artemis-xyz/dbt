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

    -- Aggregator Metrics
    , dau AS aggregator_dau
    , daily_txns AS aggregator_txns
    , trading_volume AS aggregator_volume
FROM metrics
ORDER BY date DESC