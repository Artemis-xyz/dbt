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

    -- Chain Metrics
    , trading_volume as chain_spot_volume
    , dau as chain_dau
    , daily_txns as chain_txns
FROM metrics
ORDER BY date DESC