{{
    config(
        materialized='table',
        snowflake_warehouse='BABYLON',
        database='BABYLON',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

WITH babylon_metrics AS (
    SELECT
        date
        , active_tvl_btc as tvl_native
        , active_tvl_usd as tvl_usd
        , active_delegations as active_delegations
        , total_stakers as total_stakers
        , active_tvl_usd - LAG(active_tvl_usd) 
        OVER (ORDER BY date) AS tvl_net_change
        , active_tvl_btc - LAG(active_tvl_btc) 
        OVER (ORDER BY date) AS tvl_native_net_change
    FROM {{ ref('fact_babylon_metrics') }}
)
, date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    where date between (select min(date) from babylon_metrics) and to_date(sysdate())
)

SELECT
    date_spine.date,
    'bitcoin' as chain

    -- Standardized Metrics

    -- Usage Metrics
    , babylon_metrics.tvl_native
    , babylon_metrics.tvl_native_net_change
    , babylon_metrics.tvl_usd as tvl
    , babylon_metrics.tvl_net_change as tvl_net_change
    , babylon_metrics.active_delegations
    , babylon_metrics.total_stakers

    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

FROM date_spine
LEFT JOIN market_metrics ON date_spine.date = market_metrics.date
LEFT JOIN babylon_metrics ON date_spine.date = babylon_metrics.date