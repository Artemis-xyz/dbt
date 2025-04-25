{{
    config(
        materialized='table',
        snowflake_warehouse='BABYLON',
        database='BABYLON',
        schema='core',
        alias='ez_metrics'
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
, defillama_tvl_data as (
    select
        date,
        tvl
    from {{ ref('fact_babylon_tvl') }}
)
, tvl_seeding as (
    select
        defillama_tvl_data.date,
        CASE
            WHEN defillama_tvl_data.date < '2025-04-25' THEN defillama_tvl_data.tvl
            ELSE tvl_usd
        END as tvl,
        tvl - LAG(tvl) 
        OVER (ORDER BY defillama_tvl_data.date) AS tvl_net_change
    from defillama_tvl_data
    left join babylon_metrics on defillama_tvl_data.date = babylon_metrics.date
)
, date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    where date between (select min(date) from tvl_seeding) and to_date(sysdate())
)
, market_metrics AS (
    {{ get_coingecko_metrics('babylon') }}
)

SELECT
    date_spine.date

    -- Standardized Metrics

    -- Market Metrics 
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , babylon_metrics.tvl_native
    , babylon_metrics.tvl_native_net_change
    , tvl_seeding.tvl
    , tvl_seeding.tvl_net_change
    , babylon_metrics.active_delegations
    , babylon_metrics.total_stakers

    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

FROM date_spine
LEFT JOIN market_metrics ON date_spine.date = market_metrics.date
LEFT JOIN babylon_metrics ON date_spine.date = babylon_metrics.date
LEFT JOIN tvl_seeding ON date_spine.date = tvl_seeding.date