{{
    config(
        materialized='table',
        snowflake_warehouse='BABYLON',
        database='BABYLON',
        schema='core',
        alias='ez_metrics'
    )
}}

with tvl_data as (
    select
        date,
        tvl,
        tvl - LAG(tvl) 
        OVER (ORDER BY date) AS tvl_net_change
    from {{ ref('fact_babylon_tvl') }}
)
, date_spine AS (
    SELECT
        date
    FROM {{ ref('dim_date_spine') }}
    where date between (select min(date) from tvl_data) and to_date(sysdate())

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
    , tvl_data.tvl
    , tvl_data.tvl_net_change

    -- Turnover Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

FROM date_spine
LEFT JOIN market_metrics using (date)
LEFT JOIN tvl_data using (date)
WHERE date_spine.date <= to_date(sysdate())
