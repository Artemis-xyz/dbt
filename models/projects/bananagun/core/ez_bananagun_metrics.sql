{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN',
        database='BANANAGUN',
        schema='core',
        alias='ez_metrics'
    )
}}

WITH metrics AS (
    SELECT
        date
        , SUM(trading_volume) AS trading_volume
        , SUM(dau) AS dau
        , SUM(daily_txns) AS daily_txns
        , SUM(fees_usd) AS fees_usd
    FROM {{ ref('fact_bananagun_all_metrics') }}
    GROUP BY date
)
, burns AS (
    SELECT
        date
        , burns_usd AS revenue
    FROM {{ ref('fact_bananagun_coin_metrics') }}
)
, market_data as (
    {{ get_coingecko_metrics('bananagun') }}
)

SELECT
    metrics.date
    , metrics.trading_volume
    , metrics.dau
    , metrics.daily_txns
    , metrics.fees_usd AS fees
    , metrics.fees_usd * 0.6 AS supply_side_fees
    , metrics.fees_usd * 0.4 + burns.revenue AS revenue
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
    , coalesce(market_data.token_volume, 0) as token_volume
FROM metrics
LEFT JOIN burns using (date)
LEFT JOIN market_data using (date)
ORDER BY metrics.date DESC