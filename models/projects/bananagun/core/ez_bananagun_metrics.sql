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
        trade_date
        , SUM(trading_volume) AS trading_volume
        , SUM(dau) AS dau
        , SUM(daily_txns) AS daily_txns
        , SUM(fees_usd) AS fees_usd
    FROM {{ ref('fact_bananagun_all_metrics') }}
    GROUP BY trade_date
)

, burns AS (
    SELECT
        date
        , burns_usd AS revenue
    FROM {{ ref('fact_bananagun_coin_metrics') }}
)

SELECT
    metrics.trade_date
    , metrics.trading_volume
    , metrics.dau
    , metrics.daily_txns
    , metrics.fees_usd AS fees
    , metrics.fees_usd * 0.6 AS supply_side_fees
    , metrics.fees_usd * 0.4 + burns.revenue AS revenue
FROM metrics
LEFT JOIN burns ON metrics.trade_date = burns.date
ORDER BY metrics.trade_date DESC
