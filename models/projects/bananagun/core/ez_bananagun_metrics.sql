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
, coin_metrics AS (
    SELECT
        date
        , net_supply_change
        , circulating_supply
        , burns
        , gross_emissions
        , pre_mine_unlocks
        , burns_usd
    FROM {{ ref('fact_bananagun_coin_metrics') }}
)
, market_data as (
    {{ get_coingecko_metrics('banana-gun') }}
)

SELECT
    metrics.date

    -- Cash Flow Metrics
    , metrics.fees_usd * 0.4 + coin_metrics.burns_usd AS gross_protocol_revenue
    , coin_metrics.gross_emissions AS token_cash_flow
    , metrics.fees_usd * 0.6 AS fee_sharing_token_cash_flow

    -- Token Metrics
    , coalesce(market_data.price, 0) AS price
    , coalesce(market_data.market_cap, 0) AS market_cap
    , coalesce(market_data.fdmc, 0) AS fdmc
    , coalesce(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) AS token_turnover_fdv
    , coalesce(market_data.token_volume, 0) AS token_volume

    -- Aggregator Metrics
    , metrics.dau AS aggregator_dau
    , metrics.daily_txns AS aggregator_txns
    , metrics.trading_volume AS aggregator_volume
    , metrics.fees_usd * 0.4 + coin_metrics.burns_usd AS aggregator_revenue

    -- Supply Metrics
    , coin_metrics.burns AS burns_native
    , coin_metrics.gross_emissions as mints_native
    , coin_metrics.gross_emissions AS emissions_native
    , coin_metrics.pre_mine_unlocks AS premine_unlocks_native
    , coin_metrics.net_supply_change AS net_supply_change_native
    , coin_metrics.circulating_supply AS circulating_supply_native

FROM metrics
LEFT JOIN coin_metrics using (date)
LEFT JOIN market_data using (date)
ORDER BY metrics.date DESC
