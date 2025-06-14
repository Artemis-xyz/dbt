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
    , coalesce(metrics.trading_volume, 0) as trading_volume
    , coalesce(metrics.dau, 0) as dau
    , coalesce(metrics.daily_txns, 0) as daily_txns
    , coalesce(metrics.fees_usd, 0) as fees
    , coalesce(metrics.fees_usd, 0) * 0.4 AS supply_side_fees
    , coalesce(metrics.fees_usd, 0) * 0.6 + coalesce(coin_metrics.burns_usd, 0) AS revenue
    , coalesce(coin_metrics.burns, 0) AS burns_native
    
    --Standardized Metrics

    -- Token Metrics
    , coalesce(market_data.price, 0) AS price
    , coalesce(market_data.market_cap, 0) AS market_cap
    , coalesce(market_data.fdmc, 0) AS fdmc
    , coalesce(market_data.token_volume, 0) AS token_volume

    -- Aggregator Metrics
    , coalesce(metrics.dau, 0) AS aggregator_dau
    , coalesce(metrics.daily_txns, 0) AS aggregator_txns
    , coalesce(metrics.fees_usd, 0) AS aggregator_revenue
    , coalesce(metrics.trading_volume, 0) AS aggregator_volume

    -- Cash Flow Metrics
    , coalesce(metrics.fees_usd, 0) AS ecosystem_revenue
    , coalesce(metrics.fees_usd, 0) * 0.6 AS treasury_fee_allocation
    , coalesce(metrics.fees_usd, 0) * 0.4 AS token_fee_allocation
    , coalesce(coin_metrics.burns, 0) AS burned_fee_allocation_native
    , coalesce(coin_metrics.burns_usd, 0) AS burned_fee_allocation

    -- Supply Metrics
    , coalesce(coin_metrics.circulating_supply, 0) AS circulating_supply_native
    , coalesce(coin_metrics.gross_emissions, 0) AS gross_emissions_native
    , coalesce(coin_metrics.net_supply_change, 0) AS net_supply_change_native
    , coalesce(coin_metrics.pre_mine_unlocks, 0) AS premine_unlocks_native

    -- Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) AS token_turnover_fdv

FROM metrics
LEFT JOIN coin_metrics using (date)
LEFT JOIN market_data using (date)
ORDER BY metrics.date DESC
