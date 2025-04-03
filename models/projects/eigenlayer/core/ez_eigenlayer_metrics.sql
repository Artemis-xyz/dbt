{{
    config(
        materialized="table",
        snowflake_warehouse="EIGENLAYER",
        database="EIGENLAYER",
        schema="core",
        alias="ez_metrics",
    )
}}

-- Simplified ez metrics table that aggregates data for eigenlayer
WITH 
    eigenlayer_aggregated AS (
        SELECT 
            date
            , protocol as app
            , category
            , SUM(num_restaked_eth) AS num_restaked_eth
            , SUM(amount_restaked_usd) AS amount_restaked_usd
        FROM {{ref('fact_eigenlayer_restaked_assets')}}
        GROUP BY date, protocol, category
    )
    , eigenlayer_supply_data AS (
        SELECT
            date
            , emissions_native
            , premine_unlocks_native
            , net_supply_change_native
            , circulating_supply
        FROM {{ ref('fact_eigenlayer_supply_data') }}
    )
    , market_data as (
        {{ get_coingecko_metrics('eigenlayer') }}
    )

SELECT 
    date
    , app
    , category

    -- Standarized Metrics

    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating

    -- Crypto Metrics
    , amount_restaked_usd as tvl
    , amount_restaked_usd - LAG(amount_restaked_usd) 
        OVER (ORDER BY date) AS tvl_net_change
    , num_restaked_eth as tvl_native
    , num_restaked_eth - LAG(num_restaked_eth) 
        OVER (ORDER BY date) AS tvl_native_net_change

    -- Supply Metrics
    , circulating_supply
    , emissions_native as emissions_native
    , net_supply_change_native
    , premine_unlocks_native as premine_unlocks_native

    -- Turnover Metrics
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
    , coalesce(market_data.token_volume, 0) as token_volume
FROM eigenlayer_aggregated
LEFT JOIN eigenlayer_supply_data using (date)
LEFT JOIN market_data using (date)
ORDER BY date