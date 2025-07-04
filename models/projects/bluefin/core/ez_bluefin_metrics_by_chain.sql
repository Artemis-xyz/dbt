{{
    config(
        materialized="table",
        snowflake_warehouse="BLUEFIN",
        database="bluefin",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


WITH
    date_spine AS(
        SELECT
            date, 'sui' AS chain
        FROM {{ ref("dim_date_spine") }}
        WHERE date < to_date(sysdate())
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_bluefin_trading_volume_silver") }})
    )
    , perp_trading_volume as (
        select date, chain, trading_volume
        from {{ ref("fact_bluefin_trading_volume_silver") }}
    )
    , spot_trading_volume AS (
        SELECT date, 'sui' AS chain, SUM(volume_usd) AS spot_dex_volumes
        FROM {{ ref("fact_bluefin_spot_volumes") }}
        GROUP BY 1, 2
    )
    , spot_dau_txns AS (
        SELECT date, 'sui' AS chain, daily_dau AS dau, daily_txns AS txns
        FROM {{ ref("fact_bluefin_spot_dau_txns") }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY date DESC) = 1
    )
    , spot_fees_revenue AS (
        SELECT date, 'sui' AS chain, SUM(fees) AS fees, SUM(foundation_fee_allocation) AS foundation_fee_allocation, SUM(service_fee_allocation) AS service_fee_allocation
        FROM {{ ref("fact_bluefin_spot_fees_revenue") }}
        GROUP BY 1, 2
    )
    , tvl AS (
        SELECT date, 'sui' AS chain, SUM(tvl) AS tvl
        FROM {{ ref("fact_bluefin_spot_tvl") }}
        GROUP BY 1, 2
    )
    , market_data AS ({{ get_coingecko_metrics("bluefin") }})
select
    date
    , 'bluefin' as app
    , 'DeFi' as category
    , perp_trading_volume.chain
    , perp_trading_volume.trading_volume

    -- Standardized Metrics

    --Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    --Cashflow Metrics
    , COALESCE(spot_fees_revenue.fees, 0) AS fees
    , COALESCE(spot_fees_revenue.foundation_fee_allocation, 0) AS foundation_fee_allocation
    , COALESCE(spot_fees_revenue.service_fee_allocation, 0) AS service_fee_allocation
    
    -- Perpetual Metrics
    , COALESCE(perp_trading_volume.trading_volume, 0) AS perp_volume

    -- Spot DEX Metrics
    , COALESCE(spot_dau_txns.dau, 0) AS spot_dau
    , COALESCE(spot_dau_txns.txns, 0) AS spot_txns
    , COALESCE(spot_fees_revenue.fees, 0) AS spot_fees
    , COALESCE(spot_trading_volume.spot_dex_volumes, 0) AS spot_volume

    -- Crypto Metrics
    , COALESCE(tvl.tvl, 0) AS tvl
    , COALESCE(tvl.tvl, 0) - COALESCE(LAG(tvl.tvl) OVER (ORDER BY date), 0) AS tvl_net_change

    -- Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circul
FROM date_spine
LEFT JOIN perp_trading_volume USING(date, chain)
LEFT JOIN spot_trading_volume USING(date, chain)
LEFT JOIN spot_dau_txns USING(date, chain)
LEFT JOIN spot_fees_revenue USING(date, chain)
LEFT JOIN tvl USING(date, chain)
LEFT JOIN market_data USING(date)