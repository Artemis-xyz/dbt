{{
    config(
        materialized="table",
        snowflake_warehouse="BLUEFIN",
        database="bluefin",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH
    date_spine AS(
        SELECT
            date
        FROM {{ ref("dim_date_spine") }}
        WHERE date < to_date(sysdate())
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_bluefin_trading_volume_silver") }})
    )
    , perp_trading_volume AS (
        SELECT date, SUM(trading_volume) AS trading_volume
        FROM {{ ref("fact_bluefin_trading_volume_silver") }}
        GROUP BY 1
    )
    , spot_trading_volume AS (
        SELECT date, SUM(volume_usd) AS spot_dex_volumes
        FROM {{ ref("fact_bluefin_spot_volumes") }}
        GROUP BY 1
    )
    , spot_dau_txns AS (
        SELECT date, SUM(dau) AS dau, SUM(txns) AS txns
        FROM {{ ref("fact_bluefin_spot_dau_txns") }}
        GROUP BY 1
    )
    , spot_fees_revenue AS (
        SELECT date, SUM(fees_usd) AS fees_usd, SUM(protocol_fee_share_usd) AS protocol_fee_share_usd
        FROM {{ ref("fact_bluefin_spot_fees_revenue") }}
        GROUP BY 1
    )
    , tvl AS (
        SELECT date, SUM(pool_tvl) AS tvl
        FROM {{ ref("fact_bluefin_spot_tvl") }}
        GROUP BY 1
    )
    , market_data AS ({{ get_coingecko_metrics("bluefin") }})
select
    date
    , 'bluefin' AS app
    , 'DeFi' AS category
    -- Standardized Metrics

    --Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    --Cashflow Metrics
    , COALESCE(spot_fees_revenue.fees_usd, 0) AS fees
    , COALESCE(spot_fees_revenue.protocol_fee_share_usd, 0) AS foundation_fee_allocation
    , COALESCE((spot_fees_revenue.fees_usd - spot_fees_revenue.protocol_fee_share_usd), 0) AS service_fee_allocation
    
    -- Perpetual Metrics
    , COALESCE(perp_trading_volume.trading_volume, 0) AS perp_volume

    -- Spot DEX Metrics
    , COALESCE(spot_dau_txns.dau, 0) AS spot_dau
    , COALESCE(spot_dau_txns.txns, 0) AS spot_txns
    , COALESCE(spot_fees_revenue.fees_usd, 0) AS spot_fees
    , COALESCE(spot_trading_volume.spot_dex_volumes, 0) AS spot_volume

    -- Crypto Metrics
    , COALESCE(tvl.tvl, 0) AS tvl
    , COALESCE(tvl.tvl, 0) - COALESCE(LAG(tvl.tvl) OVER (ORDER BY date), 0) AS tvl_net_change

    -- Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv
FROM date_spine
LEFT JOIN perp_trading_volume USING(date)
LEFT JOIN spot_trading_volume USING(date)
LEFT JOIN spot_dau_txns USING(date)
LEFT JOIN spot_fees_revenue USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN market_data USING(date)
