{{
    config(
        materialized="table",
        snowflake_warehouse="CETUS",
        database="cetus",
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
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_raw_cetus_spot_swaps") }})
    )
    , spot_trading_volume AS (
        SELECT date, SUM(volume_usd) AS spot_dex_volumes
        FROM {{ ref("fact_cetus_spot_volume") }}
        GROUP BY 1
    )
    , spot_dau_txns AS (
        SELECT date, daily_dau AS dau, daily_txns AS txns
        FROM {{ ref("fact_cetus_spot_dau_txns") }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY date DESC) = 1
    )
    , spot_fees_revenue AS (
        SELECT date, SUM(fees) AS fees, SUM(service_cash_flow) AS service_cash_flow, SUM(foundation_cash_flow) AS foundation_cash_flow
        FROM {{ ref("fact_cetus_spot_fees_revenue") }}
        GROUP BY 1
    )
    , tvl AS (
        SELECT date, SUM(tvl) AS tvl
        FROM {{ ref("fact_cetus_spot_tvl") }}
        GROUP BY 1
    )
    , market_data AS ({{ get_coingecko_metrics("cetus-protocol") }})
select
    date
    , 'cetus' AS app
    , 'DeFi' AS category
    -- Standardized Metrics

    --Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    --Cashflow Metrics
    , COALESCE(spot_fees_revenue.fees, 0) AS fees
    , COALESCE(spot_fees_revenue.foundation_cash_flow, 0) AS foundation_cash_flow
    , COALESCE(spot_fees_revenue.service_cash_flow, 0) AS service_cash_flow
    
    -- Spot DEX Metrics
    , COALESCE(spot_dau_txns.dau, 0) AS spot_dau
    , COALESCE(spot_dau_txns.txns, 0) AS spot_txns
    , COALESCE(spot_fees_revenue.fees, 0) AS spot_fees
    , COALESCE(spot_trading_volume.spot_dex_volumes, 0) AS spot_volume

    -- Crypto Metrics
    , COALESCE(tvl.tvl, 0) AS tvl
    , COALESCE(tvl.tvl, 0) - COALESCE(LAG(tvl.tvl) OVER (ORDER BY date), 0) AS tvl_net_change

    -- Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv
FROM date_spine
LEFT JOIN spot_trading_volume USING(date)
LEFT JOIN spot_dau_txns USING(date)
LEFT JOIN spot_fees_revenue USING(date)
LEFT JOIN tvl USING(date)
LEFT JOIN market_data USING(date)