{{
    config(
        materialized="table",
        snowflake_warehouse="CETUS",
        database="cetus",
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
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_raw_cetus_spot_swaps") }})
    )
    , spot_trading_volume AS (
        SELECT date, 'sui' AS chain, SUM(volume_usd) AS spot_dex_volumes
        FROM {{ ref("fact_cetus_spot_volume") }}
        GROUP BY 1, 2
    )
    , spot_dau_txns AS (
        SELECT date, 'sui' AS chain, SUM(dau) AS dau, SUM(txns) AS txns
        FROM {{ ref("fact_cetus_spot_dau_txns") }}
        GROUP BY 1, 2
    )
    , spot_fees_revenue AS (
        SELECT date, 'sui' AS chain, SUM(fees_usd) AS fees_usd, SUM(service_cash_flow) AS service_cash_flow, SUM(foundation_cash_flow) AS foundation_cash_flow
        FROM {{ ref("fact_cetus_spot_fees_revenue") }}
        GROUP BY 1, 2
    )
    , tvl AS (
        SELECT date, 'sui' AS chain, SUM(pool_tvl) AS tvl
        FROM {{ ref("fact_cetus_spot_tvl") }}
        GROUP BY 1, 2
    )
    , market_data AS ({{ get_coingecko_metrics("cetus-protocol") }})
select
    date
    , 'cetus' as app
    , 'DeFi' as category
    , date_spine.chain 

    -- Standardized Metrics

    --Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    --Cashflow Metrics
    , COALESCE(spot_fees_revenue.fees_usd, 0) AS ecosystem_revenue
    , COALESCE(spot_fees_revenue.foundation_cash_flow, 0) AS foundation_cash_flow
    , COALESCE(spot_fees_revenue.service_cash_flow, 0) AS service_cash_flow
    
    -- Spot DEX Metrics
    , COALESCE(spot_dau_txns.dau, 0) AS spot_dau
    , COALESCE(spot_dau_txns.txns, 0) AS spot_txns
    , COALESCE(spot_fees_revenue.fees_usd, 0) AS spot_fees
    , COALESCE(spot_trading_volume.spot_dex_volumes, 0) AS spot_volume

    -- Crypto Metrics
    , COALESCE(tvl.tvl, 0) AS tvl
    , COALESCE(tvl.tvl, 0) - COALESCE(LAG(tvl.tvl) OVER (ORDER BY date), 0) AS tvl_net_change

    -- Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circul
FROM date_spine
LEFT JOIN spot_trading_volume USING(date, chain)
LEFT JOIN spot_dau_txns USING(date, chain)
LEFT JOIN spot_fees_revenue USING(date, chain)
LEFT JOIN tvl USING(date, chain)
LEFT JOIN market_data USING(date)