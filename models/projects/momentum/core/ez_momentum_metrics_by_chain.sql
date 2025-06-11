{{
    config(
        materialized="table",
        snowflake_warehouse="MOMENTUM",
        database="momentum",
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
            AND date >= (SELECT MIN(date) FROM {{ ref("fact_raw_momentum_spot_swaps") }})
    )
    , spot_trading_volume AS (
        SELECT date, 'sui' AS chain, SUM(volume_usd) AS spot_dex_volumes
        FROM {{ ref("fact_momentum_spot_volume") }}
        GROUP BY 1, 2
    )
    , spot_dau_txns AS (
        SELECT date, 'sui' AS chain, daily_dau AS dau, daily_txns AS txns
        FROM {{ ref("fact_momentum_spot_dau_txns") }}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY date DESC) = 1
    )
    , spot_fees_revenue AS (
        SELECT date, 'sui' AS chain, SUM(fees) AS fees, SUM(service_fee_allocation) AS service_fee_allocation, SUM(foundation_fee_allocation) AS foundation_fee_allocation
        FROM {{ ref("fact_momentum_spot_fees_revenue") }}
        GROUP BY 1, 2
    )
    , tvl AS (
        SELECT date, 'sui' AS chain, SUM(tvl) AS tvl
        FROM {{ ref("fact_momentum_spot_tvl") }}
        GROUP BY 1, 2
    )
select
    date
    , chain
    , 'momentum' as app
    , 'DeFi' as category

    -- Standardized Metrics

    --Cashflow Metrics
    , COALESCE(spot_fees_revenue.fees, 0) AS fees
    , COALESCE(spot_fees_revenue.service_fee_allocation, 0) AS service_fee_allocation
    , COALESCE(spot_fees_revenue.foundation_fee_allocation, 0) AS foundation_fee_allocation

    -- Spot DEX Metrics
    , COALESCE(spot_dau_txns.dau, 0) AS spot_dau
    , COALESCE(spot_dau_txns.txns, 0) AS spot_txns
    , COALESCE(spot_fees_revenue.fees, 0) AS spot_fees
    , COALESCE(spot_trading_volume.spot_dex_volumes, 0) AS spot_volume

    -- Crypto Metrics
    , COALESCE(tvl.tvl, 0) AS tvl
FROM date_spine
LEFT JOIN spot_trading_volume USING(date, chain)
LEFT JOIN spot_dau_txns USING(date, chain)
LEFT JOIN spot_fees_revenue USING(date, chain)
LEFT JOIN tvl USING(date, chain)