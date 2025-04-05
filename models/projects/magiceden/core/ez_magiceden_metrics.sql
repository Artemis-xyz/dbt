{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN",
        database="magiceden",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH date_spine AS (
    SELECT * 
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN '2020-03-16' AND TO_DATE(SYSDATE())
)
, magiceden_metrics AS (
    SELECT
        date,
        SUM(daily_trading_volume) AS daily_trading_volume,
        SUM(active_wallets) AS dau, 
        SUM(collections_transacted) AS collections_transacted,
        SUM(total_trades) AS txns,
        SUM(total_platform_fees) AS revenue,
        SUM(total_creator_fees) AS supply_side_fees,
        SUM(total_fees_usd) AS fees
    FROM
        {{ ref('fact_magiceden_metrics_by_chain') }}
    GROUP BY
        date
)
, market_data as (
    {{ get_coingecko_metrics('magic-eden') }}
)
SELECT
    date_spine.date
    , COALESCE(m.daily_trading_volume, 0) AS daily_trading_volume
    , COALESCE(m.dau, 0) AS dau 
    , COALESCE(m.collections_transacted, 0) AS collections_transacted
    , COALESCE(m.txns, 0) AS txns
    , COALESCE(m.revenue, 0) AS revenue
    , COALESCE(m.supply_side_fees, 0) AS supply_side_fees
    , COALESCE(m.fees, 0) AS fees

    -- Standardized Metrics

    -- Token Metrics
    , COALESCE(md.price, 0) AS price
    , COALESCE(md.market_cap, 0) AS market_cap
    , COALESCE(md.fdmc, 0) AS fdmc
    , COALESCE(md.token_volume, 0) AS token_volume

    -- NFT Metrics
    , COALESCE(m.dau, 0) AS nft_dau
    , COALESCE(m.txns, 0) AS nft_txns
    , COALESCE(m.collections_transacted, 0) AS nft_collections_transacted
    , COALESCE(m.supply_side_fees, 0) AS nft_royalties
    , COALESCE(m.fees, 0) AS nft_fees
    , COALESCE(m.daily_trading_volume, 0) AS nft_volume

    -- Cash Flow Metrics
    , COALESCE(m.fees, 0) AS gross_protocol_revenue
    , COALESCE(m.supply_side_fees, 0) AS service_cash_flow

    -- Turnover Metrics
    , COALESCE(md.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(md.token_turnover_fdv, 0) AS token_turnover_fdv
FROM date_spine
LEFT JOIN magiceden_metrics m ON date_spine.date = m.date
LEFT JOIN market_data md ON date_spine.date = md.date
WHERE date_spine.date < to_date(SYSDATE())
ORDER BY date_spine.date