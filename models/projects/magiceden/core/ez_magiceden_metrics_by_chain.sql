{{
    config(
        materialized="table",
        snowflake_warehouse="MAGICEDEN",
        database="magiceden",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

WITH date_spine AS (
    SELECT * 
    FROM {{ ref('dim_date_spine') }}
    WHERE date BETWEEN '2020-03-16' AND TO_DATE(SYSDATE())
)
, magiceden_metrics AS (
    SELECT
        date
        , chain
        , SUM(trading_volume) AS daily_trading_volume
        , SUM(active_wallets) AS dau 
        , SUM(total_trades) AS txns
        , SUM(total_platform_fees) AS revenue
        , SUM(total_creator_fees) AS supply_side_fees
        , SUM(total_fees_usd) AS fees
    FROM
        {{ ref('fact_magiceden_metrics_by_chain') }}
    GROUP BY
        date, 
        chain
)

SELECT 
    date_spine.date
    , chain
    , COALESCE(daily_trading_volume, 0) AS daily_trading_volume
    , COALESCE(dau, 0) AS dau 
    , COALESCE(txns, 0) AS txns
    , COALESCE(revenue, 0) AS revenue
    , COALESCE(supply_side_fees, 0) AS supply_side_fees

    -- Standardized Metrics

    -- NFT Metrics
    , COALESCE(dau, 0) AS nft_dau
    , COALESCE(txns, 0) AS nft_txns
    , COALESCE(supply_side_fees, 0) AS nft_royalties
    , COALESCE(fees, 0) AS nft_fees
    , COALESCE(daily_trading_volume, 0) AS nft_volume

    -- Cash Flow Metrics
    , COALESCE(fees, 0) AS fees
    , COALESCE(revenue, 0) AS treasury_fee_allocation
    , COALESCE(supply_side_fees, 0) AS service_fee_allocation  
FROM date_spine
LEFT JOIN magiceden_metrics m ON date_spine.date = m.date
WHERE date_spine.date < to_date(SYSDATE())
ORDER BY date_spine.date, chain
