{{
    config(
        materialized="incremental",
        snowflake_warehouse="DYDX",
        database="dydx_v4",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2023-11-13'
        AND date < to_date(sysdate())
    )
    , trading_volume_data AS (
        SELECT 
            date
            , SUM(trading_volume) AS trading_volume
        FROM {{ ref("fact_dydx_v4_trading_volume") }}
        GROUP BY date
    )
    , fees_data AS (
        SELECT 
            date
            , maker_fees
            , taker_fees
            , fees
        FROM {{ ref("fact_dydx_v4_fees") }}
    )
    , chain_data AS (
        SELECT 
            date
            , maker_fees
            , maker_rebates
            , txn_fees
        FROM {{ ref("fact_dydx_v4_txn_fees") }}
    )
    , trading_fees AS (
        SELECT 
            date
            , total_fees
        FROM {{ ref("fact_dydx_v4_trading_fees") }}
    )
    , unique_traders_data AS (
        SELECT 
            date
            , SUM(unique_traders) AS unique_traders
        FROM {{ ref("fact_dydx_v4_unique_traders") }}
        GROUP BY date
    )
    , market_data AS ({{get_coingecko_metrics("dydx-chain")}})

SELECT 
    date_spine.date
    , 'dydx_v4' AS artemis_id

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data 
    , unique_traders_data.unique_traders AS perp_dau
    , unique_traders_data.unique_traders AS dau
    , trading_volume_data.trading_volume AS perp_volume

    -- Fee data
    , fees_data.maker_fees
    , fees_data.taker_fees
    , chain_data.txn_fees
    , fees_data.maker_fees + fees_data.taker_fees + chain_data.txn_fees AS fees
    , CASE WHEN unique_traders_data.date > '2025-03-25' THEN fees * 0.25 ELSE 0 END AS buybacks
    
    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv
    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN trading_volume_data USING (date)
LEFT JOIN fees_data USING (date)
LEFT JOIN chain_data USING (date)
LEFT JOIN unique_traders_data USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental("date_spine.date", backfill_date) }}
AND date_spine.date < to_date(sysdate())
