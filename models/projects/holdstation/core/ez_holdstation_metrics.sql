{{
    config(
        materialized="incremental",
        snowflake_warehouse="HOLDSTATION",
        database="holdstation",
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

with
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2023-05-09' AND date < TO_DATE(SYSDATE())
    )
    , zk_sync_volume_data AS (
        SELECT date, SUM(trading_volume) AS trading_volume
        FROM {{ ref("fact_holdstation_trading_volume") }}
        GROUP BY date
    )
    , bera_trading_volume_data AS (
        SELECT date, SUM(perp_volume) AS perp_volume
        FROM {{ ref("fact_holdstation_bera_perp_volume") }}
        GROUP BY date
    )
    , agg_volume_data AS (
        SELECT date, SUM(trading_volume) AS perp_volume
        FROM zk_sync_volume_data
        GROUP BY date
        UNION ALL
        SELECT date, SUM(perp_volume) AS perp_volume
        FROM bera_trading_volume_data
        GROUP BY date
    )
    , unique_traders_data AS (
        SELECT date, SUM(unique_traders) AS unique_traders
        FROM {{ ref("fact_holdstation_unique_traders") }}
        GROUP BY date
    )
    , market_data AS ({{ get_coingecko_metrics("holdstation") }})
    
SELECT
    date_spine.date
    , 'holdstation' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , unique_traders_data.unique_traders AS perp_dau
    , unique_traders_data.unique_traders AS dau
    , agg_volume_data.perp_volume

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN agg_volume_data USING (date)
LEFT JOIN unique_traders_data USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental('agg_volume_data.date', backfill_date) }}
AND date < TO_DATE(SYSDATE())
