{{
    config(
        materialized="incremental",
        snowflake_warehouse="AVANTIS",
        database="avantis",
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
        WHERE date >= '2024-01-27'
        AND date < to_date(sysdate())
    )
    , trading_volume_data AS (
        SELECT 
            date
            , SUM(trading_volume) AS trading_volume
        FROM {{ ref("fact_avantis_trading_volume_silver") }}
        GROUP BY date
    )
    , unique_traders_data AS (
        SELECT 
            date
            , SUM(unique_traders) AS unique_traders
        FROM {{ ref("fact_avantis_unique_traders_silver") }}
        GROUP BY date
    )
    
SELECT
    date_spine.date
    , 'avantis' as artemis_id

    -- Standardized Metrics

    -- Usage Data
    , unique_traders_data.unique_traders as perp_dau
    , unique_traders_data.unique_traders as dau
    , trading_volume_data.trading_volume as perp_volume

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN trading_volume_data USING (date)
LEFT JOIN unique_traders_data USING (date)
WHERE true
{{ ez_metrics_incremental("date_spine.date", backfill_date) }}
AND date_spine.date < to_date(sysdate())
