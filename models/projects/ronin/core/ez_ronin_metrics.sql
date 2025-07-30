{{
    config(
        materialized="incremental",
        snowflake_warehouse="RONIN",
        database="ronin",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

WITH 
    date_spine AS (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2021-11-01'
        AND date < to_date(sysdate())
    )
    , ronin_dex_volumes AS (
        SELECT 
            date
            , daily_volume AS dex_volumes
            , daily_volume_adjusted AS adjusted_dex_volumes
        FROM {{ ref("fact_ronin_daily_dex_volumes") }}
    )
    , market_data AS ({{ get_coingecko_metrics("ronin") }})
select
    date_spine.date
    , 'ronin' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , dex_volumes AS chain_spot_volume
    , adjusted_dex_volumes AS chain_spot_volume_adjusted

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN ronin_dex_volumes USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
