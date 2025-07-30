{{
    config(
        materialized="incremental",
        snowflake_warehouse="GEODNET",
        database="geodnet",
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
        WHERE date >= '2023-04-20'
        AND date < to_date(sysdate())
    )
    , revenue_data AS (
        SELECT 
            date
            , fees
            , revenue
            , protocol
        FROM {{ ref("fact_geodnet_fees_revenue") }}
    )
    , market_data AS ({{ get_coingecko_metrics("geodnet") }})
SELECT
    date_spine.date
    , 'geodnet' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Fee Data
    , revenue_data.fees
    , revenue_data.fees * 0.8 AS buyback_fee_allocation
    , revenue_data.fees * 0.2 AS foundation_fee_allocation

    -- Financial Statements
    , revenue_data.revenue

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN revenue_data USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())
