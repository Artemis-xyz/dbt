{{
    config(
        materialized="incremental",
        snowflake_warehouse="WHITEBIT",
        database="whitebit",
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
    date_spine as (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date >= '2022-08-05' AND date < to_date(sysdate())
    )
    , supply_data as (
        SELECT * FROM {{ ref("fact_whitebit_supply") }}
    )
    , revenue_data as (
        SELECT * FROM {{ ref("fact_whitebit_revenue") }}
    )
    , market_data AS (
        {{ get_coingecko_metrics("whitebit") }}
    )

SELECT 
    date_spine.date
    -- Standardized Metrics

    --Token Metrics
    , COALESCE(market_data.price, 0) AS price
    , COALESCE(market_data.market_cap, 0) AS market_cap
    , COALESCE(market_data.fdmc, 0) AS fdmc
    , COALESCE(market_data.token_volume, 0) AS token_volume

    -- Supply Metrics
    , COALESCE(supply_data.max_supply_native, 0) AS max_supply_native
    , COALESCE(supply_data.max_supply_native, 0) - COALESCE(supply_data.cumulative_burns_native, 0) - COALESCE(revenue_data.cumulative_revenue_native, 0) AS total_supply_native
    , CASE 
        WHEN date = '2022-08-05' THEN COALESCE(revenue_data.revenue_native, 0) + 25000000
        ELSE COALESCE(revenue_data.revenue_native, 0)
    END AS burns_native

    -- Turnover Metrics
    , COALESCE(market_data.token_turnover_circulating, 0) AS token_turnover_circulating
    , COALESCE(market_data.token_turnover_fdv, 0) AS token_turnover_fdv

    -- Timestamp Columns
    , sysdate() as created_on
    , sysdate() as modified_on
FROM date_spine
LEFT JOIN supply_data USING (date)
LEFT JOIN revenue_data USING (date)
LEFT JOIN market_data USING (date)
WHERE true
{{ ez_metrics_incremental('date_spine.date', backfill_date) }}
AND date_spine.date < to_date(sysdate())








-- 