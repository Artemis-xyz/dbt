{{
    config(
        materialized="incremental",
        snowflake_warehouse="LEVEL_FINANCE",
        database="level_finance",
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
        WHERE date >= '2022-12-10'
        AND date < to_date(sysdate())
    )
    , trading_volume_data AS (
        SELECT
            date
            , sum(trading_volume) AS trading_volume
        FROM {{ ref("fact_level_finance_trading_volume") }}
        GROUP BY date
    )
    , unique_traders_data AS (
        SELECT
            date
            , sum(unique_traders) AS unique_traders
        FROM {{ ref("fact_level_finance_unique_traders") }}
        GROUP BY date
    )
    , market_data AS ({{ get_coingecko_metrics("level") }})
select
    date
    , 'level-finance' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Usage Data
    , unique_traders as perp_dau 
    , unique_traders as dau
    , trading_volume as perp_volume

    -- Turnover Data
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) AS modified_on
FROM date_spine
LEFT JOIN trading_volume_data USING (date)
LEFT JOIN unique_traders_data USING (date)
LEFT JOIN market_data USING (date)
where true
{{ ez_metrics_incremental("date_spine.date", backfill_date) }}
AND date < to_date(sysdate())
