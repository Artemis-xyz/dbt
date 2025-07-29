{{
    config(
        materialized="incremental",
        snowflake_warehouse="BOBA",
        database="boba",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
    boba_dex_volumes as (
        select date, coalesce(daily_volume, 0) as dex_volumes, coalesce(daily_volume_adjusted, 0) as adjusted_dex_volumes
        from {{ ref("fact_boba_daily_dex_volumes") }}
    )
    , market_metrics as ({{ get_coingecko_metrics('boba-network') }})
select
    date
    , 'boba' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , dex_volumes as chain_spot_volume
    , adjusted_dex_volumes

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from boba_dex_volumes
left join market_metrics using(date)
where true 
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
