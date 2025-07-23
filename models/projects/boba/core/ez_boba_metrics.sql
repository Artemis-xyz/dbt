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
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = var("backfill_date", None) %}

with 
    boba_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_boba_daily_dex_volumes") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    price_data as ({{ get_coingecko_metrics('boba-network') }})
select
    d.date
    , dex_volumes
    , adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , dex_volumes as chain_spot_volume
    , token_turnover_circulating
    , token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from boba_dex_volumes d
left join price_data using(d.date)
where d.date < to_date(sysdate())
