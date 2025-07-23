{{
    config(
        materialized="incremental",
        snowflake_warehouse="KAIA",
        database="kaia",
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

{% set backfill_date = var("backfill_date", None) %}

with 
    kaia_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_kaia_daily_dex_volumes") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ), 
    price_data as ({{ get_coingecko_metrics("kaia") }})
select
    date
    , kaia_dex_volumes.dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Metrics
    , kaia_dex_volumes.dex_volumes AS chain_spot_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from kaia_dex_volumes   
left join price_data using (date)
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())