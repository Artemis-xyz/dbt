{{
    config(
        materialized="incremental",
        snowflake_warehouse="INK",
        database="ink",
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
    ink_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_ink_daily_dex_volumes") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
    , ink_dex_volumes.dex_volumes
    , ink_dex_volumes.adjusted_dex_volumes
    -- Standardized Metrics
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , ink_dex_volumes.dex_volumes AS chain_spot_volume
    -- Cashflow Metrics
    , fees AS ecosystem_revenue
    , fees_native AS ecosystem_revenue_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_ink_fundamental_metrics") }}
left join ink_dex_volumes using (date)
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
