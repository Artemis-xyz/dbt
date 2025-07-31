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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
    ink_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_ink_daily_dex_volumes") }}
    )
select
    fundamental_data.date
    , 'ink' as artemis_id

    -- Usage Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , ink_dex_volumes.dex_volumes as chain_spot_volume
    , ink_dex_volumes.adjusted_dex_volumes as adjusted_dex_volumes

    -- Cashflow Metrics
    , fundamental_data.fees_native as fees_native
    , fundamental_data.fees as fees
    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from {{ ref("fact_ink_fundamental_metrics") }} fundamental_data
left join ink_dex_volumes using (date)
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
