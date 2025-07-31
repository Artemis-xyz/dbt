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
        full_refresh=false,
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
    , sysdate() as created_on
    , sysdate() as modified_on
from {{ ref("fact_ink_fundamental_metrics") }}
left join ink_dex_volumes using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
