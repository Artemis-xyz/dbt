{{
    config(
        materialized="incremental",
        snowflake_warehouse="UNICHAIN",
        database="unichain",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
     price_data as ({{ get_coingecko_metrics('uniswap') }})
     , unichain_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_unichain_daily_dex_volumes") }}
    )
select
    f.date
    , dune_dex_volumes_unichain.dex_volumes
    , dune_dex_volumes_unichain.adjusted_dex_volumes
    -- Old Metrics Needed For Compatibility
    , txns
    , daa as dau
    , fees
    , fees_native
    , cost
    , cost_native
    , revenue
    , revenue_native

    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Chain Metrics
    , txns as chain_txns
    , daa as chain_dau
    , dune_dex_volumes_unichain.dex_volumes as chain_spot_volume
    -- Cash Flow Metrics
    , fees as ecosystem_revenue
    , fees_native as ecosystem_revenue_native
    , cost as l1_fee_allocation
    , cost_native as l1_fee_allocation_native
    , revenue as foundation_fee_allocation
    , revenue_native as foundation_fee_allocation_native
    , token_turnover_circulating
    , token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_unichain_fundamental_metrics") }} as f
left join price_data on f.date = price_data.date
left join unichain_dex_volumes as dune_dex_volumes_unichain on f.date = dune_dex_volumes_unichain.date
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())
