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
     market_metrics as ({{ get_coingecko_metrics('uniswap') }})
     , unichain_dex_volumes as (
        select date, coalesce(daily_volume, 0) as dex_volumes, coalesce(daily_volume_adjusted, 0) as adjusted_dex_volumes
        from {{ ref("fact_unichain_daily_dex_volumes") }}
    )
select
    fundamental_metrics.date
    , unichain_dex_volumes.dex_volumes

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , fundamental_metrics.txns as chain_txns
    , fundamental_metrics.daa as chain_dau
    , unichain_dex_volumes.dex_volumes as chain_spot_volume
    , unichain_dex_volumes.adjusted_dex_volumes

    -- Fee Data
    , fundamental_metrics.fees_native as fees_native
    , fundamental_metrics.fees as fees
    , fundamental_metrics.cost as l1_fee_allocation
    , fundamental_metrics.cost_native as l1_fee_allocation_native
    , fundamental_metrics.revenue as foundation_fee_allocation
    , fundamental_metrics.revenue_native as foundation_fee_allocation_native

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from {{ ref("fact_unichain_fundamental_metrics") }} as fundamental_metrics
left join market_metrics on fundamental_metrics.date = market_metrics.date
left join unichain_dex_volumes on fundamental_metrics.date = unichain_dex_volumes.date
where true
{{ ez_metrics_incremental('fundamental_metrics.date', backfill_date) }}
and fundamental_metrics.date < to_date(sysdate())
