{{
    config(
        materialized="incremental",
        snowflake_warehouse="WORLDCHAIN",
        database="worldchain",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
     market_data as ({{ get_coingecko_metrics('worldcoin-wld') }})
    , dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_worldchain_daily_dex_volumes") }}
    )
select
    f.date
    , 'worldchain' as artemis_id

    -- Standardized Metrics
    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume
    -- Usage Metrics
    , f.daa as chain_dau
    , f.daa as dau
    , f.txns as chain_txns
    , f.txns
    , dex.dex_volumes as chain_spot_volume

    -- Fees Metrics
    , f.fees as chain_fees
    , f.fees
    , f.fees_native
    , f.cost as l1_fee_allocation
    , f.cost_native as l1_fee_allocation_native
    , f.revenue as foundation_fee_allocation
    , f.revenue_native as foundation_fee_allocation_native
    
    -- Financial Metrics
    , f.revenue
    , f.revenue_native

    -- Other Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- Legacy Metrics
    , dex.adjusted_dex_volumes
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from {{ ref("fact_worldchain_fundamental_metrics") }} as f
left join market_data on f.date = market_data.date
left join dex_volumes dex on f.date = dex.date
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())
