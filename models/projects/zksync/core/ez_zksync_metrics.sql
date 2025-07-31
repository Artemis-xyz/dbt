--depends_on {{ ref("fact_zksync_rolling_active_addresses") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="ZKSYNC",
        database="zksync",
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
    fundamental_data as (
        select
            date,
            chain,
            daa as dau,
            txns,
            gas,
            gas_usd,
            median_gas_usd as median_txn_fee
        from {{ ref("fact_zksync_daa_txns_gas_gas_usd") }}
    ),
    rolling_metrics as ({{ get_rolling_active_address_metrics("zksync") }}),
    revenue_data as (
        select date, revenue, revenue_native, l1_data_cost, l1_data_cost_native
        from {{ ref("fact_zksync_revenue") }}
    ),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_zksync_era_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_zksync_era_bridge_bridge_daa") }}
    ),
    zksync_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_zksync_daily_dex_volumes") }}
    )
    , supply_data as (
        select
            date
            , gross_emissions_native
            , premine_unlocks_native
            , burns_native
            , net_supply_change_native
            , circulating_supply_native
        from {{ ref("fact_zksync_supply_data") }}
    )
    , market_metrics as (
        {{ get_coingecko_metrics('zksync') }}
    )
select
    fundamental_data.date
    , 'zksync' as artemis_id
    , fundamental_data.chain
    ,

    -- Standardized Metrics
    
    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee
    , fundamental_data.median_txn_fee as chain_median_txn_fee
    , zksync_dex_volumes.dex_volumes as chain_spot_volume
    , zksync_dex_volumes.adjusted_dex_volumes as adjusted_dex_volumes

    -- Fee Data
    , fundamental_data.gas_usd as fees
    , fundamental_data.gas as fees_native
    , revenue_data.l1_data_cost as l1_fee_allocation
    , revenue_data.l1_data_cost_native as l1_fee_allocation_native
    , revenue_data.revenue as foundation_fee_allocation
    , revenue_data.revenue_native as foundation_fee_allocation_native
    
    -- Financial Statement
    , revenue_data.revenue_native as revenue_native
    , revenue_data.revenue as revenue

    -- Bridge Metrics
    , bridge_volume_metrics.bridge_volume as bridge_volume
    , bridge_daa_metrics.bridge_daa as bridge_dau
    
    -- Token Turnover/Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

    -- Supply Data
    , supply_data.gross_emissions_native
    , supply_data.premine_unlocks_native
    , supply_data.burns_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from fundamental_data
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join bridge_volume_metrics on fundamental_data.date = bridge_volume_metrics.date
left join bridge_daa_metrics on fundamental_data.date = bridge_daa_metrics.date
left join zksync_dex_volumes as dune_dex_volumes_zksync on fundamental_data.date = dune_dex_volumes_zksync.date
left join market_metrics on fundamental_data.date = market_metrics.date
left join supply_data on fundamental_data.date = supply_data.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
