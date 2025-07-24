{{
    config(
        materialized="incremental",
        snowflake_warehouse="POLYGON_ZK",
        database="polygon_zk",
        schema="core",
        alias="ez_metrics",
        enabled=false,
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as (
        select date, chain, daa as dau, txns, gas as fees_native, gas_usd as fees
        from {{ ref("fact_polygon_zk_daa_txns_gas_usd") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    price_data as ({{ get_coingecko_metrics("matic-network") }}),
    defillama_data as ({{ get_defillama_metrics("polygon zkevm") }}),
    l1_data_cost as (
        select
            date,
            l1_data_cost_native,
            l1_data_cost
        from {{ ref("fact_polygon_zk_l1_data_cost") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    github_data as ({{ get_github_metrics("Polygon Hermez") }}),
    polygon_zk_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_polygon_zk_daily_dex_volumes") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , l1_data_cost_native
    , l1_data_cost
    , fees
    , fees / txns as avg_txn_fee
    , coalesce(fees, 0) - l1_data_cost as revenue
    , dune_dex_volumes_polygon_zk.dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , fees / txns as chain_avg_txn_fee
    , dune_dex_volumes_polygon_zk.dex_volumes AS chain_spot_volume
    -- Cash Flow Metrics
    , fees AS chain_fees
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , coalesce(fees_native, 0) - l1_data_cost_native as service_fee_allocation_native
    , coalesce(fees, 0) - l1_data_cost as service_fee_allocation
    , l1_data_cost_native AS l1_fee_allocation_native
    , l1_data_cost AS l1_fee_allocation
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join l1_data_cost on fundamental_data.date = l1_data_cost.date
left join github_data on fundamental_data.date = github_data.date
left join polygon_zk_dex_volumes as dune_dex_volumes_polygon_zk on fundamental_data.date = dune_dex_volumes_polygon_zk.date
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
