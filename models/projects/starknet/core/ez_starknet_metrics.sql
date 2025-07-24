-- depends_on {{ ref("ez_starknet_transactions") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="STARKNET",
        database="starknet",
        schema="core",
        alias="ez_metrics",
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
    fundamental_data as ({{ get_fundamental_data_for_chain("starknet") }}),
    price_data as ({{ get_coingecko_metrics("starknet") }}),
    defillama_data as ({{ get_defillama_metrics("starknet") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_starknet_l1_data_cost") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    github_data as ({{ get_github_metrics("starknet") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("starknet") }}),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_starknet_bridge_bridge_volume") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        and chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_starknet_bridge_bridge_daa") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    ),
    fees_data as (
        select date
        , fees_native
        from {{ ref("fact_starknet_fees") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , supply_data as (
        select date, gross_emissions_native, premine_unlocks_native, burns_native, net_supply_change_native, circulating_supply_native
        from {{ ref("fact_starknet_supply_data") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )

select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees
    , fees_data.fees_native
    , l1_data_cost_native
    , l1_data_cost
    , coalesce(fees_data.fees_native, 0) - l1_data_cost_native as revenue_native
    , coalesce(fees, 0) -  l1_data_cost as revenue
    , avg_txn_fee
    , median_txn_fee
    , dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , txns AS chain_txns
    , avg_txn_fee AS chain_avg_txn_fee
    , dex_volumes AS chain_spot_volume
    , returning_users
    , new_users
    -- Cashflow Metrics
    , fees as chain_fees
    , fees AS ecosystem_revenue
    , fees_data.fees_native AS ecosystem_revenue_native
    , median_txn_fee AS chain_median_txn_fee
    , coalesce(fees_data.fees_native, 0) - l1_data_cost_native as equity_fee_allocation_native
    , coalesce(fees, 0) -  l1_data_cost as equity_fee_allocation
    , l1_data_cost_native AS l1_fee_allocation_native
    , l1_data_cost AS l1_fee_allocation
    -- Bridge Metrics,
    , bridge_volume
    , bridge_daa
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- Supply Metrics
    , premine_unlocks_native
    , gross_emissions_native
    , burns_native
    , net_supply_change_native
    , circulating_supply_native

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join bridge_volume_metrics on fundamental_data.date = bridge_volume_metrics.date
left join bridge_daa_metrics on fundamental_data.date = bridge_daa_metrics.date
left join fees_data on fundamental_data.date = fees_data.date
left join supply_data on fundamental_data.date = supply_data.date
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
