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
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("starknet") }}),
    market_metrics as ({{ get_coingecko_metrics("starknet") }}),
    defillama_data as ({{ get_defillama_metrics("starknet") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_starknet_l1_data_cost") }}
    ),
    github_data as ({{ get_github_metrics("starknet") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("starknet") }}),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_starknet_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_starknet_bridge_bridge_daa") }}
    ),
    fees_data as (
        select date
        , fees_native
        from {{ ref("fact_starknet_fees") }}
    )
    , supply_data as (
        select date, gross_emissions_native, premine_unlocks_native, burns_native, net_supply_change_native, circulating_supply_native
        from {{ ref("fact_starknet_supply_data") }}
    )

select
    fundamental_data.date
    , 'starknet' as artemis_id
    
    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.tvl

    -- Usage Data
    , fundamental_data.dau as chain_dau
    , fundamental_data.dau as dau
    , fundamental_data.wau as chain_wau
    , fundamental_data.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee
    , fundamental_data.dex_volumes as chain_spot_volume
    , fundamental_data.median_txn_fee as chain_median_txn_fee
    , fundamental_data.returning_users
    , fundamental_data.new_users

    -- Fee Data
    , fees_data.fees_native as fees_native
    , fees_data.fees as chain_fees
    , fees_data.fees as fees
    , l1_data_cost_native as l1_fee_allocation_native
    , l1_data_cost as l1_fee_allocation
    , coalesce(fees_data.fees_native, 0) - l1_data_cost_native as equity_fee_allocation_native
    , coalesce(fees, 0) -  l1_data_cost as equity_fee_allocation

    -- Bridge Metrics
    , bridge_volume_metrics.bridge_volume
    , bridge_daa_metrics.bridge_daa

    -- Developer Metrics
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem

    -- Supply Metrics
    , supply_data.premine_unlocks_native
    , supply_data.gross_emissions_native
    , supply_data.burns_native
    , supply_data.net_supply_change_native
    , supply_data.circulating_supply_native

    -- Market Data
    , market_metrics.token_turnover_fdv
    , market_metrics.token_turnover_circulating

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from fundamental_data
left join market_metrics on fundamental_data.date = market_metrics.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join github_data on fundamental_data.date = github_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join bridge_volume_metrics on fundamental_data.date = bridge_volume_metrics.date
left join bridge_daa_metrics on fundamental_data.date = bridge_daa_metrics.date
left join fees_data on fundamental_data.date = fees_data.date
left join supply_data on fundamental_data.date = supply_data.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
