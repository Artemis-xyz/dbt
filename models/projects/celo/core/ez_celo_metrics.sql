{{
    config(
        materialized="incremental",
        snowflake_warehouse="CELO",
        database="celo",
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
    fundamental_data as (
        select
            date, chain, gas_usd as fees, revenue, txns, dau, avg_txn_fee
        from {{ ref("fact_celo_dau_txns_gas_usd_revenue_avg_txn_fee") }}
    ),
    price_data as ({{ get_coingecko_metrics("celo") }}),
    defillama_data as ({{ get_defillama_metrics("celo") }}),
    github_data as ({{ get_github_metrics("celo") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("celo") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("celo") }}),
    celo_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_celo_daily_dex_volumes") }}
    )
select
    fundamental_data.date
    , 'celo' as artemis_id

    -- Standardized Metrics
    -- Market Data 
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , txns AS chain_txns
    , txns
    , dau AS chain_dau
    , dau
    , wau AS chain_wau
    , mau AS chain_mau
    , avg_txn_fee AS chain_avg_txn_fee
    , celo_dex_volumes.dex_volumes AS chain_spot_volume

    -- Cashflow metrics
    , fees AS chain_fees
    , fees
    , revenue AS burned_fee_allocation

    -- Financial Metrics
    , revenue

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

    -- Stablecoin Metrics
    , stablecoin_total_supply as stablecoin_supply
    , stablecoin_txns
    , stablecoin_dau
    , stablecoin_mau
    , stablecoin_transfer_volume
    , stablecoin_tokenholder_count
    , artemis_stablecoin_txns
    , artemis_stablecoin_dau
    , artemis_stablecoin_mau
    , artemis_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , p2p_stablecoin_transfer_volume

    -- Legacy Metrics
    , celo_dex_volumes.adjusted_dex_volumes

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join github_data on fundamental_data.date = github_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join celo_dex_volumes on fundamental_data.date = celo_dex_volumes.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())