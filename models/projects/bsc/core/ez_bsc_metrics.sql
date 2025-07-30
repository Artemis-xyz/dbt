-- depends_on {{ ref("fact_bsc_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="BSC_SM",
        database="bsc",
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
    fundamental_data as ({{ get_fundamental_data_for_chain("bsc", "v2") }}),
    price_data as ({{ get_coingecko_metrics("binancecoin") }}),
    defillama_data as ({{ get_defillama_metrics("bsc") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("bsc") }}),
    github_data as ({{ get_github_metrics("Binance Smart Chain") }}),
    contract_data as ({{ get_contract_metrics("bsc") }}),
    nft_metrics as ({{ get_nft_metrics("bsc") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("bsc") }}),
    binance_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_binance_daily_dex_volumes") }}
    )
    , staked_eth_metrics as (
        select
            date,
            sum(num_staked_eth) as num_staked_eth,
            sum(amount_staked_usd) as amount_staked_usd,
            sum(num_staked_eth_net_change) as num_staked_eth_net_change,
            sum(amount_staked_usd_net_change) as amount_staked_usd_net_change
        from {{ ref('fact_binance_staked_eth_count_with_usd_and_change') }}
        group by 1
    )
select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees_native
    , fees
    , avg_txn_fee
    , median_txn_fee
    , fees_native * .1 as revenue_native
    , fees * .1 as revenue
    , dau_over_100
    , nft_trading_volume
    , dune_dex_volumes_binance.dex_volumes
    , dune_dex_volumes_binance.adjusted_dex_volumes
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
    , median_txn_fee AS chain_median_txn_fee
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    , sybil_users
    , non_sybil_users
    , dau_over_100 AS dau_over_100_balance
    , nft_trading_volume AS chain_nft_trading_volume
    , dune_dex_volumes_binance.dex_volumes AS chain_spot_volume
    -- LST Metrics
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    -- Cashflow metrics
    , fees as chain_fees
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , fees_native * .1 AS burned_fee_allocation_native
    , fees * .1 AS burned_fee_allocation
    -- Developer metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
    -- Stablecoin metrics
    , stablecoin_total_supply
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
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join binance_dex_volumes as dune_dex_volumes_binance on fundamental_data.date = dune_dex_volumes_binance.date
left join staked_eth_metrics on fundamental_data.date = staked_eth_metrics.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
    and fundamental_data.date < to_date(sysdate())
