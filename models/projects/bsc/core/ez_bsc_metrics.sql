-- depends_on {{ ref("ez_bsc_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BSC_SM",
        database="bsc",
        schema="core",
        alias="ez_metrics",
    )
}}

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
        select date, daily_volume as dex_volumes
        from {{ ref("fact_binance_daily_dex_volumes") }}
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
    , dune_dex_volumes_binance.dex_volumes AS chain_dex_volumes
    -- Cashflow metrics
    , fees as chain_fees
    , fees_native AS gross_protocol_revenue_native
    , fees AS gross_protocol_revenue
    , fees_native * .1 AS burned_cash_flow_native
    , fees * .1 AS burned_cash_flow
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
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join binance_dex_volumes as dune_dex_volumes_binance on fundamental_data.date = dune_dex_volumes_binance.date
where fundamental_data.date < to_date(sysdate())
