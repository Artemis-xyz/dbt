{{
    config(
        materialized="table",
        snowflake_warehouse="CELO",
        database="celo",
        schema="core",
        alias="ez_metrics",
    )
}}

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
    , fundamental_data.chain
    , txns
    , dau
    , wau
    , mau
    , fees
    , revenue
    , avg_txn_fee
    , celo_dex_volumes.dex_volumes
    , celo_dex_volumes.adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , txns AS chain_txns
    , dau AS chain_dau
    , wau AS chain_wau
    , mau AS chain_mau
    , avg_txn_fee AS chain_avg_txn_fee
    , celo_dex_volumes.dex_volumes AS chain_spot_volume
    -- Cashflow metrics
    , fees AS chain_fees
    , fees AS ecosystem_revenue
    , revenue AS burned_fee_allocation
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    -- Stablecoin Metrics
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
left join github_data on fundamental_data.date = github_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join celo_dex_volumes on fundamental_data.date = celo_dex_volumes.date
where fundamental_data.date < to_date(sysdate())