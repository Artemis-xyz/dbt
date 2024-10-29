-- depends_on {{ ref("ez_bsc_transactions") }}
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
    fundamental_data as ({{ get_fundamental_data_for_chain("bsc") }}),
    price_data as ({{ get_coingecko_metrics("binancecoin") }}),
    defillama_data as ({{ get_defillama_metrics("bsc") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("bsc") }}),
    github_data as ({{ get_github_metrics("Binance Smart Chain") }}),
    contract_data as ({{ get_contract_metrics("bsc") }}),
    nft_metrics as ({{ get_nft_metrics("bsc") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("bsc") }})
select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    wau,
    mau,
    fees_native,
    fees,
    avg_txn_fee,
    median_txn_fee,
    fees_native * .1 as revenue_native,
    fees * .1 as revenue,
    sybil_users,
    non_sybil_users,
    returning_users,
    new_users,
    low_sleep_users,
    high_sleep_users,
    dau_over_100,
    price,
    market_cap,
    fdmc,
    tvl,
    dex_volumes,
    weekly_commits_core_ecosystem,
    weekly_commits_sub_ecosystem,
    weekly_developers_core_ecosystem,
    weekly_developers_sub_ecosystem,
    weekly_contracts_deployed,
    weekly_contract_deployers,
    stablecoin_total_supply,
    stablecoin_txns,
    stablecoin_dau,
    stablecoin_mau,
    stablecoin_transfer_volume,
    artemis_stablecoin_txns,
    artemis_stablecoin_dau,
    artemis_stablecoin_mau,
    artemis_stablecoin_transfer_volume,
    stablecoin_tokenholder_count,
    p2p_stablecoin_tokenholder_count,
    p2p_stablecoin_txns,
    p2p_stablecoin_dau,
    p2p_stablecoin_mau,
    p2p_stablecoin_transfer_volume,
    nft_trading_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
where fundamental_data.date < to_date(sysdate())
