-- depends_on {{ ref("ez_polygon_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="polygon",
        database="polygon",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("polygon") }}),
    price_data as ({{ get_coingecko_metrics("matic-network") }}),
    defillama_data as ({{ get_defillama_metrics("polygon") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("polygon") }}),
    github_data as ({{ get_github_metrics("polygon") }}),
    contract_data as ({{ get_contract_metrics("polygon") }}),
    revenue_data as (
        select date, native_token_burn as revenue_native, revenue
        from {{ ref("agg_daily_polygon_revenue") }}
    ),
    nft_metrics as ({{ get_nft_metrics("polygon") }}),
    p2p_metrics as ({{ get_p2p_metrics("polygon") }}),
    mau_metrics as ({{ get_mau_metrics("polygon") }})

select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    mau,
    fees_native,
    fees,
    avg_txn_fee,
    revenue_native,
    revenue,
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
    stablecoin_transfer_volume,
    deduped_stablecoin_transfer_volume,
    nft_trading_volume,
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_stablecoin_transfer_volume,
    p2p_transfer_volume,
    coalesce(deduped_stablecoin_transfer_volume, 0) - coalesce(p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume,
    coalesce(dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join mau_metrics on fundamental_data.date = mau_metrics.date
where fundamental_data.date < to_date(sysdate())
