-- depends_on {{ ref("ez_optimism_transactions") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="optimism",
        database="optimism",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("optimism") }}),
    price_data as ({{ get_coingecko_metrics("optimism") }}),
    defillama_data as ({{ get_defillama_metrics("optimism") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("optimism") }}),
    github_data as ({{ get_github_metrics("optimism") }}),
    contract_data as ({{ get_contract_metrics("optimism") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_optimism_l1_data_cost") }}
    ),  -- supply side revenue and fees
    nft_metrics as ({{ get_nft_metrics("optimism") }}),
    p2p_metrics as ({{ get_p2p_metrics("optimism") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("optimism") }}),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_optimism_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_optimism_bridge_bridge_daa") }}
    ),
    optimism_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_optimism_daily_dex_volumes") }}
    )

select
    coalesce(
        fundamental_data.date,
        price_data.date,
        defillama_data.date,
        expenses_data.date,
        stablecoin_data.date,
        github_data.date,
        contract_data.date
    ) as date,
    'optimism' as chain,
    txns,
    dau,
    wau,
    mau,
    fees_native,  -- total gas fees paid on l2 by users(L2 Fees)
    fees,
    l1_data_cost_native,  -- fees paid to l1 by sequencer (L1 Fees)
    l1_data_cost,
    coalesce(fees_native, 0) - l1_data_cost_native as revenue_native,  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    coalesce(fees, 0) - l1_data_cost as revenue,
    avg_txn_fee,
    median_txn_fee,
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
    p2p_stablecoin_txns,
    p2p_stablecoin_dau,
    p2p_stablecoin_mau,
    stablecoin_data.p2p_stablecoin_transfer_volume,
    stablecoin_tokenholder_count,
    p2p_stablecoin_tokenholder_count,
    nft_trading_volume,
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_transfer_volume,
    coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume,
    coalesce(dune_dex_volumes_optimism.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume,
    bridge_volume,
    bridge_daa,
    dune_dex_volumes_optimism.dex_volumes
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join bridge_volume_metrics on fundamental_data.date = bridge_volume_metrics.date
left join bridge_daa_metrics on fundamental_data.date = bridge_daa_metrics.date
left join optimism_dex_volumes as dune_dex_volumes_optimism on fundamental_data.date = dune_dex_volumes_optimism.date
where fundamental_data.date < to_date(sysdate())
