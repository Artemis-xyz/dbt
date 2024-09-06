-- depends_on {{ ref("ez_avalanche_transactions") }}
-- depends_on {{ ref('fact_avalanche_amount_staked_silver') }}
{{
    config(
        materialized="table",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("avalanche") }}),
    price_data as ({{ get_coingecko_metrics("avalanche-2") }}),
    defillama_data as ({{ get_defillama_metrics("avalanche") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("avalanche") }}),
    staking_data as ({{ get_staking_metrics("avalanche") }}),
    github_data as ({{ get_github_metrics("avalanche") }}),
    contract_data as ({{ get_contract_metrics("avalanche") }}),
    issuance_data as (
        select date, validator_rewards as issuance
        from {{ ref("fact_avalanche_validator_rewards_silver") }}
    ),
    nft_metrics as ({{ get_nft_metrics("avalanche") }}),
    p2p_metrics as ({{ get_p2p_metrics("avalanche") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("avalanche") }})

select
    coalesce(fundamental_data.date, staking_data.date) as date,
    coalesce(fundamental_data.chain, 'avalanche') as chain,
    txns,
    dau,
    wau,
    mau,
    fees_native,
    case when fees is null then fees_native * price else fees end as fees,
    avg_txn_fee,
    fees_native as revenue_native,
    fees as revenue,
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
    p2p_stablecoin_txns,
    p2p_stablecoin_dau,
    p2p_stablecoin_mau,
    stablecoin_data.p2p_stablecoin_transfer_volume,
    total_staked_native,
    total_staked_usd,
    issuance,
    nft_trading_volume,
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_transfer_volume,
    coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume,
    coalesce(dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
from staking_data
left join fundamental_data on staking_data.date = fundamental_data.date
left join price_data on staking_data.date = price_data.date
left join defillama_data on staking_data.date = defillama_data.date
left join stablecoin_data on staking_data.date = stablecoin_data.date
left join github_data on staking_data.date = github_data.date
left join contract_data on staking_data.date = contract_data.date
left join issuance_data on staking_data.date = issuance_data.date
left join nft_metrics on staking_data.date = nft_metrics.date
left join p2p_metrics on staking_data.date = p2p_metrics.date
left join rolling_metrics on staking_data.date = rolling_metrics.date
where coalesce(fundamental_data.date, staking_data.date) < to_date(sysdate())
