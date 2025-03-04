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
    rolling_metrics as ({{ get_rolling_active_address_metrics("avalanche") }}),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_avalanche_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_avalanche_bridge_bridge_daa") }}
    )
    , date_spine as (
        SELECT date
        FROM {{ ref("dim_date_spine") }}
        WHERE date between '2014-04-13' AND to_date(sysdate()) -- Dev data goes back to 2014
    )

select
    ds.date,
    coalesce(fundamental_data.chain, 'avalanche') as chain,
    txns,
    dau,
    wau,
    mau,
    fees_native,
    case when fees is null then fees_native * price else fees end as fees,
    avg_txn_fee,
    median_txn_fee,
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
    stablecoin_tokenholder_count,
    p2p_stablecoin_tokenholder_count,
    total_staked_native,
    total_staked_usd,
    issuance,
    nft_trading_volume,
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_transfer_volume,
    coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume,
    coalesce(dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume,
    bridge_volume,
    bridge_daa
from date_spine ds
left join staking_data on ds.date = staking_data.date
left join fundamental_data on ds.date = fundamental_data.date
left join price_data on ds.date = price_data.date
left join defillama_data on ds.date = defillama_data.date
left join stablecoin_data on ds.date = stablecoin_data.date
left join github_data on ds.date = github_data.date
left join contract_data on ds.date = contract_data.date
left join issuance_data on ds.date = issuance_data.date
left join nft_metrics on ds.date = nft_metrics.date
left join p2p_metrics on ds.date = p2p_metrics.date
left join rolling_metrics on ds.date = rolling_metrics.date
left join bridge_volume_metrics on ds.date = bridge_volume_metrics.date
left join bridge_daa_metrics on ds.date = bridge_daa_metrics.date
where ds.date < to_date(sysdate())
