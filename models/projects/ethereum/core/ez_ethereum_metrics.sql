-- depends_on {{ ref("fact_ethereum_transactions_gold") }}
-- depends_on {{ ref('fact_ethereum_block_producers_silver') }}
-- depends_on {{ ref('fact_ethereum_amount_staked_silver') }}
{{
    config(
        materialized="table",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("ethereum") }}),
    price_data as ({{ get_coingecko_metrics("ethereum") }}),
    defillama_data as ({{ get_defillama_metrics("ethereum") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("ethereum") }}),
    staking_data as ({{ get_staking_metrics("ethereum") }}),
    censored_block_metrics as ({{ get_censored_block_metrics("ethereum") }}),
    revenue_data as (
        select date, revenue, native_token_burn as revenue_native
        from {{ ref("agg_daily_ethereum_revenue_gold") }}
    ),
    github_data as ({{ get_github_metrics("ethereum") }}),
    contract_data as ({{ get_contract_metrics("ethereum") }}),
    validator_queue_data as (
        select date, queue_entry_amount, queue_exit_amount, queue_active_amount
        from {{ ref("fact_ethereum_beacon_chain_queue_entry_active_exit_silver") }}
    ),
    nft_metrics as ({{ get_nft_metrics("ethereum") }}),
    p2p_metrics as ({{ get_p2p_metrics("ethereum") }}),
    mau_metrics as ({{ get_mau_metrics("ethereum") }})

select
    fundamental_data.date,
    fundamental_data.chain,
    txns,
    dau,
    mau,
    fees_native,
    case when fees is null then fees_native * price else fees end as fees,
    avg_txn_fee,
    revenue_native,
    revenue,
    fees_native - revenue_native as priority_fee_native,
    case
        when fees is null then (fees_native * price) - revenue else fees - revenue
    end as priority_fee_usd,
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
    censored_blocks,
    semi_censored_blocks,
    non_censored_blocks,
    total_blocks_produced,
    percent_censored,
    percent_semi_censored,
    percent_non_censored,
    total_staked_native,
    total_staked_usd,
    queue_entry_amount,
    queue_exit_amount,
    queue_active_amount,
    nft_trading_volume,
    p2p_native_transfer_volume,
    p2p_token_transfer_volume,
    p2p_stablecoin_transfer_volume,
    p2p_transfer_volume
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join censored_block_metrics on fundamental_data.date = censored_block_metrics.date
left join staking_data on fundamental_data.date = staking_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join validator_queue_data on fundamental_data.date = validator_queue_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join mau_metrics on fundamental_data.date = mau_metrics.date
where fundamental_data.date < to_date(sysdate())
