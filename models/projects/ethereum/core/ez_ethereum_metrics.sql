-- depends_on {{ ref("fact_ethereum_transactions_v2") }}
-- depends_on {{ ref('fact_ethereum_block_producers_silver') }}
-- depends_on {{ ref('fact_ethereum_amount_staked_silver') }}
-- depends_on {{ ref('fact_ethereum_p2p_transfer_volume') }}

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
    fundamental_data as ({{ get_fundamental_data_for_chain("ethereum", "v2") }}),
    price_data as ({{ get_coingecko_metrics("ethereum") }}),
    defillama_data as ({{ get_defillama_metrics("ethereum") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("ethereum") }}),
    staking_data as ({{ get_staking_metrics("ethereum") }}),
    censored_block_metrics as ({{ get_censored_block_metrics("ethereum") }}),
    revenue_data as (
        select date, revenue, native_token_burn as revenue_native
        from {{ ref("agg_daily_ethereum_revenue") }}
    ),
    github_data as ({{ get_github_metrics("ethereum") }}),
    contract_data as ({{ get_contract_metrics("ethereum") }}),
    validator_queue_data as (
        select date, queue_entry_amount, queue_exit_amount, queue_active_amount
        from {{ ref("fact_ethereum_beacon_chain_queue_entry_active_exit_silver") }}
    ),
    nft_metrics as ({{ get_nft_metrics("ethereum") }}),
    p2p_metrics as ({{ get_p2p_metrics("ethereum") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("ethereum") }}),
    da_metrics as (
        select date, blob_fees_native, blob_fees, blob_size_mib, avg_mib_per_second, avg_cost_per_mib, avg_cost_per_mib_gwei, submitters
        from {{ ref("fact_ethereum_da_metrics") }}
    ),
    etf_metrics as (
        SELECT
            date,
            sum(net_etf_flow_native) as net_etf_flow_native,
            sum(net_etf_flow) as net_etf_flow,
            sum(cumulative_etf_flow_native) as cumulative_etf_flow_native,
            sum(cumulative_etf_flow) as cumulative_etf_flow
        FROM {{ ref("ez_ethereum_etf_metrics") }}
        GROUP BY 1
    ),
    ethereum_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_ethereum_daily_dex_volumes") }}
    ),
    block_rewards_data as (
        select date, block_rewards_native
        from {{ ref("fact_ethereum_block_rewards") }}
    )

select
    fundamental_data.date
    , fundamental_data.chain
    , fundamental_data.txns
    , dau
    , wau
    , mau
    , fees_native
    , case when fees is null then fees_native * price else fees end as fees
    , avg_txn_fee
    , median_txn_fee
    , revenue_native
    , revenue
    , case
        when fees is null then (fees_native * price) - revenue else fees - revenue
    end as priority_fee_usd
    , nft_trading_volume
    , dau_over_100
    , percent_censored
    , percent_semi_censored
    , percent_non_censored
    , dune_dex_volumes_ethereum.dex_volumes
    , dune_dex_volumes_ethereum.adjusted_dex_volumes
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
    , censored_blocks
    , semi_censored_blocks
    , non_censored_blocks
    , total_blocks_produced
    , percent_censored AS percent_censored_blocks
    , percent_semi_censored AS percent_semi_censored_blocks
    , percent_non_censored AS percent_non_censored_blocks
    , total_staked_native
    , total_staked_usd
    , total_staked_usd AS total_staked
    , queue_entry_amount
    , queue_exit_amount
    , queue_active_amount
    , nft_trading_volume AS chain_nft_trading_volume
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(dune_dex_volumes_ethereum.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    , blob_fees_native
    , blob_fees
    , blob_size_mib
    , avg_mib_per_second
    , avg_cost_per_mib_gwei
    , avg_cost_per_mib
    , submitters
    , dune_dex_volumes_ethereum.dex_volumes AS chain_spot_volume
    -- Cashflow metrics
    , fees as chain_fees
    , fees_native AS gross_protocol_revenue_native
    , fees AS gross_protocol_revenue
    , revenue_native AS burned_cash_flow_native
    , revenue AS burned_cash_flow
    , fees_native - revenue_native as priority_fee_native
    , priority_fee_usd AS priority_fee
    -- Developer metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
    -- Supply metrics
    , block_rewards_native AS gross_emissions_native
    , block_rewards_native * price AS gross_emissions
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
    , stablecoin_data.p2p_stablecoin_transfer_volume
    -- ETF Metrics
    , net_etf_flow_native
    , net_etf_flow
    , cumulative_etf_flow_native
    , cumulative_etf_flow
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
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join da_metrics on fundamental_data.date = da_metrics.date
left join etf_metrics on fundamental_data.date = etf_metrics.date
left join ethereum_dex_volumes as dune_dex_volumes_ethereum on fundamental_data.date = dune_dex_volumes_ethereum.date
left join block_rewards_data on fundamental_data.date = block_rewards_data.date
where fundamental_data.date < to_date(sysdate())
