-- depends_on {{ ref("fact_polygon_transactions_v2") }}
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
    fundamental_data as ({{ get_fundamental_data_for_chain("polygon", "v2") }}),
    price_data as ({{ get_coingecko_metrics("matic-network") }}),
    defillama_data as ({{ get_defillama_metrics("polygon") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("polygon") }}),
    github_data as ({{ get_github_metrics("polygon") }}),
    contract_data as ({{ get_contract_metrics("polygon") }}),
    revenue_data as (
        select date, native_token_burn as revenue_native, revenue
        from {{ ref("agg_daily_polygon_revenue") }}
    ),
    l1_cost_data as (
        select
            raw_date as date,
            sum(tx_fee) as l1_data_cost_native,
            sum(gas_usd) as l1_data_cost
        from {{ref("fact_ethereum_transactions_v2")}}
        where lower(contract_address) = lower('0x86E4Dc95c7FBdBf52e33D563BbDB00823894C287')   
        group by date
    ),
    nft_metrics as ({{ get_nft_metrics("polygon") }}),
    p2p_metrics as ({{ get_p2p_metrics("polygon") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("polygon") }}),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_polygon_pos_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_polygon_pos_bridge_bridge_daa") }}
    ),
    polygon_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_polygon_daily_dex_volumes") }}
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
    , revenue_native
    , revenue
    , l1_data_cost_native
    , l1_data_cost
    , dau_over_100
    , nft_trading_volume
    , dune_dex_volumes_polygon.dex_volumes

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
    , sybil_users
    , non_sybil_users
    , low_sleep_users
    , high_sleep_users
    , dau_over_100 AS dau_over_100_balance
    , nft_trading_volume AS chain_nft_trading_volume
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(dune_dex_volumes_polygon.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    , dune_dex_volumes_polygon.dex_volumes AS chain_spot_volume
    -- Cashflow Metrics
    , fees AS chain_fees
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , revenue_native AS validator_cash_flow_native
    , revenue AS validator_cash_flow
    , l1_data_cost_native AS l1_cash_flow_native
    , l1_data_cost AS l1_cash_flow
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers
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
    , stablecoin_data.p2p_stablecoin_transfer_volume
    -- Bridge Metrics
    , bridge_volume
    , bridge_daa
from fundamental_data
left join price_data on fundamental_data.date = price_data.date
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join github_data on fundamental_data.date = github_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join revenue_data on fundamental_data.date = revenue_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join l1_cost_data on fundamental_data.date = l1_cost_data.date
left join bridge_volume_metrics on fundamental_data.date = bridge_volume_metrics.date
left join bridge_daa_metrics on fundamental_data.date = bridge_daa_metrics.date
left join polygon_dex_volumes as dune_dex_volumes_polygon on fundamental_data.date = dune_dex_volumes_polygon.date
where fundamental_data.date < to_date(sysdate())
