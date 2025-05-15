-- depends_on {{ ref("fact_base_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="BASE",
        database="base",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("base", "v2") }}),
    defillama_data as ({{ get_defillama_metrics("base") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("base") }}),
    contract_data as ({{ get_contract_metrics("base") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_base_l1_data_cost") }}
    ),  -- supply side revenue and fees
    nft_metrics as ({{ get_nft_metrics("base") }}),
    p2p_metrics as ({{ get_p2p_metrics("base") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("base") }}),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_base_bridge_bridge_volume") }}
        where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_base_bridge_bridge_daa") }}
    ),
    base_dex_volumes as (
        select date, daily_volume as dex_volumes
        from {{ ref("fact_base_daily_dex_volumes") }}
    ),
    adjusted_dau_metrics as (
        select date, adj_daus as adjusted_dau
        from {{ ref("ez_base_adjusted_dau") }}
    )

select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , adjusted_dau
    , wau
    , mau
    , fees_native
    , fees
    , l1_data_cost_native
    , l1_data_cost
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as revenue
    , avg_txn_fee
    , median_txn_fee
    , dau_over_100
    , nft_trading_volume
    , dune_dex_volumes_base.dex_volumes AS dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , tvl
    -- Chain Usage Metrics
    , dau as chain_dau
    , txns as chain_txns
    , avg_txn_fee as chain_avg_txn_fee
    , median_txn_fee as chain_median_txn_fee
    , dau_over_100 as chain_dau_over_100_balance
    , nft_trading_volume as chain_nft_trading_volume
    , sybil_users
    , non_sybil_users
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , dune_dex_volumes_base.dex_volumes AS chain_spot_volume
    , coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(dune_dex_volumes_base.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    -- Cashflow Metrics
    , fees_native as ecosystem_revenue_native
    , fees as ecosystem_revenue
    , l1_data_cost_native AS l1_cash_flow_native  -- fees paid to l1 by sequencer (L1 Fees)
    , l1_data_cost AS l1_cash_flow
    , coalesce(fees_native, 0) - coalesce(l1_data_cost_native, 0) as treasury_cash_flow_native
    , coalesce(fees, 0) - coalesce(l1_data_cost, 0) as treasury_cash_flow
    -- Developer Metrics
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
left join defillama_data on fundamental_data.date = defillama_data.date
left join stablecoin_data on fundamental_data.date = stablecoin_data.date
left join expenses_data on fundamental_data.date = expenses_data.date
left join contract_data on fundamental_data.date = contract_data.date
left join nft_metrics on fundamental_data.date = nft_metrics.date
left join p2p_metrics on fundamental_data.date = p2p_metrics.date
left join rolling_metrics on fundamental_data.date = rolling_metrics.date
left join bridge_volume_metrics on fundamental_data.date = bridge_volume_metrics.date
left join bridge_daa_metrics on fundamental_data.date = bridge_daa_metrics.date
left join base_dex_volumes as dune_dex_volumes_base on fundamental_data.date = dune_dex_volumes_base.date
left join adjusted_dau_metrics on fundamental_data.date = adjusted_dau_metrics.date
where fundamental_data.date < to_date(sysdate())
