-- depends_on {{ ref("fact_optimism_transactions_v2") }}
{{
    config(
        materialized="table",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("optimism", "v2") }}),
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
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_optimism_daily_dex_volumes") }}
    ),
    adjusted_dau_metrics as (
        select date, adj_daus as adjusted_dau
        from {{ ref("ez_optimism_adjusted_dau") }}
    )
    , token_incentives as (
        select 
            date, 
            sum(token_incentives) as token_incentives
        from {{ ref("fact_optimism_token_incentives") }}
        group by 1
    )
    , revenue_share as (
        select
            date, 
            sum(revenue_share_native) as revenue_share_native,
            sum(revenue_share) as revenue_share
        from {{ ref("fact_optimism_revenue_share") }}
        group by 1
    )
    , mints_burns as (
        select
            date,
            mints_native,
            burns_native, 
            cumulative_mints_native,
            cumulative_burns_native
        from {{ ref("fact_optimism_mints_burns") }}
    )
    , unvested_supply as (
        select
            date, 
            total_vested_supply, 
            1545523296 - total_vested_supply AS total_unvested_supply
        from {{ ref("fact_optimism_all_supply_events") }}
    )
    , owned_supply as (
        select
            date, 
            native_balance AS foundation_owned_supply_native
        from {{ ref("fact_optimism_owned_supply") }}
        where contract_address = '0x4200000000000000000000000000000000000042'
    )
    , application_fees AS (
        SELECT 
            DATE_TRUNC(DAY, date) AS date 
            , SUM(COALESCE(fees, 0)) AS application_fees
        FROM {{ ref("ez_protocol_datahub_by_chain") }}
        WHERE chain = 'optimism'
        GROUP BY 1
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
    'optimism' as chain
    , txns
    , dau
    , adjusted_dau
    , wau
    , mau
    , fees_native
    , fees
    , l1_data_cost_native
    , l1_data_cost
    , avg_txn_fee
    , median_txn_fee
    , dau_over_100
    , coalesce(fees_native, 0) + coalesce(revenue_share_native, 0) - l1_data_cost_native as revenue_native  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) + (coalesce(revenue_share, 0)) - l1_data_cost as revenue
    , coalesce(revenue_share_native, 0) as revenue_share_native
    , coalesce(revenue_share, 0) as revenue_share
    , nft_trading_volume
    , dune_dex_volumes_optimism.dex_volumes
    , dune_dex_volumes_optimism.adjusted_dex_volumes

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
    , coalesce(dune_dex_volumes_optimism.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    , dune_dex_volumes_optimism.dex_volumes AS chain_spot_volume
    , coalesce(fees, 0) - coalesce(l1_data_cost, 0) + coalesce(settlement_volume, 0) + coalesce(application_fees.application_fees, 0) as total_economic_activity

    -- Cashflow Metrics
    , fees AS chain_fees
    , revenue - token_incentives.token_incentives as earnings
    , l1_data_cost_native AS l1_fee_allocation_native
    , l1_data_cost AS l1_fee_allocation
    , coalesce(fees_native, 0) - l1_data_cost_native as treasury_fee_allocation_native
    , coalesce(fees, 0) - l1_data_cost as treasury_fee_allocation

    , token_incentives.token_incentives

    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem
    , weekly_contracts_deployed
    , weekly_contract_deployers

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

    -- Bridge Metrics
    , bridge_volume
    , bridge_daa

    -- Supply Metrics
    , cumulative_mints_native AS max_supply_native
    , cumulative_mints_native AS total_supply_native
    , cumulative_mints_native - cumulative_burns_native - foundation_owned_supply_native AS issued_supply_native
    , cumulative_mints_native - cumulative_burns_native - foundation_owned_supply_native - total_unvested_supply AS circulating_supply_native
    , total_unvested_supply
    , foundation_owned_supply_native
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
left join adjusted_dau_metrics on fundamental_data.date = adjusted_dau_metrics.date
left join token_incentives on fundamental_data.date = token_incentives.date
left join revenue_share on fundamental_data.date = revenue_share.date
left join mints_burns on fundamental_data.date = mints_burns.date
left join unvested_supply on fundamental_data.date = unvested_supply.date
left join owned_supply on fundamental_data.date = owned_supply.date
left join application_fees on fundamental_data.date = application_fees.date
where fundamental_data.date < to_date(sysdate())
