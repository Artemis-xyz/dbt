-- depends_on {{ ref("fact_arbitrum_transactions_v2") }}
{{
    config(
        materialized="incremental",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("arbitrum", "v2") }}),
    price_data as ({{ get_coingecko_metrics("arbitrum") }}),
    defillama_data as ({{ get_defillama_metrics("arbitrum") }}),
    stablecoin_data as ({{ get_stablecoin_metrics("arbitrum") }}),
    expenses_data as (
        select date, chain, l1_data_cost_native, l1_data_cost
        from {{ ref("fact_arbitrum_l1_data_cost") }}
    ),  -- supply side revenue and fees
    github_data as ({{ get_github_metrics("arbitrum") }}),
    contract_data as ({{ get_contract_metrics("arbitrum") }}),
    nft_metrics as ({{ get_nft_metrics("arbitrum") }}),
    p2p_metrics as ({{ get_p2p_metrics("arbitrum") }}),
    rolling_metrics as ({{ get_rolling_active_address_metrics("arbitrum") }}),
    bridge_volume_metrics as (
        select date, bridge_volume
        from {{ ref("fact_arbitrum_one_bridge_bridge_volume") }}
            where chain is null
    ),
    bridge_daa_metrics as (
        select date, bridge_daa
        from {{ ref("fact_arbitrum_one_bridge_bridge_daa") }}
    ),
    arbitrum_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_arbitrum_daily_dex_volumes") }}
    ),
    adjusted_dau_metrics as (
        select date, adj_daus as adjusted_dau
        from {{ ref("ez_arbitrum_adjusted_dau") }}
    ),
    timeboost_fees as (
        select date, timeboost_fees AS timeboost_fees_native, timeboost_fees_usd AS timeboost_fees
        from {{ ref("fact_arbitrum_timeboost_fees") }}
    )
    , unvested_supply as (
        select date, (2694000000 + 1753000000 + 1162000000 + 113000000 + 750000000 - total_vested_supply) as unvested_supply_native
        from {{ ref("ez_arbitrum_circulating_supply_metrics") }}
    )
    , burns_mints as (
        select date, SUM(burns_native) as burns_native, SUM(mints_native) as mints_native, SUM(burns) as burns, SUM(mints) as mints, SUM(cumulative_burns_native) as cumulative_burns_native, SUM(cumulative_mints_native) as cumulative_mints_native, SUM(cumulative_burns) as cumulative_burns, SUM(cumulative_mints) as cumulative_mints
        from {{ ref("fact_arbitrum_burns_mints") }}
        group by 1
    )
    , foundation_owned_supply as (
        select date, native_balance as foundation_owned_supply_native, usd_balance as foundation_owned_supply
        from {{ ref("fact_arbitrum_owned_supply") }}
        WHERE LOWER(contract_address) = LOWER('0x912ce59144191c1204e64559fe8253a0e49e6548')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY native_balance DESC) = 1
    )
    , application_fees AS (
        SELECT 
            DATE_TRUNC(DAY, date) AS date 
            , SUM(COALESCE(fees, 0)) AS application_fees
        FROM {{ ref("ez_protocol_datahub_by_chain") }}
        WHERE chain = 'arbitrum'
        GROUP BY 1
    )

select
    fundamental_data.date
    , fundamental_data.chain
    , txns
    , dau
    , adjusted_dau
    , wau
    , mau
    , coalesce(fees_native, 0) + coalesce(timeboost_fees_native, 0) as fees_native
    , coalesce(fees, 0) + coalesce(timeboost_fees, 0) as fees
    , l1_data_cost_native
    , l1_data_cost
    , coalesce(fees_native, 0) - l1_data_cost_native + coalesce(0.97 * timeboost_fees_native, 0) as revenue_native
    , coalesce(fees, 0) - l1_data_cost + coalesce(0.97 * timeboost_fees, 0) as revenue
    , avg_txn_fee
    , median_txn_fee
    , dau_over_100
    , nft_trading_volume
    , dune_dex_volumes_arbitrum.dex_volumes
    , dune_dex_volumes_arbitrum.adjusted_dex_volumes
    , bridge_daa
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
    , dune_dex_volumes_arbitrum.dex_volumes AS chain_spot_volume
    , returning_users
    , new_users
    , low_sleep_users
    , high_sleep_users
    , sybil_users
    , non_sybil_users
    , dau_over_100 AS dau_over_100_balance
    , nft_trading_volume AS chain_nft_trading_volume
    , p2p_native_transfer_volume
    , p2p_token_transfer_volume
    , p2p_transfer_volume
    , coalesce(artemis_stablecoin_transfer_volume, 0) - coalesce(stablecoin_data.p2p_stablecoin_transfer_volume, 0) as non_p2p_stablecoin_transfer_volume
    , coalesce(dune_dex_volumes_arbitrum.dex_volumes, 0) + coalesce(nft_trading_volume, 0) + coalesce(p2p_transfer_volume, 0) as settlement_volume
    , coalesce(revenue, 0) + coalesce(settlement_volume, 0) + coalesce(application_fees.application_fees, 0) as total_economic_activity
    -- Cashflow Metrics
    , coalesce(fees_native, 0) + coalesce(timeboost_fees_native, 0) AS chain_fees
    , coalesce(fees_native, 0) + coalesce(timeboost_fees_native, 0) AS ecosystem_revenue_native -- Total gas fees paid on L2 by users (L2 Fees)
    , coalesce(fees, 0) + coalesce(timeboost_fees, 0) AS ecosystem_revenue
    , coalesce(fees_native, 0) - l1_data_cost_native + coalesce(0.97 * timeboost_fees_native, 0) as treasury_fee_allocation_native  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost + coalesce(0.97 * timeboost_fees, 0) as treasury_fee_allocation
    , l1_data_cost_native AS l1_fee_allocation_native -- fees paid to l1 by sequencer (L1 Fees)
    , l1_data_cost AS l1_fee_allocation
    , coalesce(timeboost_fees_native, 0) as timeboost_fees_native
    , coalesce(timeboost_fees, 0) as timeboost_fees
    , coalesce(burns_native, 0) as burns_native
    , coalesce(mints_native, 0) as mints_native
    , coalesce(burns, 0) as burns
    , coalesce(mints, 0) as mints
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
    , bridge_volume_metrics.bridge_volume as bridge_volume
    , bridge_daa_metrics.bridge_daa as bridge_dau
    -- Supply Metrics
    , CASE
        WHEN fundamental_data.date > '2023-03-22' THEN 10000000000 
        ELSE 0 
    END AS max_supply_native
    , CASE
        WHEN fundamental_data.date > '2023-03-22' THEN 10000000000 
        ELSE 0 
    END AS total_supply_native
    , COALESCE(total_supply_native, 0) - COALESCE(foundation_owned_supply_native, 0) - COALESCE(cumulative_burns_native, 0) AS issued_supply_native
    , COALESCE(total_supply_native, 0) - COALESCE(foundation_owned_supply_native, 0) - COALESCE(cumulative_burns_native, 0) - COALESCE(unvested_supply_native, 0) AS circulating_supply_native

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
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
left join arbitrum_dex_volumes as dune_dex_volumes_arbitrum on fundamental_data.date = dune_dex_volumes_arbitrum.date
left join adjusted_dau_metrics on fundamental_data.date = adjusted_dau_metrics.date
left join timeboost_fees on fundamental_data.date = timeboost_fees.date
left join unvested_supply on fundamental_data.date = unvested_supply.date
left join burns_mints on fundamental_data.date = burns_mints.date
left join foundation_owned_supply on fundamental_data.date = foundation_owned_supply.date
left join application_fees on fundamental_data.date = application_fees.date
where true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())
