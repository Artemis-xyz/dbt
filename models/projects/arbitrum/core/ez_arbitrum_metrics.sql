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
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    fundamental_data as ({{ get_fundamental_data_for_chain("arbitrum", "v2") }}),
    market_metrics as ({{ get_coingecko_metrics("arbitrum") }}),
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
        select date, coalesce(bridge_volume, 0) as bridge_volume
        from {{ ref("fact_arbitrum_one_bridge_bridge_volume") }}
            where chain is null
    ),
    bridge_daa_metrics as (
        select date, coalesce(bridge_daa, 0) as bridge_daa
        from {{ ref("fact_arbitrum_one_bridge_bridge_daa") }}
    ),
    arbitrum_dex_volumes as (
        select date, coalesce(daily_volume, 0) as dex_volumes, coalesce(daily_volume_adjusted, 0) as adjusted_dex_volumes
        from {{ ref("fact_arbitrum_daily_dex_volumes") }}
    ),
    adjusted_dau_metrics as (
        select date, coalesce(adj_daus, 0) as adjusted_dau
        from {{ ref("ez_arbitrum_adjusted_dau") }}
    ),
    timeboost_fees as (
        select date, coalesce(timeboost_fees, 0) AS timeboost_fees_native, coalesce(timeboost_fees_usd, 0) AS timeboost_fees
        from {{ ref("fact_arbitrum_timeboost_fees") }}
    )
    , unvested_supply as (
        select date, (2694000000 + 1753000000 + 1162000000 + 113000000 + 750000000 - total_vested_supply) as unvested_supply_native
        from {{ ref("ez_arbitrum_circulating_supply_metrics") }}
    )
    , burns_mints as (
        select date, coalesce(SUM(burns_native), 0) as burns_native, coalesce(SUM(mints_native), 0) as mints_native, coalesce(SUM(burns), 0) as burns, coalesce(SUM(mints), 0) as mints, coalesce(SUM(cumulative_burns_native), 0) as cumulative_burns_native, coalesce(SUM(cumulative_mints_native), 0) as cumulative_mints_native, coalesce(SUM(cumulative_burns), 0) as cumulative_burns, coalesce(SUM(cumulative_mints), 0) as cumulative_mints
        from {{ ref("fact_arbitrum_burns_mints") }}
        group by 1
    )
    , foundation_owned_supply as (
        select date, coalesce(native_balance, 0) as foundation_owned_supply_native, coalesce(usd_balance, 0) as foundation_owned_supply
        from {{ ref("fact_arbitrum_owned_supply") }}
        WHERE LOWER(contract_address) = LOWER('0x912ce59144191c1204e64559fe8253a0e49e6548')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY native_balance DESC) = 1
    )
    , application_fees AS (
        SELECT 
            DATE_TRUNC(DAY, date) AS date 
            , coalesce(SUM(COALESCE(fees, 0)), 0) AS application_fees
        FROM {{ ref("ez_protocol_datahub_by_chain") }}
        WHERE chain = 'arbitrum'
        GROUP BY 1
    )

select
    fundamental_data.date
    , 'arbitrum' as artemis_id

    -- Standardized Metrics
    
    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Data
    , fundamental_data.dau as chain_dau
    , adjusted_dau_metrics.adjusted_dau as adjusted_dau
    , fundamental_data.dau as dau
    , rolling_metrics.wau as chain_wau
    , rolling_metrics.mau as chain_mau
    , fundamental_data.txns as chain_txns
    , fundamental_data.txns as txns
    , fundamental_data.avg_txn_fee as chain_avg_txn_fee  
    , fundamental_data.median_txn_fee as chain_median_txn_fee
    , arbitrum_dex_volumes.dex_volumes as chain_spot_volume
    , fundamental_data.returning_users
    , fundamental_data.new_users
    , fundamental_data.low_sleep_users
    , fundamental_data.high_sleep_users
    , fundamental_data.sybil_users
    , fundamental_data.non_sybil_users
    , fundamental_data.dau_over_100 as dau_over_100_balance
    , nft_metrics.nft_trading_volume as chain_nft_trading_volume
    , p2p_metrics.p2p_native_transfer_volume
    , p2p_metrics.p2p_token_transfer_volume
    , p2p_metrics.p2p_transfer_volume
    , stablecoin_data.artemis_stablecoin_transfer_volume - stablecoin_data.p2p_stablecoin_transfer_volume as non_p2p_stablecoin_transfer_volume
    , arbitrum_dex_volumes.dex_volumes + nft_metrics.nft_trading_volume + p2p_metrics.p2p_transfer_volume as settlement_volume
    
    -- Fee Data
    , fundamental_data.fees_native + timeboost_fees_native as fees_native
    , fundamental_data.fees+ timeboost_fees as chain_fees
    , fundamental_data.fees + timeboost_fees as fees
    , fees_native - l1_data_cost_native + 0.97 * timeboost_fees_native as treasury_fee_allocation_native  -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , fees - l1_data_cost + 0.97 * timeboost_fees as treasury_fee_allocation
    , l1_data_cost_native as l1_fee_allocation_native -- fees paid to l1 by sequencer (L1 Fees)
    , l1_data_cost as l1_fee_allocation
    , timeboost_fees_native as timeboost_fees_native
    , timeboost_fees as timeboost_fees

    -- Financial Statements
    , fees_native - l1_data_cost_native + 0.97 * timeboost_fees_native as revenue_native
    , fees - l1_data_cost + 0.97 * timeboost_fees as revenue

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

    -- Developer Data
    , github_data.weekly_commits_core_ecosystem
    , github_data.weekly_commits_sub_ecosystem
    , github_data.weekly_developers_core_ecosystem
    , github_data.weekly_developers_sub_ecosystem
    , contract_data.weekly_contracts_deployed
    , contract_data.weekly_contract_deployers

    -- Supply Data
    , burns_mints.mints_native as gross_emissions_native
    , burns_mints.mints as gross_emissions
    , CASE
        WHEN fundamental_data.date > '2023-03-22' THEN 10000000000 
        ELSE 0 
    END AS max_supply_native
    , CASE
        WHEN fundamental_data.date > '2023-03-22' THEN 10000000000 
        ELSE 0 
    END AS total_supply_native
    , burns_mints.burns_native as burns_native
    , burns_mints.burns as burns
    , total_supply_native - foundation_owned_supply_native - cumulative_burns_native AS issued_supply_native
    , total_supply_native - foundation_owned_supply_native - cumulative_burns_native - unvested_supply_native AS circulating_supply_native

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_fdv
    , market_metrics.token_turnover_circulating

    -- Bespoke Metrics
    , bridge_volume_metrics.bridge_volume as bridge_volume
    , bridge_daa_metrics.bridge_daa as bridge_dau
    , arbitrum_dex_volumes.dex_volumes
    , arbitrum_dex_volumes.adjusted_dex_volumes
    , revenue + settlement_volume + application_fees.application_fees as total_economic_activity    

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
    
from fundamental_data
left join market_metrics on fundamental_data.date = market_metrics.date
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
left join arbitrum_dex_volumes on fundamental_data.date = arbitrum_dex_volumes.date
left join adjusted_dau_metrics on fundamental_data.date = adjusted_dau_metrics.date
left join timeboost_fees on fundamental_data.date = timeboost_fees.date
left join unvested_supply on fundamental_data.date = unvested_supply.date
left join burns_mints on fundamental_data.date = burns_mints.date
left join foundation_owned_supply on fundamental_data.date = foundation_owned_supply.date
left join application_fees on fundamental_data.date = application_fees.date
where true
{{ ez_metrics_incremental("fundamental_data.date", backfill_date) }}
and fundamental_data.date < to_date(sysdate())
