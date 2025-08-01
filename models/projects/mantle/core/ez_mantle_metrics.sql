-- depends_on {{ ref("fact_mantle_transactions_v2") }}
{{
    config(
        materialized='incremental'
        , snowflake_warehouse='MANTLE'
        , database="mantle"
        , schema="core"
        , alias="ez_metrics"
        , incremental_strategy="merge"
        , unique_key="date"
        , on_schema_change="append_new_columns"
        , merge_update_columns=var("backfill_columns", [])
        , merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none
        , full_refresh=var("full_refresh", false)
        , tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
fundamental_data as ({{ get_fundamental_data_for_chain("mantle", "v2") }})
, expenses_data as (
    select date, chain, l1_data_cost_native, l1_data_cost
    from {{ ref("fact_mantle_l1_data_cost") }}
)
, treasury_data as (
    SELECT   
        date,
        sum(native_balance) as treasury_value_native,
        sum(native_balance) - lag(sum(native_balance)) over (order by date) as treasury_value_native_change,
    FROM {{ ref("fact_mantle_treasury_balance") }}
    WHERE token = 'MNT'
    GROUP BY 1
)
, github_data as ({{ get_github_metrics("mantle") }})
, rolling_metrics as ({{ get_rolling_active_address_metrics("mantle") }})
, defillama_data as ({{ get_defillama_metrics("mantle") }})
, stablecoin_data as ({{ get_stablecoin_metrics("mantle", backfill_date="2023-07-02") }})
, market_data as ({{ get_coingecko_metrics("mantle") }})
, mantle_dex_volumes as (
    select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
    from {{ ref("fact_mantle_daily_dex_volumes") }}
)
, staked_eth_metrics as (
    select
        date,
        num_staked_eth,
        amount_staked_usd,
        num_staked_eth_net_change,
        amount_staked_usd_net_change
    from {{ ref('fact_meth_staked_eth_count_with_USD_and_change') }}
)

select
    f.date
    , 'mantle' as artemis_id

    , dune_dex_volumes_mantle.adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume

    -- Chain Usage Metrics
    , f.dau AS chain_dau
    , f.dau
    , rolling.wau AS chain_wau
    , rolling.mau AS chain_mau
    , f.txns AS chain_txns
    , f.txns
    , f.returning_users
    , f.new_users
    , f.avg_txn_fee AS chain_avg_txn_fee
    , dune_dex_volumes_mantle.dex_volumes AS chain_spot_volume

    -- LST Usage Metrics
    , staking.num_staked_eth as tvl_native
    , staking.num_staked_eth as lst_tvl_native
    , staking.amount_staked_usd as lst_tvl
    , staking.amount_staked_usd as tvl

    
    -- Fee Metrics
    , f.fees as chain_fees
    , f.fees
    , f.fees_native
    , coalesce(f.fees_native, 0) - expenses_data.l1_data_cost_native as validator_fee_allocation_native -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(f.fees, 0) - expenses_data.l1_data_cost as validator_fee_allocation
    , expenses_data.l1_data_cost_native AS l1_fee_allocation_native
    , expenses_data.l1_data_cost AS l1_fee_allocation
    
    -- Financial Metrics
    , coalesce(fees, 0) - l1_data_cost as revenue
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    
    -- Treasury Metrics 
    , treasury_data.treasury_value_native AS treasury_native
    , treasury_data.treasury_value_native_change AS treasury_native_change
    
    -- Developer Metrics
    , weekly_commits_core_ecosystem
    , weekly_commits_sub_ecosystem
    , weekly_developers_core_ecosystem
    , weekly_developers_sub_ecosystem

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
    , p2p_stablecoin_txns
    , p2p_stablecoin_dau
    , p2p_stablecoin_mau
    , stablecoin_data.p2p_stablecoin_transfer_volume
    , p2p_stablecoin_tokenholder_count

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data f
left join github_data using (date)
left join defillama_data using (date)
left join stablecoin_data using (date)
left join market_data using (date)
left join expenses_data using (date)
left join rolling_metrics rolling using (date)
left join treasury_data using (date)
left join mantle_dex_volumes as dune_dex_volumes_mantle using (date)
left join staked_eth_metrics as staking using (date)
where true
{{ ez_metrics_incremental('f.date', backfill_date) }}
and f.date < to_date(sysdate())
