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
        , full_refresh=false
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
, stablecoin_data as ({{ get_stablecoin_metrics("mantle") }})
, price_data as ({{ get_coingecko_metrics("mantle") }})
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
    fundamental_data.date
    , 'mantle' as chain
    , txns
    , dau
    , wau
    , mau
    , fees
    , fees_native
    , l1_data_cost
    , l1_data_cost_native
    , coalesce(fees_native, 0) - l1_data_cost_native as revenue_native -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as revenue
    , avg_txn_fee
    , treasury_data.treasury_value_native
    , treasury_data.treasury_value_native_change
    , dune_dex_volumes_mantle.dex_volumes
    , dune_dex_volumes_mantle.adjusted_dex_volumes
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
    , returning_users
    , new_users
    , avg_txn_fee AS chain_avg_txn_fee
    , dune_dex_volumes_mantle.dex_volumes AS chain_spot_volume
    -- LST Metrics
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as tvl_native_net_change
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    -- Cashflow Metrics
    , fees as chain_fees
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    , coalesce(fees_native, 0) - l1_data_cost_native as validator_fee_allocation_native -- supply side: fees paid to squencer - fees paied to l1 (L2 Revenue)
    , coalesce(fees, 0) - l1_data_cost as validator_fee_allocation
    , l1_data_cost_native AS l1_fee_allocation_native
    , l1_data_cost AS l1_fee_allocation
    -- Protocol Metrics 
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
from fundamental_data
left join github_data using (date)
left join defillama_data using (date)
left join stablecoin_data using (date)
left join price_data using (date)
left join expenses_data using (date)
left join rolling_metrics using (date)
left join treasury_data using (date)
left join mantle_dex_volumes as dune_dex_volumes_mantle on fundamental_data.date = dune_dex_volumes_mantle.date
left join staked_eth_metrics on fundamental_data.date = staked_eth_metrics.date
where true
{{ ez_metrics_incremental('fundamental_data.date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())
