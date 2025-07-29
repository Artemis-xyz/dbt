{{
    config(
        materialized="table",
        snowflake_warehouse="internet_computer",
        database="internet_computer",
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
with
    -- Latest data from ICP api is 2023-12-21
    icp_metrics as (select * from {{ ref("fact_internet_computer_fundamental_metrics_silver") }} where date > '2023-12-21')
    , icp_blocks as (select * from {{ ref("fact_internet_computer_block_count_silver") }})
    , icp_total_canister_state as (select * from {{ ref("fact_internet_computer_canister_total_state_silver") }})
    , icp_neuron_funds as (select * from {{ ref("fact_internet_computer_neuron_funds_silver") }})
    , market_data as ({{ get_coingecko_metrics("internet-computer") }})
    , defillama_data as ({{ get_defillama_metrics("icp") }})
select
    coalesce(market_data.date, defillama_data.date, icp_metrics.date, icp_total_canister_state.date, icp_neuron_funds.date, icp_blocks.date) as date
    , 'internet_computer' as artemis_id

    -- Standardized Metrics
    -- Market Data Metrics
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , defillama_data.tvl

    -- Chain Usage Metrics
    , dau AS chain_dau
    , dau
    , txns AS chain_txns
    , txns
    , icp_transaction_fees / txns AS chain_avg_txn_fee
    , dex_volumes AS chain_spot_volume

    -- Fees Metrics
    , total_native_fees * price AS chain_fees
    , total_native_fees * price AS fees
    , total_native_fees AS fees_native -- total transaction fees
    , icp_burned AS burned_fee_allocation_native
    , icp_burned * price AS burned_fee_allocation

    -- Financial Metrics
    , icp_burned * price as revenue

    -- Supply Metrics
    , icp_burned AS burns_native

    -- Bespoke metrics
    , total_transactions
    , update_txns
    , icp_txns
    , neurons_total -- accounts that are staking ICP
    , avg_tps as average_tps
    , avg_blocks_per_second
    , nns_tvl_native * price as nns_tvl -- same as total icp staked in NNS
    , nns_tvl_native 
    , nns_proposal_count
    , neuron_funds_staked_native as neuron_funds_staked_native
    , neuron_funds_staked_native * price as neuron_funds_staked
    , total_canister_state_tib
    , total_registered_canister_count -- total cannister count 
    , canister_memory_usage_gb -- cannister state
    , one_year_staking_apy
    , ckbtc_total_supply
    , cycle_burn_rate_average
    , total_internet_identity_user_count
    , icp_blocks.block_count
    , 5 as storage_cost
from market_data
left join icp_metrics on market_data.date = icp_metrics.date
left join icp_blocks on market_data.date = icp_blocks.date
left join icp_total_canister_state on market_data.date = icp_total_canister_state.date
left join icp_neuron_funds on market_data.date = icp_neuron_funds.date
left join defillama_data on market_data.date = defillama_data.date
where coalesce(market_data.date, defillama_data.date, icp_metrics.date, icp_blocks.date, icp_total_canister_state.date, icp_neuron_funds.date) < to_date(sysdate())
