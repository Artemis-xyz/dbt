{{
    config(
        materialized="table",
        snowflake_warehouse="internet_computer",
        database="internet_computer",
        schema="core",
        alias="ez_metrics",
    )
}}
with
    -- Latest data from ICP api is 2023-12-21
    icp_metrics as (select * from {{ ref("fact_internet_computer_fundamental_metrics_silver") }} where date > '2023-12-21')
    , icp_blocks as (select * from {{ ref("fact_internet_computer_block_count_silver") }})
    , icp_total_canister_state as (select * from {{ ref("fact_internet_computer_canister_total_state_silver") }})
    , icp_neuron_funds as (select * from {{ ref("fact_internet_computer_neuron_funds_silver") }})
    , price_data as ({{ get_coingecko_metrics("internet-computer") }})
    , defillama_data as ({{ get_defillama_metrics("icp") }})
    , icp_circulating_supply as (select date, circulating_supply_native from {{ ref("fact_internet_computer_circulating_supply_data") }})
    , icp_total_supply as (select date, total_supply_native from {{ ref("fact_internet_computer_total_supply_data") }})
select
    coalesce(price_data.date, defillama_data.date, icp_metrics.date, icp_total_canister_state.date, icp_neuron_funds.date, icp_blocks.date, icp_circulating_supply.date, icp_total_supply.date) as date
    , 'internet_computer' as chain
    , dau
    , txns
    , icp_burned
    , icp_burned * price as fees
    , icp_burned * price as revenue
    , icp_transaction_fees / txns as avg_txn_fee
    , total_native_fees -- total transaction fees
    , dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , tvl
    -- Chain Usage Metrics
    , dau AS chain_dau
    , txns AS chain_txns
    , icp_transaction_fees / txns AS chain_avg_txn_fee
    , dex_volumes AS chain_spot_volume
    -- Cashflow Metrics
    , total_native_fees * price AS chain_fees
    , total_native_fees AS ecosystem_revenue_native -- total transaction fees
    , total_native_fees * price AS ecosystem_revenue
    , icp_burned AS burned_fee_allocation_native
    , icp_burned * price AS burned_fee_allocation
    -- Bespoke metrics
    , total_transactions
    , update_txns
    , icp_txns
    , neurons_total -- accounts that are staking ICP
    , avg_tps
    , avg_blocks_per_second
    , nns_tvl_native * price as nns_tvl -- same as total icp staked in NNS
    , nns_tvl_native 
    , nns_proposal_count
    , neuron_funds_staked_native as neuron_funds_staked_native
    , neuron_funds_staked_native * price as neuron_funds_staked
    , total_canister_state_tib
    , total_icp_burned as total_burned_native
    , total_registered_canister_count -- total cannister count 
    , canister_memory_usage_gb -- cannister state
    , one_year_staking_apy
    , ckbtc_total_supply
    , cycle_burn_rate_average
    , total_internet_identity_user_count
    , icp_blocks.block_count
    , 5 as storage_cost

    -- Supply Data
    , icp_total_supply.total_supply_native as max_supply_native
    , 0 as uncreated_tokens
    , icp_total_supply.total_supply_native
    , sum(icp_burned) over (order by icp_metrics.date) as cumulative_icp_burned
    , icp_circulating_supply.circulating_supply_native as issued_supply_native
    , icp_total_supply.total_supply_native - icp_circulating_supply.circulating_supply_native as foundation_owned
    , 0 as unvested_tokens
    , icp_circulating_supply.circulating_supply_native
    
from price_data
left join icp_metrics on price_data.date = icp_metrics.date
left join icp_blocks on price_data.date = icp_blocks.date
left join icp_total_canister_state on price_data.date = icp_total_canister_state.date
left join icp_neuron_funds on price_data.date = icp_neuron_funds.date
left join defillama_data on price_data.date = defillama_data.date
left join icp_circulating_supply on price_data.date = icp_circulating_supply.date
left join icp_total_supply on price_data.date = icp_total_supply.date
where coalesce(price_data.date, defillama_data.date, icp_metrics.date, icp_blocks.date, icp_total_canister_state.date, icp_neuron_funds.date) < to_date(sysdate())
