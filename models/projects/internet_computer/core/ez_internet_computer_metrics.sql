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
    , price_data as ({{ get_coingecko_metrics("internet-computer") }})
    , defillama_data as ({{ get_defillama_metrics("icp") }})
select
    coalesce(price_data.date, defillama_data.date, icp_metrics.date, icp_blocks.date) as date
    , total_transactions
    , dau
    , txns
    , neurons_total -- accounts that are staking ICP
    , avg_tps
    , avg_blocks_per_second
    , icp_burned
    , icp_burned * price as fees
    , icp_burned * price as revenue
    , total_native_fees -- total transaction fees
    , nns_tvl -- same as total icp staked in NNS
    , nns_proposal_count
    , total_registered_canister_count -- total cannister count 
    , canister_memory_usage_gb -- cannister state
    , one_year_staking_apy
    , ckbtc_total_supply
    , cycle_burn_rate_average
    , total_internet_identity_user_count
    , 'internet_computer' as chain
    , icp_blocks.block_count
    , price
    , market_cap
    , fdmc
    , tvl
    , dex_volumes
from price_data
full join icp_metrics on price_data.date = icp_metrics.date
full join icp_blocks on price_data.date = icp_blocks.date
full join defillama_data on price_data.date = defillama_data.date
where coalesce(price_data.date, defillama_data.date, icp_metrics.date, icp_blocks.date) < to_date(sysdate())
