{{
    config(
        materialized="view",
        snowflake_warehouse="ROCKETPOOL",
        database="rocketpool",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    staked_eth_metrics as (
        select
            date,
            'ethereum' as chain,
            num_staked_eth,
            amount_staked_usd,
            num_staked_eth_net_change,
            amount_staked_usd_net_change
        from {{ ref('fact_rocketpool_staked_eth_count_with_USD_and_change') }}
    )
select 
    s.date
    , 'rocketpool' as artemis_id
    , 'ethereum' as chain

    --Standardized Metrics
    --Usage Metrics
    , lst_tvl_native
    , lst_tvl
    , tvl_native
    , tvl

    --Fee Metrics
    , block_rewards
    , mev_priority_fees
    , lst_deposit_fees
    , yield_generated
    , fees
    , validator_fee_allocation
    , service_fee_allocation
    
    --Financial Statement Metrics
    , revenue
    , token_incentives
    , operating_expenses
    , total_expenses
    , earnings
    
from staked_eth_metrics s
left join {{ ref('ez_rocketpool_metrics') }} using(date)