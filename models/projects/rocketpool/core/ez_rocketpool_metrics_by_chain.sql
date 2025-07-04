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
    , 'ethereum' as chain

    --Old metrics needed for compatibility
    , s.num_staked_eth
    , s.amount_staked_usd
    , s.num_staked_eth_net_change
    , s.amount_staked_usd_net_change
    , cl_rewards_usd
    , el_rewards_usd
    , lst_deposit_fees
    , primary_supply_side_revenue
    , secondary_supply_side_revenue
    , total_supply_side_revenue
    , net_deposits
    , outstanding_supply
    , treasury_value

    --Standardized Metrics

    --Usage Metrics
    , s.num_staked_eth as tvl_native
    , s.num_staked_eth as lst_tvl_native
    , s.amount_staked_usd as tvl
    , s.amount_staked_usd as lst_tvl

    --Cash Flow Metrics
    , COALESCE(cl_rewards_usd, 0) as block_rewards
    , COALESCE(el_rewards_usd, 0) as mev_priority_fees
    , COALESCE(lst_deposit_fees, 0) as lst_deposit_fees
    , COALESCE(fees, 0) as yield_generated
    , COALESCE(fees, 0) as fees
    , fees * 0.14 as validator_fee_allocation
    , fees * 0.86 as service_fee_allocation
    
    --Financial Statement Metrics
    , COALESCE(revenue, 0) as revenue
    , COALESCE(token_incentives, 0) as token_incentives
    , COALESCE(operating_expenses, 0) as operating_expenses
    , COALESCE(total_expenses, 0) as total_expenses
    , COALESCE(earnings, 0) as earnings
    
from staked_eth_metrics s
left join {{ ref('ez_rocketpool_metrics') }} using(date)