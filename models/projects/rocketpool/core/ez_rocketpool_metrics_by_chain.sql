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
    , deposit_fees
    , fees
    , primary_supply_side_revenue
    , secondary_supply_side_revenue
    , total_supply_side_revenue
    , protocol_revenue
    , token_incentives
    , operating_expenses
    , total_expenses
    , protocol_earnings
    , net_deposits
    , outstanding_supply
    , tvl
    , treasury_value

    --Standardized Metrics
    , COALESCE(cl_rewards_usd, 0) as block_rewards
    , COALESCE(el_rewards_usd, 0) as mev_priority_fees
    , COALESCE(deposit_fees, 0) as lst_deposit_fees
    , COALESCE(fees, 0) as yield_generated
    , COALESCE(fees, 0) as gross_protocol_revenue
    , gross_protocol_revenue * 0.14 as ecosystem_revenue
    , gross_protocol_revenue - ecosystem_revenue as lp_revenue
    , s.num_staked_eth as tvl_native
    , s.amount_staked_usd as tvl
from staked_eth_metrics s
left join {{ ref('ez_rocketpool_metrics') }} using(date)