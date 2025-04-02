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
    , treasury_value_native
    , net_treasury_value

    --Standardized Metrics
    , COALESCE(f.cl_rewards_usd, 0) as lst_cl_rewards
    , COALESCE(f.el_rewards_usd, 0) as lst_el_rewards
    , COALESCE(f.deposit_fees, 0) as lst_deposit_fees
    , COALESCE(f.fees, 0) as gross_protocol_revenue
    , gross_protocol_revenue * 0.14 as ecosystem_revenue
    , gross_protocol_revenue - ecosystem_revenue as lp_revenue
    , COALESCE(f.operating_expenses, 0) as operating_expenses
    , COALESCE(ti.token_incentives, 0) as token_incentives
    , token_incentives + operating_expenses as total_expenses
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl
    , os.reth_supply as outstanding_supply
    , COALESCE(t.treasury_value, 0) as treasury_value
    , COALESCE(tn.treasury_native, 0) as treasury_value_native
    , COALESCE(nt.net_treasury_value, 0) as net_treasury_value

from staked_eth_metrics s
left join {{ ref('ez_rocketpool_metrics') }} using(date)