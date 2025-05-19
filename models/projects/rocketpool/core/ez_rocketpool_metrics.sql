{{
    config(
        materialized="table",
        snowflake_warehouse="ROCKETPOOL",
        database="rocketpool",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    staked_eth_metrics as (
        select
            date
            , num_staked_eth
            , amount_staked_usd
            , num_staked_eth_net_change
            , amount_staked_usd_net_change
        from {{ ref('fact_rocketpool_staked_eth_count_with_USD_and_change') }}
    )
    , fees_revs_cte as (
        select
            date
            , cl_rewards_usd
            , el_rewards_usd
            , deposit_fee_usd as deposit_fees
            , cl_rewards_usd + el_rewards_usd + deposit_fees as fees
            , cl_rewards_usd + el_rewards_usd as primary_supply_side_revenue
            , deposit_fees as secondary_supply_side_revenue
            , fees as total_supply_side_revenue
        from {{ ref('fact_rocketpool_fees_revs') }}
        left join {{ ref('fact_rocketpool_deposit_fees') }} d using(date)
    )
    , token_incentives_cte as (
        SELECT
            date
            , token_incentives_usd
        FROM
            {{ ref('fact_rocketpool_expenses') }}
    )
    , outstanding_supply_cte as (
        SELECT
            date
            , reth_supply
        FROM
            {{ ref('fact_reth_outstanding') }}
    )
    , treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        GROUP BY 1
    )
    , treasury_native_cte as (
        SELECT
            date
            , native_balance as treasury_native
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        WHERE token = 'RPL'
    )
    , net_treasury_cte as (
        SELECT
            date
            , sum(usd_balance) as net_treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        WHERE token <> 'RPL'
        GROUP BY 1
    )
    , prices_cte as (
        {{ get_coingecko_metrics('rocket-pool')}}
    )
    , token_holders_cte as (
        SELECT
            date
            , token_holder_count
        FROM
            {{ ref('fact_rocketpool_token_holders') }}
    )
select
    p.date

    --Old metrics needed for compatibility
    , COALESCE(f.cl_rewards_usd, 0) as cl_rewards_usd
    , COALESCE(f.el_rewards_usd, 0) as el_rewards_usd
    , COALESCE(f.fees, 0) as fees
    , COALESCE(f.primary_supply_side_revenue, 0) as primary_supply_side_revenue
    , COALESCE(f.secondary_supply_side_revenue, 0) as secondary_supply_side_revenue
    , COALESCE(f.total_supply_side_revenue, 0) as total_supply_side_revenue
    , 0 as protocol_revenue
    , COALESCE(ti.token_incentives_usd, 0) as token_incentives
    , 0 as operating_expenses
    , COALESCE(token_incentives_usd, 0) as total_expenses
    , protocol_revenue - token_incentives as protocol_earnings
    , staked_eth_metrics.num_staked_eth as net_deposits
    , os.reth_supply as outstanding_supply
    , COALESCE(t.treasury_value, 0) as treasury_value

    --Standardized Metrics

    --Market Metrics
    , COALESCE(p.fdmc, 0) as fdmc
    , COALESCE(p.market_cap, 0) as market_cap
    , COALESCE(p.token_volume, 0) as token_volume

    --Usage Metrics
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.num_staked_eth_net_change as tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as tvl_net_change

    --Cash Flow Metrics
    , COALESCE(f.cl_rewards_usd, 0) as block_rewards
    , COALESCE(f.el_rewards_usd, 0) as mev_priority_fees
    , COALESCE(f.deposit_fees, 0) as lst_deposit_fees
    , COALESCE(f.cl_rewards_usd, 0) + COALESCE(f.el_rewards_usd, 0) as yield_generated
    , COALESCE(f.fees, 0) as ecosystem_revenue
    , yield_generated * 0.14 as validator_cash_flow
    , yield_generated * 0.86 as service_cash_flow

    --Treasury Metrics
    , COALESCE(t.treasury_value, 0) as treasury
    , COALESCE(tn.treasury_native, 0) as treasury_value_native
    , COALESCE(nt.net_treasury_value, 0) as net_treasury_value
    
    --Other Metrics
    , COALESCE(p.token_turnover_circulating, 0) as token_turnover_circulating
    , COALESCE(p.token_turnover_fdv, 0) as token_turnover_fdv
    , COALESCE(th.token_holder_count, 0) as token_holder_count
    
from prices_cte p
left join fees_revs_cte f using(date)
left join staked_eth_metrics using(date)
left join token_incentives_cte ti using(date)
left join treasury_cte t using(date)
left join treasury_native_cte tn using(date)
left join net_treasury_cte nt using(date)
left join outstanding_supply_cte os using(date)
left join token_holders_cte th using(date)
where p.date < to_date(sysdate())
