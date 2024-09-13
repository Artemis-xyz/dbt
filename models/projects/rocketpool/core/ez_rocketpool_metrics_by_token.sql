{{
    config(
        materialized="table",
        snowflake_warehouse="ROCKETPOOL",
        database="rocketpool",
        schema="core",
        alias="ez_metrics_by_token",
    )
}}

with
    staked_eth_metrics as (
        select
            date
            , 'ETH' as token
            , num_staked_eth
        from {{ ref('fact_rocketpool_staked_eth_count_with_USD_and_change') }}
    )
    , fees_revs_cte as (
        select
            date
            , 'ETH' as token
            , cl_rewards_eth
            , el_rewards_eth
            , deposit_fee_eth as deposit_fees
            , cl_rewards_eth + el_rewards_eth + deposit_fees as fees
            , cl_rewards_eth + el_rewards_eth as primary_supply_side_revenue
            , deposit_fees as secondary_supply_side_revenue
            , fees as total_supply_side_revenue
        from {{ ref('fact_rocketpool_fees_revs') }}
        left join {{ ref('fact_rocketpool_deposit_fees') }} using(date)
    )
    , token_incentives_cte as (
        SELECT
            date
            , 'RPL' as token
            , token_incentives_native as token_incentives
        FROM
            {{ ref('fact_rocketpool_expenses') }}
    )
    , outstanding_supply_cte as (
        SELECT
            date
            , 'rETH' as token
            , reth_supply
        FROM
            {{ ref('fact_reth_outstanding') }}
    )
    , treasury_cte as (
        SELECT
            date
            , token
            , sum(native_balance) as treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        GROUP BY 1, 2
    )
    , treasury_native_cte as (
        SELECT
            date
            , token
            , native_balance as treasury_native
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        WHERE token = 'RPL'
    )
    , net_treasury_cte as (
        SELECT
            date
            , token
            , sum(native_balance) as net_treasury_value
        FROM
            {{ ref('fact_rocketpool_treasury') }}
        WHERE token <> 'RPL'
        GROUP BY 1, 2
    )
    , token_holders_cte as (
        SELECT
            date
            , 'RPL' as token
            , token_holder_count
        FROM
            {{ ref('fact_rocketpool_token_holders') }}
    )
select
    coalesce(staked_eth_metrics.date, f.date, ti.date, t.date, nt.date, tn.date, os.date, th.date) as date
    , token
    , COALESCE(f.cl_rewards_eth, 0) as cl_rewards_eth
    , COALESCE(f.el_rewards_eth, 0) as el_rewards_eth
    , COALESCE(f.deposit_fees, 0) as deposit_fees
    , COALESCE(f.fees, 0) as fees
    , COALESCE(f.primary_supply_side_revenue, 0) as primary_supply_side_revenue
    , COALESCE(f.secondary_supply_side_revenue, 0) as secondary_supply_side_revenue
    , COALESCE(f.total_supply_side_revenue, 0) as total_supply_side_revenue
    , 0 as protocol_revenue
    , COALESCE(ti.token_incentives, 0) as token_incentives
    , 0 as operating_expenses
    , COALESCE(token_incentives, 0) as total_expenses
    , coalesce(protocol_revenue,0) - coalesce(token_incentives,0) as protocol_earnings
    , coalesce(staked_eth_metrics.num_staked_eth, 0) as net_deposits
    , coalesce(os.reth_supply, 0) as outstanding_supply
    , coalesce(staked_eth_metrics.num_staked_eth, 0) as tvl
    , COALESCE(t.treasury_value, 0) as treasury_value
    , COALESCE(tn.treasury_native, 0) as treasury_value_native
    , COALESCE(nt.net_treasury_value, 0) as net_treasury_value
    , COALESCE(th.token_holder_count, 0) as token_holder_count
from staked_eth_metrics
full join fees_revs_cte f using (date, token)
full join token_incentives_cte ti using (date, token)
full join treasury_cte t using (date, token)
full join treasury_native_cte tn using (date, token)
full join net_treasury_cte nt using (date, token)
full join outstanding_supply_cte os using (date, token)
full join token_holders_cte th using (date, token)
where coalesce(staked_eth_metrics.date, f.date, ti.date, t.date, nt.date, tn.date, os.date, th.date) < to_date(sysdate())
