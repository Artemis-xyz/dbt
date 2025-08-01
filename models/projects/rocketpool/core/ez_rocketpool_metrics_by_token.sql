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
            , amount_staked_usd
        from {{ ref('fact_rocketpool_staked_eth_count_with_USD_and_change') }}
    )
    , fees_revs_cte as (
        select
            date
            , 'ETH' as token
            , cl_rewards_eth
            , el_rewards_eth
            , deposit_fee_eth as deposit_fees_native
            , total_node_rewards_eth + deposit_fee_eth as fees_native
            , total_node_rewards_eth as primary_supply_side_revenue_native
            , deposit_fee_eth as secondary_supply_side_revenue_native
            , total_node_rewards_eth + deposit_fee_eth as total_supply_side_revenue_native
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
    , 'rocketpool' as artemis_id
    , token

    --Standardized Metrics
    --Usage Metrics
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl

    --Fee Metrics
    , f.cl_rewards_eth as block_rewards_native
    , f.el_rewards_eth as mev_priority_fees_native
    , f.deposit_fees_native as lst_deposit_fees_native
    , coalesce(f.cl_rewards_eth, 0) + coalesce(f.el_rewards_eth, 0) as yield_generated_native
    , f.fees_native as fees_native
    , coalesce(f.fees_native, 0) * 0.14 as validator_fee_allocation_native
    , coalesce(f.fees_native, 0) * 0.86 as service_fee_allocation_native

    --Financial Statement Metrics
    , 0 as revenue
    , ti.token_incentives as token_incentives
    , 0 as operating_expenses
    , ti.token_incentives as total_expenses
    , coalesce(revenue,0) - coalesce(token_incentives,0) as earnings
    

from staked_eth_metrics
full join fees_revs_cte f using (date, token)
full join token_incentives_cte ti using (date, token)
full join treasury_cte t using (date, token)
full join treasury_native_cte tn using (date, token)
full join net_treasury_cte nt using (date, token)
full join outstanding_supply_cte os using (date, token)
full join token_holders_cte th using (date, token)
where coalesce(staked_eth_metrics.date, f.date, ti.date, t.date, nt.date, tn.date, os.date, th.date) < to_date(sysdate())
