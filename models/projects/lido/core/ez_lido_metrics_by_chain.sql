{{
    config(
        materialized="table",
        snowflake_warehouse="LIDO",
        database="lido",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    fees_revenue_expenses as (
        SELECT
            date
            , coalesce(block_rewards, 0) as block_rewards
            , coalesce(mev_priority_fees, 0) as mev_priority_fees
            , coalesce(total_staking_yield, 0) as yield_generated
            , coalesce(fees, 0) as fees
            , coalesce(validator_fee_allocation, 0) as validator_fee_allocation
            , coalesce(treasury_fee_allocation, 0) as treasury_fee_allocation
            , coalesce(protocol_revenue, 0) as protocol_revenue
            , coalesce(primary_supply_side_revenue, 0) as primary_supply_side_revenue
            , coalesce(secondary_supply_side_revenue, 0) as secondary_supply_side_revenue
            , coalesce(total_supply_side_revenue, 0) as total_supply_side_revenue
        FROM {{ ref('fact_lido_fees_revs_expenses') }}
    )
    , token_incentives_cte as (
        SELECT
            date
            , coalesce(sum(amount_usd), 0) as token_incentives
        FROM
            {{ ref('fact_lido_token_incentives') }}
        GROUP BY 1
    )
    , staked_eth_metrics as (
        select
            date
            , coalesce(num_staked_eth, 0) as num_staked_eth
            , coalesce(amount_staked_usd, 0) as amount_staked_usd
            , coalesce(num_staked_eth_net_change, 0) as num_staked_eth_net_change
            , coalesce(amount_staked_usd_net_change, 0) as amount_staked_usd_net_change
        from {{ ref('fact_lido_staked_eth_count_with_USD_and_change') }}
    )
    , treasury_cte as (
        SELECT
            date
            , coalesce(sum(usd_balance), 0) as treasury_value
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        GROUP BY 1
    )
    , treasury_native_cte as (
        SELECT
            date
            , coalesce(sum(native_balance), 0) as treasury_native
        FROM
            {{ ref('fact_lido_dao_treasury') }}
        where token = 'LDO'
        GROUP BY 1
    )
    , net_treasury_cte as (
        SELECT
            date
            , coalesce(sum(usd_balance), 0) as net_treasury_value
        FROM {{ ref('fact_lido_dao_treasury') }}
        where token <> 'LDO'
        group by 1
    )
    , market_metrics as (
        {{ get_coingecko_metrics('lido-dao') }}
    )
    , tokenholder_cte as (
        SELECT
            date,
            coalesce(token_holder_count, 0) as token_holder_count
        FROM
            {{ ref('fact_ldo_tokenholder_count')}}
    )
select
    staked_eth_metrics.date
    , 'lido' as artemis_id
    , 'ethereum' as chain

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price as price
    , market_metrics.fdmc as fdmc
    , market_metrics.market_cap as market_cap
    , market_metrics.token_volume as token_volume

    -- Usage Data
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , fees_revenue_expenses.yield_generated as yield_generated

    -- Fee Data
    , fees_revenue_expenses.mev_priority_fees as mev_priority_fees
    , fees_revenue_expenses.block_rewards as block_rewards
    , fees_revenue_expenses.fees as fees
    , fees_revenue_expenses.treasury_fee_allocation as treasury_fee_allocation
    , fees_revenue_expenses.validator_fee_allocation as validator_fee_allocation

    -- Financial Statements
    , fees_revenue_expenses.protocol_revenue as revenue
    , token_incentives_cte.token_incentives as token_incentives
    , token_incentives_cte.token_incentives as total_expenses
    , fees_revenue_expenses.protocol_revenue - token_incentives_cte.token_incentives as earnings

    --Treasury Data
    , treasury_cte.treasury_value as treasury
    , treasury_native_cte.treasury_native as own_token_treasury_native
    , net_treasury_cte.net_treasury_value as net_treasury

    -- Token Turnover/Other Data
    , market_metrics.token_turnover_fdv as token_turnover_fdv
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , tokenholder_cte.token_holder_count as token_holder_count
    
from staked_eth_metrics
left join fees_revenue_expenses using(date)
left join treasury_cte using(date)
left join treasury_native_cte using(date)
left join net_treasury_cte using(date)
left join token_incentives_cte using(date)
left join market_metrics using(date)
left join tokenholder_cte using(date)
where staked_eth_metrics.date < to_date(sysdate())