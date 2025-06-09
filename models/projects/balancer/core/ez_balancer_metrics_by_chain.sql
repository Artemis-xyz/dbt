{{
    config(
        materialized='table',
        snowflake_warehouse='BALANCER',
        database='BALANCER',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with swap_metrics as (
    SELECT
        block_timestamp::date as date,
        chain,
        count(distinct sender) as unique_traders,
        count(*) as number_of_swaps,
        sum(trading_volume) as trading_volume,
        sum(fee_usd) as trading_fees,
        sum(service_fee_allocation) as primary_supply_side_revenue,
        sum(treasury_fee_allocation + vebal_fee_allocation) as revenue,
        sum(service_fee_allocation) as service_fee_allocation,
        sum(treasury_fee_allocation) as treasury_fee_allocation,
        sum(vebal_fee_allocation) as vebal_fee_allocation
    FROM {{ ref('ez_balancer_dex_swaps') }}
    group by 1,2
)
, tvl as (
    SELECT
        date,
        chain,
        sum(amount_usd) as tvl_usd
    FROM {{ ref('fact_balancer_tvl_by_chain_and_token') }}
    group by 1,2
)
, token_incentives as (
    SELECT
        date,
        case
            when chain ilike '%ethereum%' then 'ethereum'
            else chain 
        end as chain,
        sum(amount_usd) as token_incentives
    FROM {{ ref('fact_balancer_token_incentives_all_chains') }}
    group by 1,2
)

, treasury_by_chain as (
    SELECT
        date,
        case when chain ilike '%ethereum%' then 'ethereum'
        else chain end as chain,
        sum(usd_balance) as usd_balance
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    group by 1,2
)
, net_treasury as(
    SELECT
        date,
        'ethereum' as chain,
        sum(usd_balance) as net_treasury_usd
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token <> 'BAL'
    group by 1,2
)
, treasury_native as (
    SELECT
        date,
        chain,
        sum(native_balance) as treasury_native,
        sum(usd_balance) as own_token_treasury
    FROM {{ ref('fact_balancer_treasury_by_token') }}
    where token = 'BAL'
    group by 1,2
)
, date_chain_spine as (
    SELECT
        distinct
        date,
        chain
    FROM {{ ref('dim_date_spine') }}
    CROSS JOIN (SELECT distinct chain from treasury_by_chain
        UNION
        SELECT distinct chain from tvl
        UNION
        SELECT distinct chain from treasury_native
        UNION
        SELECT distinct chain from net_treasury
    )
    where date between '2020-03-01' and to_date(sysdate())
)

select
    date_chain_spine.date
    , date_chain_spine.chain
    , swap_metrics.number_of_swaps
    , swap_metrics.trading_volume
    , swap_metrics.unique_traders
    , swap_metrics.trading_fees
    , swap_metrics.trading_fees as fees
    , swap_metrics.primary_supply_side_revenue
    , swap_metrics.primary_supply_side_revenue as total_supply_side_revenue
    , swap_metrics.revenue
    , swap_metrics.revenue - token_incentives.token_incentives as earnings
    , tvl.tvl_usd as net_deposits
    , treasury_by_chain.usd_balance as treasury_value
    , treasury_native.treasury_native as treasury_native
    , net_treasury.net_treasury_usd as net_treasury_value

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , coalesce(swap_metrics.unique_traders, 0) as spot_dau
    , coalesce(swap_metrics.number_of_swaps, 0) as spot_txns
    , coalesce(swap_metrics.trading_volume, 0) as spot_volume
    , coalesce(tvl.tvl_usd, 0) as tvl

    -- Money Metrics
    , coalesce(swap_metrics.trading_fees, 0) as spot_fees
    , coalesce(swap_metrics.trading_fees, 0) as ecosystem_revenue
    , coalesce(swap_metrics.service_fee_allocation, 0) as service_fee_allocation
    , coalesce(swap_metrics.treasury_fee_allocation, 0) as treasury_fee_allocation
    , coalesce(swap_metrics.vebal_fee_allocation, 0) as staking_fee_allocation
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

    -- Treasury Metrics
    , coalesce(net_treasury.net_treasury_usd, 0) as treasury
    , coalesce(net_treasury.net_treasury_usd, 0) as net_treasury
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
from date_chain_spine
left join treasury_by_chain using (date, chain)
left join treasury_native using (date, chain)
left join net_treasury using (date, chain)
left join swap_metrics using (date, chain)
left join token_incentives using (date, chain)
left join tvl using (date, chain)
