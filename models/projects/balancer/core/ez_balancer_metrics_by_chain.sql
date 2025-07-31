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
        sum(service_cash_flow) as primary_supply_side_revenue,
        sum(treasury_cash_flow + vebal_cash_flow) as revenue,
        sum(service_cash_flow) as service_fee_allocation,
        sum(treasury_cash_flow) as treasury_fee_allocation,
        sum(vebal_cash_flow) as vebal_fee_allocation
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
    , 'balancer' as artemis_id
    , date_chain_spine.chain

    --Usage Data
    , swap_metrics.unique_traders as spot_dau
    , swap_metrics.unique_traders as dau
    , swap_metrics.number_of_swaps as spot_txns
    , swap_metrics.number_of_swaps as txns
    , tvl.tvl_usd as tvl
    , swap_metrics.trading_volume as spot_volume

    --Fee Data
    , swap_metrics.trading_fees as spot_fees
    , swap_metrics.trading_fees as fees

    --Fee Allocation
    , swap_metrics.service_fee_allocation as lp_fee_allocation
    , swap_metrics.treasury_fee_allocation as foundation_fee_allocation
    , swap_metrics.vebal_fee_allocation as staking_fee_allocation

    --Financial Statements
    , swap_metrics.revenue as revenue
    , coalesce(token_incentives.token_incentives, 0) as token_incentives
    , coalesce(swap_metrics.revenue, 0) - coalesce(token_incentives.token_incentives, 0) as earnings

    -- Treasury Metrics
    , net_treasury.net_treasury_usd as treasury
    , net_treasury.net_treasury_usd as net_treasury
    , treasury_native.own_token_treasury as own_token_treasury



from date_chain_spine
left join treasury_by_chain using (date, chain)
left join treasury_native using (date, chain)
left join net_treasury using (date, chain)
left join swap_metrics using (date, chain)
left join token_incentives using (date, chain)
left join tvl using (date, chain)
