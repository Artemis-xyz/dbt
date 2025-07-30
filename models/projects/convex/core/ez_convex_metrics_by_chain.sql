{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

with
 fees_and_revenue as (
    select
        date
        , chain
        , coalesce(sum(fees), 0) as fees
        , coalesce(sum(revenue), 0) as revenue
        , coalesce(sum(primary_supply_side_fees), 0) as primary_supply_side_fees
    from {{ ref('fact_convex_revenue') }}
    group by 1, 2
 )
, token_incentives as (
    select
        date
        , 'ethereum' as chain
        , coalesce(sum(token_incentives), 0) as token_incentives
    from {{ ref('fact_convex_token_incentives') }}
    group by 1, 2
)
, tvl as (
    select
        date
        , 'ethereum' as chain
        , coalesce(sum(tvl), 0) as tvl
    from {{ ref('fact_convex_combined_tvl') }}
    group by 1, 2
)
, treasury as (
    select
        date
        , chain
        , coalesce(sum(usd_balance), 0) as treasury
        , coalesce(sum(native_balance), 0) as treasury_native
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1, 2
)
, net_treasury as (
    select
        date
        , chain
        , coalesce(sum(usd_balance), 0) as net_treasury
        , coalesce(sum(native_balance), 0) as net_treasury_native
    from {{ ref('fact_convex_treasury_balance') }}
    where token != 'CVX'
    group by 1, 2
)
, treasury_native as (
    select
        date
        , chain
        , coalesce(sum(usd_balance), 0) as own_token_treasury
        , coalesce(sum(native_balance), 0) as own_token_treasury_native
    from {{ ref('fact_convex_treasury_balance') }}
    where token = 'CVX'
    group by 1, 2
)  
, date_chain_spine as (
    SELECT
        distinct
        date,
        chain
    FROM {{ ref('dim_date_spine') }}
    CROSS JOIN (SELECT distinct chain from fees_and_revenue
                UNION
                SELECT distinct chain from treasury
                UNION
                SELECT distinct chain from net_treasury
                UNION
                SELECT distinct chain from treasury_native
                )
    where date between '2020-03-01' and to_date(sysdate())
)

select
    date_chain_spine.date
    , 'convex' as artemis_id
    , date_chain_spine.chain

    -- Standardized Metrics

    -- Usage Data
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change

    -- Fee Data
    , fees_and_revenue.fees
    , (fees_and_revenue.primary_supply_side_fees + 0.005) * (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees) as service_fee_allocation
    , (0.145 * (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees)) as staking_fee_allocation
    , (0.02 * (fees_and_revenue.revenue + fees_and_revenue.primary_supply_side_fees)) as treasury_fee_allocation

    -- Financial Statements
    , fees_and_revenue.revenue
    , token_incentives.token_incentives
    , token_incentives.token_incentives as expenses
    , fees_and_revenue.revenue - token_incentives.token_incentives as earnings
    
    -- Treasury Data
    , treasury.treasury as treasury
    , treasury.treasury_native as treasury_native
    , net_treasury.net_treasury as net_treasury
    , net_treasury.net_treasury_native as net_treasury_native
    , treasury_native.own_token_treasury as own_token_treasury
    , treasury_native.own_token_treasury_native as own_token_treasury_native

from date_chain_spine
left join fees_and_revenue using (date, chain)
left join tvl using (date, chain)
left join treasury using (date, chain)
left join net_treasury using (date, chain)
left join treasury_native using (date, chain)
left join token_incentives using (date, chain)