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
        date,
        chain,
        sum(fees) as fees,
        sum(revenue) as revenue,
        sum(primary_supply_side_fees) as primary_supply_side_fees
    from {{ ref('fact_convex_revenue') }}
    group by 1, 2
 )
, token_incentives as (
    select
        date,
        'ethereum' as chain,
        sum(token_incentives) as token_incentives
    from {{ ref('fact_convex_token_incentives') }}
    group by 1, 2
)
, tvl as (
    select
        date,
        'ethereum' as chain,
        sum(tvl) as tvl
    from {{ ref('fact_convex_combined_tvl') }}
    group by 1, 2
)
, treasury as (
    select
        date,
        chain,
        sum(usd_balance) as treasury,
        sum(native_balance) as treasury_native
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1, 2
)
, net_treasury as (
    select
        date,
        chain,
        sum(usd_balance) as net_treasury,
        sum(native_balance) as net_treasury_native
    from {{ ref('fact_convex_treasury_balance') }}
    where token != 'CVX'
    group by 1, 2
)
, treasury_native as (
    select
        date,
        chain,
        sum(usd_balance) as own_token_treasury,
        sum(native_balance) as own_token_treasury_native
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
    , date_chain_spine.chain
    , fees_and_revenue.fees
    , fees_and_revenue.revenue
    , fees_and_revenue.primary_supply_side_fees as primary_supply_side_revenue
    , fees_and_revenue.primary_supply_side_fees as total_supply_side_revenue
    , token_incentives.token_incentives
    , token_incentives.token_incentives as expenses
    , fees_and_revenue.revenue - token_incentives.token_incentives as earnings
    , treasury.treasury as treasury_value
    , net_treasury.net_treasury as net_treasury_value
    , tvl.tvl as net_deposits

    -- Standardized Metrics

    -- Crypto Metrics
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change

    -- Cash Flow Metrics
    , coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0) as gross_protocol_revenue
    , coalesce(fees_and_revenue.primary_supply_side_fees, 0) + 0.005 * (coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0)) as service_cash_flow
    , 0.145 * (coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0)) as fee_sharing_token_cash_flow
    , 0.02 * (coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0)) as treasury_cash_flow

    -- Protocol Metrics
    , coalesce(treasury.treasury, 0) as treasury
    , coalesce(treasury.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
from date_chain_spine
left join fees_and_revenue using (date, chain)
left join tvl using (date, chain)
left join treasury using (date, chain)
left join net_treasury using (date, chain)
left join treasury_native using (date, chain)
left join token_incentives using (date, chain)