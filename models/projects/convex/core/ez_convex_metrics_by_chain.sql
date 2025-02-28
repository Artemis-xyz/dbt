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
, treasury_value as (
    select
        date,
        chain,
        sum(usd_balance) as treasury_value
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1, 2
)
, net_treasury as (
    select
        date,
        chain,
        sum(usd_balance) as net_treasury_value
    from {{ ref('fact_convex_treasury_balance') }}
    where token != 'CVX'
    group by 1, 2
)
, treasury_native as (
    select
        date,
        chain,
        sum(native_balance) as treasury_native
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
                SELECT distinct chain from treasury_value
                UNION
                SELECT distinct chain from net_treasury
                UNION
                SELECT distinct chain from treasury_native
                )
    where date between '2020-03-01' and to_date(sysdate())
)

select
    date_chain_spine.date,
    date_chain_spine.chain,
    fees_and_revenue.fees,
    fees_and_revenue.revenue,
    fees_and_revenue.primary_supply_side_fees as primary_supply_side_revenue,
    fees_and_revenue.primary_supply_side_fees as total_supply_side_revenue,
    token_incentives.token_incentives,
    token_incentives.token_incentives as expenses,
    fees_and_revenue.revenue - token_incentives.token_incentives as earnings,
    tvl.tvl,
    treasury_value.treasury_value,
    net_treasury.net_treasury_value,
    treasury_native.treasury_native
from date_chain_spine
left join fees_and_revenue using (date, chain)
left join tvl using (date, chain)
left join treasury_value using (date, chain)
left join net_treasury using (date, chain)
left join treasury_native using (date, chain)
left join token_incentives using (date, chain)