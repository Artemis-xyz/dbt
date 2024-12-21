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
, treasury_value as (
    select
        date,
        chain,
        usd_balance as treasury_value
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1, 2
)
, net_treasury as (
    select
        date,
        chain,
        usd_balance as net_treasury_value
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1, 2
    where token != 'CVX'
)
, treasury_native as (
    select
        date,
        chain,
        native_balance as treasury_native
    from {{ ref('fact_convex_treasury_balance') }}
    group by 1, 2
    where token = 'CVX'
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
    fees_and_revenue.primary_supply_side_fees,
    treasury_value.treasury_value,
    net_treasury.net_treasury_value,
    treasury_native.treasury_native
from date_chain_spine
left join fees_and_revenue using (date, chain)
left join treasury_value using (date, chain)
left join net_treasury using (date, chain)
left join treasury_native using (date, chain)