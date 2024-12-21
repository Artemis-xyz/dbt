{{
    config(
        materialized='table',
        snowflake_warehouse='CONVEX',
        database='CONVEX',
        schema='core',
        alias='ez_metrics_by_token'
    )
}}

with 
    fees_and_revenue as (
        select
            date,
            token,
            sum(fees) as fees,
            sum(revenue) as revenue,
            sum(primary_supply_side_fees) as primary_supply_side_fees
        from {{ ref('fact_convex_revenue') }}
        group by 1, 2
    )
    , treasury_by_token as (
        select
            date,
            token,
            usd_balance as treasury_value
        from {{ ref('fact_convex_treasury_balance') }}
        group by 1, 2
    )
    , net_treasury as (
        select
            date,
            token,
            usd_balance as net_treasury_value
        from {{ ref('fact_convex_treasury_balance') }}
        group by 1, 2
        where token != 'CVX'
    )
    , treasury_native as (
        select
            date,
            token,
            native_balance as treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        group by 1, 2
        where token = 'CVX'
    )   


, date_token_spine as (
    SELECT
        distinct
        date,
        token
    from {{ ref('dim_date_spine') }}
    CROSS JOIN (
                SELECT distinct token from treasury_by_token
                UNION
                SELECT distinct token from net_treasury
                UNION
                SELECT distinct token from treasury_native
                )
    where date between '2020-03-01' and to_date(sysdate())
)

select
    date_token_spine.date,
    token,
    fees_and_revenue.fees,
    fees_and_revenue.revenue,
    fees_and_revenue.primary_supply_side_fees,
    treasury_by_token.treasury_value,
    net_treasury.net_treasury_value,
    treasury_native.treasury_native
from date_token_spine
full outer join treasury_by_token using (date, token)
full outer join net_treasury using (date, token)
full outer join treasury_native using (date, token)
full outer join fees_and_revenue using (date, token)