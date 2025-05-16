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
    , token_incentives as (
        select
            date,
            symbol as token,
            sum(token_incentives_native) as token_incentives_native,
            sum(token_incentives) as token_incentives
        from {{ ref('fact_convex_token_incentives') }}
        group by 1, 2
    )
    , tvl as (
        select
            date,
            symbol as token,
            sum(tvl) as tvl,
            sum(tvl_native) as tvl_native
        from {{ ref('fact_convex_combined_tvl') }}
        group by 1, 2
    )
    , treasury_by_token as (
        select
            date,
            token,
            sum(usd_balance) as treasury,
            sum(native_balance) as treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        group by 1, 2
    )
    , net_treasury as (
        select
            date,
            token,
            sum(usd_balance) as net_treasury,
            sum(native_balance) as net_treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        where token != 'CVX'
        group by 1, 2
    )
    , treasury_native as (
        select
            date,
            token,
            sum(usd_balance) as own_token_treasury,
            sum(native_balance) as own_token_treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        where token = 'CVX'
        group by 1, 2
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
                    UNION
                    SELECT distinct token from token_incentives
                    UNION
                    SELECT distinct token from tvl
                    )
        where date between '2020-03-01' and to_date(sysdate())
    )

select
    date_token_spine.date
    , token
    , fees_and_revenue.fees
    , fees_and_revenue.revenue
    , fees_and_revenue.primary_supply_side_fees
    , net_treasury.net_treasury as net_treasury_value
    , treasury_by_token.treasury_native as treasury_value_native
    , treasury_native.own_token_treasury_native as treasury_native_value
    , token_incentives.token_incentives
    , token_incentives.token_incentives_native

   -- Standardized Metrics

    -- Crypto Metrics
    , tvl.tvl
    , tvl.tvl - lag(tvl.tvl) over (order by date) as tvl_net_change

    -- Cash Flow Metrics
    , coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0) as ecosystem_revenue
    , coalesce(fees_and_revenue.primary_supply_side_fees, 0) + 0.005 * (coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0)) as service_cash_flow
    , 0.145 * (coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0)) as fee_sharing_token_cash_flow
    , 0.02 * (coalesce(fees_and_revenue.revenue, 0) + coalesce(fees_and_revenue.primary_supply_side_fees, 0)) as treasury_cash_flow
    
    -- Protocol Metrics
    , coalesce(treasury_by_token.treasury, 0) as treasury
    , coalesce(treasury_by_token.treasury_native, 0) as treasury_native
    , coalesce(net_treasury.net_treasury, 0) as net_treasury
    , coalesce(net_treasury.net_treasury_native, 0) as net_treasury_native
    , coalesce(treasury_native.own_token_treasury, 0) as own_token_treasury
    , coalesce(treasury_native.own_token_treasury_native, 0) as own_token_treasury_native
from date_token_spine
full outer join treasury_by_token using (date, token)
full outer join net_treasury using (date, token)
full outer join treasury_native using (date, token)
full outer join fees_and_revenue using (date, token)
full outer join token_incentives using (date, token)
full outer join tvl using (date, token)