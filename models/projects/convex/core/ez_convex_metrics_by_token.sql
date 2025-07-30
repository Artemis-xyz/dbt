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
            date
            , token
            , coalesce(sum(fees), 0) as fees
            , coalesce(sum(revenue), 0) as revenue
            , coalesce(sum(primary_supply_side_fees), 0) as primary_supply_side_fees
        from {{ ref('fact_convex_revenue') }}
        group by 1, 2
    )
    , token_incentives as (
        select
            date
            , symbol as token
            , coalesce(sum(token_incentives_native), 0) as token_incentives_native
            , coalesce(sum(token_incentives), 0) as token_incentives
        from {{ ref('fact_convex_token_incentives') }}
        group by 1, 2
    )
    , tvl as (
        select
            date
            , symbol as token
            , coalesce(sum(tvl), 0) as tvl
            , coalesce(sum(tvl_native), 0) as tvl_native
        from {{ ref('fact_convex_combined_tvl') }}
        group by 1, 2
    )
    , treasury_by_token as (
        select
            date
            , token
            , coalesce(sum(usd_balance), 0) as treasury
            , coalesce(sum(native_balance), 0) as treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        group by 1, 2
    )
    , net_treasury as (
        select
            date
            , token
            , coalesce(sum(usd_balance), 0) as net_treasury
            , coalesce(sum(native_balance), 0) as net_treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        where token != 'CVX'
        group by 1, 2
    )
    , treasury_native as (
        select
            date
            , token
            , coalesce(sum(usd_balance), 0) as own_token_treasury
            , coalesce(sum(native_balance), 0) as own_token_treasury_native
        from {{ ref('fact_convex_treasury_balance') }}
        where token = 'CVX'
        group by 1, 2
    )  
    , date_token_spine as (
        SELECT
            distinct
            , date
            , token
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
    , 'convex' as artemis_id
    , date_token_spine.token

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
    , treasury_by_token.treasury as treasury
    , treasury_by_token.treasury_native as treasury_native
    , net_treasury.net_treasury as net_treasury
    , net_treasury.net_treasury_native as net_treasury_native
    , treasury_native.own_token_treasury as own_token_treasury
    , treasury_native.own_token_treasury_native as own_token_treasury_native

from date_token_spine
full outer join treasury_by_token using (date, token)
full outer join net_treasury using (date, token)
full outer join treasury_native using (date, token)
full outer join fees_and_revenue using (date, token)
full outer join token_incentives using (date, token)
full outer join tvl using (date, token)